import grpc
import time
import threading
from concurrent import futures
from collections import defaultdict
import os
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.parse

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.utils import setup_logging, split_file, calculate_checksum
from controller import controller_pb2
from controller import controller_pb2_grpc
from node import node_pb2
from node import node_pb2_grpc

class ControllerService(controller_pb2_grpc.ControllerServiceServicer):
    def __init__(self, replication_factor=2, controller_port=50051):
        self.replication_factor = replication_factor
        self.controller_port = controller_port
        self.nodes = {}
        self.active_nodes = set()
        self.file_locations = defaultdict(list)
        self.file_metadata = {}
        self.lock = threading.Lock()
        self.logger = setup_logging("controller")
        
        # Create storage directory if it doesn't exist
        if not os.path.exists("storage"):
            os.makedirs("storage")
            
        self.events = []
        self.last_seen = {}
        self.node_status = {}
        # Timeout-based offline detection disabled: nodes only go offline on explicit exit
        self.logger.info(f"Controller initialized with replication factor {replication_factor}")

    def _normalize_ip(self, ip: str) -> str:
        try:
            return '127.0.0.1' if ip and ip.lower() == 'localhost' else ip
        except Exception:
            return ip

    def _log_event(self, etype, message, data=None):
        try:
            self.events.append({
                'time': time.time(),
                'type': etype,
                'message': message,
                'data': data or {}
            })
            if len(self.events) > 200:
                self.events = self.events[-200:]
        except Exception:
            pass

    def RegisterNode(self, request, context):
        norm_ip = self._normalize_ip(request.ip)
        node_key = f"{norm_ip}:{request.port}"
        norm_request = controller_pb2.NodeInfo(
            ip=norm_ip,
            mac=getattr(request, 'mac', ''),
            ram=getattr(request, 'ram', 0),
            cpu=getattr(request, 'cpu', ''),
            bandwidth=getattr(request, 'bandwidth', 0),
            storage=getattr(request, 'storage', 0),
            hdd=getattr(request, 'hdd', ''),
            port=request.port
        )
        with self.lock:
            self.nodes[node_key] = norm_request
            self.active_nodes.add(node_key)
            self.last_seen[node_key] = time.time()
            self.logger.debug(f"Node {node_key} registered and online")
            self._log_event('node', 'Node registered', {'node': node_key})
            # Print only on state transition to online
            prev = self.node_status.get(node_key)
            if prev != 'online':
                print(f"Node online: {node_key}. Active nodes: {len(self.active_nodes)}", flush=True)
            self.node_status[node_key] = 'online'
        
        # Trigger replication to new node in background
        threading.Thread(
            target=self._replicate_to_new_node,
            args=(node_key,),
            daemon=True
        ).start()
        
        return controller_pb2.RegistrationResponse(success=True, message="Registration successful")

    def _replicate_to_new_node(self, new_node_key):
        """Replicate files to a newly registered node if replication factor isn't met"""
        with self.lock:
            for filename, locations in self.file_locations.items():
                # Replicate every file to the newly online node if it doesn't have it yet
                if new_node_key not in locations:
                    source_key = "controller" if "controller" in locations else (locations[0] if locations else "controller")
                    threading.Thread(
                        target=self._replicate_file_with_retries,
                        args=(filename, source_key, new_node_key, 3, 1.0),
                        daemon=True
                    ).start()
                    # Small delay between replications to avoid overwhelming the node
                    time.sleep(0.1)

    def ListFiles(self, request, context):
        with self.lock:
            return controller_pb2.FileList(filenames=list(self.file_locations.keys()))

    def RequestFile(self, request, context):
        filename = request.filename
        with self.lock:
            if filename not in self.file_locations:
                return controller_pb2.FileLocation(is_online=False)
            locations = self.file_locations[filename]
            
            # Try to find an active location
            for node_key in locations:
                if node_key == "controller":
                    # Check if file exists on controller
                    if os.path.exists(f"storage/{filename}"):
                        return controller_pb2.FileLocation(
                            node_ip="controller",
                            node_port=self.controller_port,
                            is_online=True
                        )
                elif node_key in self.active_nodes and node_key in self.nodes:
                    node_info = self.nodes[node_key]
                    return controller_pb2.FileLocation(
                        node_ip=self._normalize_ip(node_info.ip),
                        node_port=node_info.port,
                        is_online=True
                    )
            
            return controller_pb2.FileLocation(is_online=False)

    def DownloadFile(self, request, context):
        filename = request.filename
        with self.lock:
            if filename not in self.file_locations:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("File not found")
                return
                
        file_path = f"storage/{filename}"
        if not os.path.exists(file_path):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("File not found on controller")
            return
            
        self.logger.info(f"File '{filename}' download started.")
        try:
            chunks, total_chunks = split_file(file_path)
            for chunk_number, data, checksum in chunks:
                yield controller_pb2.FileChunk(
                    filename=filename,
                    content=data,
                    chunk_number=chunk_number,
                    total_chunks=total_chunks,
                    checksum=checksum
                )
            self.logger.info(f"File '{filename}' download completed.")
        except Exception as e:
            self.logger.error(f"Error downloading file '{filename}': {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error reading file: {str(e)}")

    def Heartbeat(self, request, context):
        node_key = f"{request.ip}:{request.port}"
        with self.lock:
            if node_key not in self.nodes:
                # Node not registered, ask it to register
                return controller_pb2.HeartbeatResponse(acknowledged=False)
            
            if request.is_active:
                self.last_seen[node_key] = time.time()
                if node_key not in self.active_nodes:
                    self.active_nodes.add(node_key)
                    self.logger.debug(f"Node {node_key} is back online")
                    self._log_event('node', 'Node back online', {'node': node_key})
                    # Update status silently (no duplicate print)
                    self.node_status[node_key] = 'online'
                    # Trigger replication in background
                    threading.Thread(
                        target=self._replicate_to_new_node,
                        args=(node_key,),
                        daemon=True
                    ).start()
            else:
                if node_key in self.active_nodes:
                    self.active_nodes.remove(node_key)
                self.logger.debug(f"Node {node_key} is offline")
                self._log_event('node', 'Node offline', {'node': node_key})
                try:
                    # Print only on transition to offline
                    if self.node_status.get(node_key) != 'offline':
                        print(f"Node offline: {node_key}. Active nodes: {len(self.active_nodes)}", flush=True)
                    self.node_status[node_key] = 'offline'
                except Exception:
                    pass

        return controller_pb2.HeartbeatResponse(acknowledged=True)

    def _remove_node(self, node_key):
        try:
            removed = False
            if node_key in self.nodes:
                del self.nodes[node_key]
                removed = True
            if node_key in self.active_nodes:
                self.active_nodes.remove(node_key)
            for name, locs in list(self.file_locations.items()):
                if node_key in locs:
                    self.file_locations[name] = [x for x in locs if x != node_key]
            if node_key in self.last_seen:
                del self.last_seen[node_key]
            if removed:
                self._log_event('node', 'Node removed', {'node': node_key})
                try:
                    if self.node_status.get(node_key) != 'offline':
                        print(f"Node offline: {node_key}. Active nodes: {len(self.active_nodes)}", flush=True)
                    self.node_status[node_key] = 'offline'
                except Exception:
                    pass
            return removed
        except Exception:
            return False

    def _monitor_nodes(self, timeout=45, interval=5):
        while True:
            time.sleep(interval)
            now = time.time()
            with self.lock:
                for node_key in list(self.active_nodes):
                    last = self.last_seen.get(node_key, 0)
                    if now - last > timeout:
                        self.active_nodes.remove(node_key)
                        self._log_event('node', 'Node offline (timeout)', {'node': node_key})
                        try:
                            print(f"Node offline (timeout): {node_key}. Active nodes: {len(self.active_nodes)}", flush=True)
                        except Exception:
                            pass

    def _get_node_stub(self, node_key, timeout=5):
        """Get gRPC stub for a node with error handling"""
        if node_key not in self.nodes or node_key not in self.active_nodes:
            raise Exception(f"Node {node_key} not available")
        
        node = self.nodes[node_key]
        ip = self._normalize_ip(node.ip)
        channel = grpc.insecure_channel(f'{ip}:{node.port}')
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
            return node_pb2_grpc.NodeServiceStub(channel), channel
        except Exception as e:
            channel.close()
            raise e

    def _replicate_file(self, filename, source_key, target_key):
        """Replicate a file from source to target node"""
        try:
            if target_key not in self.nodes or target_key not in self.active_nodes:
                self.logger.warning(f"Target node {target_key} not available for replication")
                return False

            target_node = self.nodes[target_key]

            # Get file chunks from source
            if source_key == "controller":
                file_path = f"storage/{filename}"
                if not os.path.exists(file_path):
                    self.logger.error(f"Replication error: file '{filename}' not found on controller.")
                    return False

                chunks, total_chunks = split_file(file_path)
                chunk_generator = (
                    node_pb2.FileChunk(
                        filename=filename,
                        content=data,
                        chunk_number=chunk_number,
                        total_chunks=total_chunks,
                        checksum=checksum
                    ) for chunk_number, data, checksum in chunks
                )
            else:
                # Get from another node
                try:
                    source_stub, source_channel = self._get_node_stub(source_key, timeout=10)
                    chunk_generator = source_stub.SendFile(node_pb2.FileRequest(filename=filename), timeout=60)
                except Exception as e:
                    self.logger.error(f"Failed to connect to source node {source_key}: {str(e)}")
                    return False

            # Send to target node
            try:
                target_stub, target_channel = self._get_node_stub(target_key, timeout=10)
                response = target_stub.ReceiveFile(chunk_generator, timeout=60)
                target_channel.close()
                
                if response.success:
                    with self.lock:
                        if target_key not in self.file_locations[filename]:
                            self.file_locations[filename].append(target_key)
                    self.logger.info(f"File '{filename}' successfully replicated to node {target_key}")
                    self._log_event('replicate', 'File replicated', {'filename': filename, 'target': target_key})
                    print(f"✓ Replicated '{filename}' to {target_key}", flush=True)
                    return True
                else:
                    self.logger.warning(f"Replication to node {target_key} failed: {response.message}")
                    print(f"✗ Replication failed for '{filename}' to {target_key}: {response.message}", flush=True)
                    return False
                    
            except Exception as e:
                self.logger.error(f"Failed to replicate to target node {target_key}: {str(e)}")
                return False

        except Exception as e:
            self.logger.error(f"Replication error: {str(e)}")
            return False

    def _replicate_file_with_retries(self, filename, source_key, target_key, retries=3, backoff=1.0):
        for attempt in range(1, retries + 1):
            ok = self._replicate_file(filename, source_key, target_key)
            if ok:
                return True
            if attempt < retries:
                try:
                    print(f"↻ Retrying replication of '{filename}' to {target_key} (attempt {attempt+1}/{retries})", flush=True)
                except Exception:
                    pass
                time.sleep(backoff * attempt)
        return False

    def UploadFile(self, request_iterator, context):
        chunks = {}
        filename = None
        total_chunks = None
        start_time = time.time()
        temp_path = None

        try:
            for chunk in request_iterator:
                if filename is None:
                    filename = chunk.filename
                    total_chunks = chunk.total_chunks
                    temp_path = os.path.join('storage', f".upload_{filename}_{int(time.time())}")
                    self.logger.info(f"Starting upload of file '{filename}' with {total_chunks} chunks")
                    print(f"→ Upload starting: '{filename}', chunks: {total_chunks}", flush=True)

                if calculate_checksum(chunk.content) != chunk.checksum:
                    return controller_pb2.UploadResponse(
                        success=False,
                        message=f"Checksum mismatch for chunk {chunk.chunk_number}"
                    )

                chunks[chunk.chunk_number] = chunk.content

            if total_chunks is None or len(chunks) != total_chunks:
                return controller_pb2.UploadResponse(
                    success=False,
                    message=f"Incomplete upload. Received {len(chunks)} of {total_chunks if total_chunks is not None else '?'} chunks"
                )

            # Write chunks to temporary file
            with open(temp_path, 'wb') as f:
                for i in range(total_chunks):
                    if i in chunks:
                        f.write(chunks[i])
                    else:
                        return controller_pb2.UploadResponse(
                            success=False,
                            message=f"Missing chunk {i}"
                        )

            # Move to final location
            final_path = os.path.join('storage', filename)
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(temp_path, final_path)
            temp_path = None

            file_size = os.path.getsize(final_path)
            end_time = time.time()
            transfer_time = end_time - start_time

            with self.lock:
                self.file_metadata[filename] = {
                    'size': file_size,
                    'chunks': total_chunks,
                    'upload_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
                }
                self.file_locations[filename] = ["controller"]

            self.logger.info(f"File '{filename}' uploaded: {total_chunks} chunks ({file_size} bytes), transfer time: {transfer_time:.2f}s")
            self._log_event('upload', 'File uploaded', {
                'filename': filename,
                'size': file_size,
                'chunks': total_chunks,
                'transfer_time': transfer_time
            })
            print(f"✓ Upload complete: '{filename}' - chunks: {total_chunks}, size: {file_size} bytes, time: {transfer_time:.2f}s", flush=True)
            print(f"→ Starting replication to up to {max(self.replication_factor-1, 0)} node(s)...", flush=True)

            # Background replication
            def background_replication():
                replica_count = 1  # Controller already has it
                with self.lock:
                    candidates = [node_key for node_key in self.active_nodes
                                 if node_key != "controller" and node_key not in self.file_locations[filename]]
                target_nodes = candidates  # replicate to all available nodes
                print(f"→ Replicating '{filename}' to {len(target_nodes)} node(s)...", flush=True)
                for idx, node_key in enumerate(target_nodes, start=1):
                    print(f"  → [{idx}/{len(target_nodes)}] {node_key}", flush=True)
                    success = self._replicate_file_with_retries(filename, "controller", node_key, 3, 1.0)
                    if success:
                        replica_count += 1
                        self.logger.info(f"File '{filename}' replicated to node {node_key}")
                    else:
                        self.logger.warning(f"File '{filename}' failed to replicate to node {node_key}")
                        # Do not change node online status on replication failure; rely on heartbeat monitoring
                        with self.lock:
                            self._log_event('replicate', 'Replication failed', {'filename': filename, 'node': node_key})

                # Report actual replicas present (including controller)
                with self.lock:
                    total_replicas = len(self.file_locations.get(filename, []))
                self.logger.info(f"Replication complete for '{filename}': {total_replicas} replicas")
                print(f"✓ Replication complete for '{filename}': {total_replicas} replicas", flush=True)

            threading.Thread(target=background_replication, daemon=True).start()

            return controller_pb2.UploadResponse(
                success=True,
                message="File uploaded successfully. Replication will continue in background.",
                replication_count=1
            )

        except Exception as e:
            self.logger.error(f"Upload error: {str(e)}")
            return controller_pb2.UploadResponse(
                success=False,
                message=f"Error processing file: {str(e)}"
            )

        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass


# --- Lightweight HTTP Management API ---

def make_management_handler(controller_service):
    class ManagementAPIHandler(BaseHTTPRequestHandler):
        def _cors(self):
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET,OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')

        def _send_json(self, code, payload):
            try:
                body = json.dumps(payload).encode('utf-8')
            except Exception:
                body = b'{}'
            self.send_response(code)
            self._cors()
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

        def do_OPTIONS(self):
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_POST(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            query = urllib.parse.parse_qs(parsed.query)
            length = int(self.headers.get('Content-Length', 0) or 0)
            body = b''
            if length:
                try:
                    body = self.rfile.read(length)
                except Exception:
                    body = b''
            # Remove a node from registry: /nodes/remove?id=<node_key>
            if path == '/nodes/remove':
                node_id = None
                if 'id' in query:
                    node_id = query['id'][0]
                else:
                    try:
                        data = json.loads(body.decode('utf-8') or '{}')
                        node_id = data.get('id')
                    except Exception:
                        node_id = None
                if not node_id:
                    return self._send_json(400, {'error': 'missing node id'})
                with controller_service.lock:
                    removed = controller_service._remove_node(node_id)
                return self._send_json(200, {
                    'removed': bool(removed),
                    'active_count': len(controller_service.active_nodes),
                    'total_count': len(controller_service.nodes)
                })
            return self._send_json(404, {'error': 'not found'})

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            
            if path == '/':
                html = """
<!doctype html>
<html><head><meta charset='utf-8'><title>Controller Dashboard</title>
<style>
body{font-family:Arial, sans-serif;margin:20px;background:#f5f5f5} 
.row{display:flex;gap:16px;margin-bottom:16px} 
.card{border:1px solid #ddd;border-radius:8px;padding:16px;flex:1;background:white;box-shadow:0 2px 4px rgba(0,0,0,0.1)} 
pre{white-space:pre-wrap;font-size:12px;background:#f8f8f8;padding:8px;border-radius:4px;max-height:300px;overflow:auto}
.status{padding:4px 8px;border-radius:4px;color:white;font-size:12px;font-weight:bold}
.online{background:#4CAF50} .offline{background:#f44336}
h2{color:#333;margin-bottom:20px} h3{color:#555;margin-top:0}
.file-item{background:#f8f9fa;margin:8px 0;padding:8px;border-radius:4px;border-left:4px solid #007bff}
.event-item{background:#f8f9fa;margin:8px 0;padding:8px;border-radius:4px;font-size:12px}
</style>
</head><body>
<h2>Distributed Storage Controller Dashboard</h2>
<div class='row'>
  <div class='card'>
    <h3>System Status</h3>
    <div id='status'>Loading...</div>
  </div>
  <div class='card'>
    <h3>Active Nodes</h3>
    <div id='nodes'>Loading...</div>
  </div>
</div>
<div class='row'>
  <div class='card'>
    <h3>Files in Cloud Storage</h3>
    <div id='files'>Loading...</div>
  </div>
  <div class='card'>
    <h3>Recent Activity</h3>
    <div id='events'>Loading...</div>
  </div>
</div>
<script>
async function get(url){ const r = await fetch(url); if(!r.ok) throw new Error(await r.text()); return r.json(); }
function fmt(ts){ const d=new Date(ts*1000); return d.toLocaleTimeString(); }
async function refresh(){
  try{ 
    const [n, f, s, ev] = await Promise.all([get('/nodes?all=1'), get('/files'), get('/stats'), get('/events')]);
    
    // Status
    const status = document.getElementById('status');
    const totalNodes = n.nodes.length;
    const onlineNodes = n.nodes.filter(x => x.online).length;
    status.innerHTML = `<strong>Nodes:</strong> ${onlineNodes}/${totalNodes} online<br><strong>Files:</strong> ${f.files.length} total<br><strong>Local files:</strong> ${s.local_files}`;
    
    // Nodes
    const nodes = document.getElementById('nodes');
    nodes.innerHTML = '';
    for(const node of n.nodes){ 
      const div = document.createElement('div');
      div.style.marginBottom = '8px';
      const statusClass = node.online ? 'online' : 'offline';
      const statusText = node.online ? 'Online' : 'Offline';
      div.innerHTML = `<strong>${node.id}</strong> <span class="status ${statusClass}">${statusText}</span><br><small>${node.cpu} | ${node.ram}MB RAM</small>`;
      nodes.appendChild(div);
    }
    
    // Files
    const files = document.getElementById('files');
    files.innerHTML = '';
    for(const file of f.files){ 
      const div = document.createElement('div');
      div.className = 'file-item';
      div.innerHTML = `<strong>${file.name}</strong><br>Size: ${(file.size/1024/1024).toFixed(2)} MB | Replicas: ${file.replicas_online}/${file.replicas_total} online<br><small>Uploaded: ${file.upload_time}</small>`;
      files.appendChild(div);
    }
    
    // Events
    const events = document.getElementById('events');
    events.innerHTML = '';
    for(const e of ev.events.slice(-10).reverse()){ 
      const div = document.createElement('div');
      div.className = 'event-item';
      div.innerHTML = `<strong>${fmt(e.time)}</strong> [${e.type}] ${e.message}`;
      events.appendChild(div);
    }
  }
  catch(e){ console.error('Refresh error:', e); }
}
refresh(); setInterval(refresh, 3000);
</script>
</body></html>
"""
                body = html.encode('utf-8')
                self.send_response(200)
                self._cors()
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
                
            elif path == '/nodes':
                # Optional query flags: all=1 to include offline, active=1 to include only online (default)
                q = urllib.parse.parse_qs(parsed.query)
                include_all = str(q.get('all', ['0'])[0]).lower() in ('1', 'true', 'yes')
                active_only = not include_all and str(q.get('active', ['1'])[0]).lower() not in ('0', 'false', 'no')
                with controller_service.lock:
                    nodes = []
                    for node_id, node in controller_service.nodes.items():
                        online = node_id in controller_service.active_nodes
                        if active_only and not online:
                            continue
                        nodes.append({
                            "id": node_id,
                            "ip": node.ip,
                            "port": node.port,
                            "online": online,
                            "cpu": getattr(node, 'cpu', ''),
                            "ram": getattr(node, 'ram', 0),
                            "storage": getattr(node, 'storage', 0),
                            "hdd": getattr(node, 'hdd', '')
                        })
                return self._send_json(200, {"nodes": nodes})
                
            elif path == '/files':
                with controller_service.lock:
                    files = []
                    # Only show files that were uploaded to the cloud (tracked in file_metadata)
                    # and that have at least one online replica on a node (excluding controller)
                    for name, meta in controller_service.file_metadata.items():
                        locations = controller_service.file_locations.get(name, [])
                        node_locations = [nid for nid in locations if nid != 'controller']
                        online_node_locations = [nid for nid in node_locations if nid in controller_service.active_nodes]
                        # Hide files that are not currently available on any online node
                        if len(online_node_locations) == 0:
                            continue
                        files.append({
                            "name": name,
                            "size": meta.get('size', 0),
                            "chunks": meta.get('chunks', 0),
                            "replicas_total": len(node_locations),
                            "replicas_online": len(online_node_locations),
                            "upload_time": meta.get('upload_time', ''),
                            "locations": node_locations
                        })
                return self._send_json(200, {"files": files})
                
            elif path == '/stats':
                # Count local-only files across nodes (not uploaded to controller)
                with controller_service.lock:
                    uploaded = set(controller_service.file_metadata.keys())
                    local_count = 0
                    try:
                        for node_id, node in controller_service.nodes.items():
                            storage_dir = f"node_storage_{node.port}"
                            if os.path.isdir(storage_dir):
                                for fname in os.listdir(storage_dir):
                                    if fname == '.healthcheck' or fname.startswith('temp_') or fname in uploaded:
                                        continue
                                    fpath = os.path.join(storage_dir, fname)
                                    if os.path.isfile(fpath):
                                        local_count += 1
                    except Exception:
                        pass
                return self._send_json(200, {"local_files": local_count})
            
            elif path == '/events':
                with controller_service.lock:
                    events = list(controller_service.events)
                return self._send_json(200, {"events": events})
                
            return self._send_json(404, {"error": "not found"})
            
    return ManagementAPIHandler


def start_management_api(controller_service, api_port=8088):
    handler = make_management_handler(controller_service)
    try:
        httpd = ThreadingHTTPServer(('localhost', api_port), handler)
    except OSError:
        httpd = ThreadingHTTPServer(('localhost', 0), handler)
        api_port = httpd.server_address[1]
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return api_port


def serve(replication_factor=2, port=50051, api_port=8088):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    controller_service = ControllerService(replication_factor=replication_factor, controller_port=port)
    controller_pb2_grpc.add_ControllerServiceServicer_to_server(
        controller_service, 
        server
    )
    
    # Bind IPv4 first, with localhost fallback
    bound = 0
    try:
        bound = server.add_insecure_port(f'0.0.0.0:{port}')
    except Exception:
        bound = 0
    if not bound:
        try:
            bound = server.add_insecure_port(f'127.0.0.1:{port}')
        except Exception:
            bound = 0
    if not bound:
        raise RuntimeError(f"Failed to bind controller to port {port} on IPv4")

    server.start()
    api_port = start_management_api(controller_service, api_port=api_port)
    # Write endpoint info for local discovery
    try:
        endpoint = {"host": "127.0.0.1", "port": port, "api_port": api_port, "time": int(time.time())}
        with open("controller_endpoint.json", "w") as f:
            json.dump(endpoint, f)
    except Exception:
        pass
    
    print(f"✓ Controller server started on port {port}")
    print(f"✓ Replication factor: {replication_factor}")
    print(f"✓ Web dashboard: http://localhost:{api_port}")
    print(f"✓ Storage directory: {os.path.abspath('storage')}")
    print("\nWaiting for nodes to connect...")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("\nShutting down controller...")
        server.stop(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed Storage Controller')
    parser.add_argument('--port', type=int, default=50051, help='Controller port (default: 50051)')
    parser.add_argument('--replication', type=int, default=2, help='Replication factor (default: 2)')
    parser.add_argument('--api-port', type=int, default=8088, help='Web API port (default: 8088)')
    args = parser.parse_args()
    serve(replication_factor=args.replication, port=args.port, api_port=args.api_port)
