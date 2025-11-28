import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import threading
import time
import argparse
import json
import urllib.request
from concurrent import futures

from shared.utils import setup_logging, split_file, reconstruct_file, calculate_checksum
from node import node_pb2
from node import node_pb2_grpc
from controller import controller_pb2
from controller import controller_pb2_grpc

class NodeService(node_pb2_grpc.NodeServiceServicer):
    def __init__(self, ip, mac, ram, cpu, bandwidth, storage, hdd, port, controller_address):
        self.ip = ip if str(ip).lower() != "localhost" else "127.0.0.1"
        self.mac = mac
        self.ram = ram
        self.cpu = cpu
        self.bandwidth = bandwidth
        self.storage = storage
        self.hdd = hdd
        self.port = port
        self.controller_address = controller_address
        # Normalize controller address to IPv4 to avoid IPv6 localhost issues on Windows
        self.controller_address = self._normalize_address(self.controller_address)
        # Try discovery file to override target controller if available
        try:
            discovered = self._try_load_discovered_controller()
            if discovered:
                print(f"Using discovered controller: {discovered}")
        except Exception:
            pass
        self.files = set()
        self.storage_path = f"node_storage_{port}"
        self.health_check_file = ".healthcheck"
        self.registered = False
        self._ctrl_channel = None
        self._ctrl_stub = None
        self._shutdown = False

        self.logger = setup_logging(f"node_{port}")

        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)

        with open(os.path.join(self.storage_path, self.health_check_file), 'w') as f:
            f.write("OK")

        # Start gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        node_pb2_grpc.add_NodeServiceServicer_to_server(self, self.server)
        
        # Bind IPv4 first with localhost fallback; do not bind IPv6 on systems without support
        bound = 0
        try:
            bound = self.server.add_insecure_port(f'0.0.0.0:{port}')
        except Exception:
            bound = 0
        if not bound:
            try:
                bound = self.server.add_insecure_port(f'127.0.0.1:{port}')
            except Exception:
                bound = 0
        if not bound:
            raise RuntimeError(f"Failed to bind node server to port {port} on IPv4")
        
        self.server.start()
        print(f"Node gRPC server started on port {port}")

        # Start persistent registration and heartbeat threads
        self.registration_thread = threading.Thread(target=self._persistent_register)
        self.registration_thread.daemon = True
        self.registration_thread.start()
        
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def _normalize_address(self, addr: str) -> str:
        try:
            host, port = addr.split(":", 1)
            if host.lower() == "localhost":
                return f"127.0.0.1:{port}"
            return addr
        except Exception:
            return addr

    def _discovery_paths(self):
        return [
            os.path.join(os.getcwd(), 'controller_endpoint.json'),
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'controller_endpoint.json')
        ]

    def _try_load_discovered_controller(self):
        for p in self._discovery_paths():
            try:
                if os.path.isfile(p):
                    with open(p, 'r') as f:
                        data = json.load(f)
                    host = data.get('host', '127.0.0.1')
                    port = data.get('port')
                    if port:
                        addr = f"{host}:{port}"
                        if addr != self.controller_address:
                            self.controller_address = self._normalize_address(addr)
                            self._reset_controller_channel()
                        return self.controller_address
            except Exception:
                continue
        return None

    def _get_management_api_base(self):
        # Try discovery for api_port, fallback to default 8088
        try:
            for p in self._discovery_paths():
                if os.path.isfile(p):
                    with open(p, 'r') as f:
                        data = json.load(f)
                    api_port = data.get('api_port')
                    if api_port:
                        return f"http://127.0.0.1:{api_port}"
        except Exception:
            pass
        return "http://127.0.0.1:8088"

    def get_cloud_nodes(self):
        base = self._get_management_api_base()
        url = f"{base}/nodes"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                payload = resp.read().decode('utf-8')
                data = json.loads(payload)
                return data.get('nodes', [])
        except Exception:
            return []

    def _reset_controller_channel(self):
        try:
            if self._ctrl_channel is not None:
                self._ctrl_channel.close()
        except Exception:
            pass
        self._ctrl_channel = None
        self._ctrl_stub = None

    def _build_stub_for(self, address: str, ready_timeout: int = 5):
        ch = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(ch).result(timeout=ready_timeout)
            return controller_pb2_grpc.ControllerServiceStub(ch), ch
        except Exception as e:
            ch.close()
            raise e

    def _get_controller_stub(self, ready_timeout: int = 5):
        # Reuse existing stub if available
        if self._ctrl_stub is not None:
            try:
                # Test the connection with a quick call
                return self._ctrl_stub
            except:
                self._reset_controller_channel()
        
        # Try configured address first
        try:
            stub, ch = self._build_stub_for(self.controller_address, ready_timeout)
            self._ctrl_channel = ch
            self._ctrl_stub = stub
            return stub
        except Exception:
            pass
        
        # Fallback between localhost/127.0.0.1 variants
        try:
            host, port = self.controller_address.split(":", 1)
            alt = None
            if host == "127.0.0.1":
                alt = f"localhost:{port}"
            elif host.lower() == "localhost":
                alt = f"127.0.0.1:{port}"
            if alt:
                stub, ch = self._build_stub_for(alt, ready_timeout)
                self._ctrl_channel = ch
                self._ctrl_stub = stub
                self.controller_address = alt  # normalize to working address
                return stub
        except Exception:
            pass
        
        raise grpc.RpcError("Controller channel not ready")

    def _register_with_controller(self):
        # If initial attempt fails, try to read controller_endpoint.json written by controller
        def _try_discovery():
            try:
                import json as _json
                # Search in CWD first
                paths = [os.path.join(os.getcwd(), 'controller_endpoint.json'),
                         os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'controller_endpoint.json')]
                for p in paths:
                    if os.path.isfile(p):
                        with open(p, 'r') as f:
                            data = _json.load(f)
                        host = data.get('host', '127.0.0.1')
                        port = data.get('port')
                        if port:
                            self.controller_address = f"{host}:{port}"
                            self.controller_address = self._normalize_address(self.controller_address)
                            self._reset_controller_channel()
                            return True
            except Exception:
                pass
            return False
        try:
            stub = self._get_controller_stub(ready_timeout=10)
            node_info = controller_pb2.NodeInfo(
                ip=self.ip,
                mac=self.mac,
                ram=self.ram,
                cpu=self.cpu,
                bandwidth=self.bandwidth,
                storage=self.storage,
                hdd=self.hdd,
                port=self.port
            )
            response = stub.RegisterNode(node_info, timeout=10)
            if getattr(response, 'success', False):
                self.registered = True
                self.logger.info(f"Node registered with controller: {self.controller_address}")
                return True
            else:
                self.registered = False
                self.logger.warning(f"Registration failed: {getattr(response, 'message', 'unknown error')}")
                return False
        except grpc.RpcError as e:
            self.registered = False
            self.logger.debug(f"Controller connection attempt failed: {e}")
            # Try discovery-based controller address if available
            if _try_discovery():
                try:
                    stub = self._get_controller_stub(ready_timeout=10)
                    node_info = controller_pb2.NodeInfo(
                        ip=self.ip,
                        mac=self.mac,
                        ram=self.ram,
                        cpu=self.cpu,
                        bandwidth=self.bandwidth,
                        storage=self.storage,
                        hdd=self.hdd,
                        port=self.port
                    )
                    response = stub.RegisterNode(node_info, timeout=10)
                    if getattr(response, 'success', False):
                        self.registered = True
                        self.logger.info(f"Node registered with controller (discovered): {self.controller_address}")
                        return True
                except Exception:
                    pass
            self._reset_controller_channel()
            return False
        except Exception as e:
            self.registered = False
            self.logger.debug(f"Unexpected error during registration: {str(e)}")
            self._reset_controller_channel()
            return False

    def _persistent_register(self):
        retry_count = 0
        max_retries = 10
        
        while not self.registered and not self._shutdown and retry_count < max_retries:
            retry_count += 1
            self.logger.info(f"Attempting to register with controller (attempt {retry_count}/{max_retries})...")
            
            if self._register_with_controller():
                print("✓ Node successfully registered with controller.")
                return
            
            # Exponential backoff with jitter
            wait_time = min(2 ** retry_count, 30) + (time.time() % 1)
            time.sleep(wait_time)
        
        if not self.registered:
            print(f"WARNING: Failed to register after {max_retries} attempts. Node will continue trying in background.")
            # Continue trying indefinitely but less frequently
            while not self.registered and not self._shutdown:
                time.sleep(30)  # Try every 30 seconds
                self._register_with_controller()

    def _heartbeat_loop(self):
        """Send periodic heartbeats to controller"""
        while not self._shutdown:
            time.sleep(10)  # Send heartbeat every 10 seconds
            if self.registered:
                try:
                    stub = self._get_controller_stub(ready_timeout=5)
                    heartbeat_req = controller_pb2.HeartbeatRequest(
                        ip=self.ip,
                        port=self.port,
                        is_active=True
                    )
                    response = stub.Heartbeat(heartbeat_req, timeout=5)
                    if not response.acknowledged:
                        self.logger.warning("Heartbeat not acknowledged, re-registering...")
                        self.registered = False
                        self._reset_controller_channel()
                except Exception as e:
                    self.logger.debug(f"Heartbeat failed: {e}")
                    self.registered = False
                    self._reset_controller_channel()
    
    def ReceiveFile(self, request_iterator, context):
        chunks = {}
        filename = None
        total_chunks = None
        
        try:
            for chunk in request_iterator:
                if filename is None:
                    filename = chunk.filename
                    total_chunks = chunk.total_chunks
                    self.logger.info(f"Receiving file '{filename}' with {total_chunks} chunks")
                
                if calculate_checksum(chunk.content) != chunk.checksum:
                    return node_pb2.UploadResponse(
                        success=False,
                        message=f"Checksum mismatch for chunk {chunk.chunk_number}"
                    )
                
                chunks[chunk.chunk_number] = chunk.content

            if total_chunks is None or len(chunks) != total_chunks:
                return node_pb2.UploadResponse(
                    success=False,
                    message=f"Incomplete file. Received {len(chunks)} of {total_chunks if total_chunks is not None else '?'} chunks"
                )

            file_path = os.path.join(self.storage_path, filename)
            with open(file_path, 'wb') as f:
                for i in range(total_chunks):
                    if i in chunks:
                        f.write(chunks[i])
                    else:
                        return node_pb2.UploadResponse(
                            success=False,
                            message=f"Missing chunk {i}"
                        )

            self.files.add(filename)
            self.logger.info(f"File '{filename}' received successfully")
            return node_pb2.UploadResponse(success=True, message="File received successfully")
            
        except Exception as e:
            self.logger.error(f"Error receiving file: {str(e)}")
            return node_pb2.UploadResponse(success=False, message=f"Error: {str(e)}")

    def SendFile(self, request, context):
        filename = request.filename
        file_path = os.path.join(self.storage_path, filename)
        
        if not os.path.exists(file_path):
            self.logger.error(f"File '{filename}' not found")
            yield node_pb2.FileChunk(
                filename=filename,
                content=b'',
                chunk_number=0,
                total_chunks=0,
                checksum=""
            )
            return

        try:
            chunks, total_chunks = split_file(file_path)
            for chunk_number, data, checksum in chunks:
                yield node_pb2.FileChunk(
                    filename=filename,
                    content=data,
                    chunk_number=chunk_number,
                    total_chunks=total_chunks,
                    checksum=checksum
                )
            self.logger.info(f"File '{filename}' sent successfully")
        except Exception as e:
            self.logger.error(f"Error sending file '{filename}': {str(e)}")

    def upload_file(self, file_path):
        if not self.registered:
            # Attempt quick re-registration to recover transient disconnects
            self._register_with_controller()
            if not self.registered:
                print("Cannot upload - node is not connected to controller")
                return False
            
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return False
            
        try:
            filename = os.path.basename(file_path)
            chunks, total_chunks = split_file(file_path)
            
            stub = self._get_controller_stub(ready_timeout=10)
            
            def chunk_generator():
                for chunk_number, data, checksum in chunks:
                    yield controller_pb2.FileChunk(
                        filename=filename,
                        content=data,
                        chunk_number=chunk_number,
                        total_chunks=total_chunks,
                        checksum=checksum
                    )
            
            response = stub.UploadFile(chunk_generator(), timeout=60)
            return response.success
            
        except Exception as e:
            print(f"Upload error: {str(e)}")
            self.registered = False
            self._reset_controller_channel()
            return False

    def download_file(self, filename, destination_path):
        if not self.registered:
            # Attempt quick re-registration to recover transient disconnects
            self._register_with_controller()
            if not self.registered:
                print("Cannot download - node is not connected to controller")
                return False
            
        try:
            stub = self._get_controller_stub(ready_timeout=10)
            
            # First, request file location
            location = stub.RequestFile(controller_pb2.FileRequest(filename=filename), timeout=10)
            if not location.is_online:
                print(f"File '{filename}' not available")
                return False
                
            # Download from the source
            if location.node_ip == "controller":
                # Download from controller
                chunks = stub.DownloadFile(controller_pb2.FileRequest(filename=filename), timeout=60)
            else:
                # Download from another node
                node_channel = grpc.insecure_channel(f"{location.node_ip}:{location.node_port}")
                try:
                    grpc.channel_ready_future(node_channel).result(timeout=10)
                    node_stub = node_pb2_grpc.NodeServiceStub(node_channel)
                    chunks = node_stub.SendFile(node_pb2.FileRequest(filename=filename), timeout=60)
                except Exception as e:
                    node_channel.close()
                    raise e
            
            # Reconstruct the file
            success = reconstruct_file(chunks, os.path.join(destination_path, filename))
            if success:
                self.files.add(filename)
            return success
            
        except Exception as e:
            print(f"Download error: {str(e)}")
            self.registered = False
            self._reset_controller_channel()
            return False

    def list_files(self):
        if not self.registered:
            # Attempt quick re-registration to recover transient disconnects
            self._register_with_controller()
            if not self.registered:
                print("Cannot list files - node is not connected to controller")
                return []
            
        try:
            stub = self._get_controller_stub(ready_timeout=10)
            response = stub.ListFiles(controller_pb2.Empty(), timeout=10)
            return list(response.filenames)
        except Exception as e:
            print(f"Error listing files: {str(e)}")
            self.registered = False
            self._reset_controller_channel()
            return []

    def get_local_files(self):
        """Enumerate files present in this node's local storage directory.
        Scans the storage path each time to include files replicated directly by the controller.
        """
        try:
            files = []
            for fname in os.listdir(self.storage_path):
                fpath = os.path.join(self.storage_path, fname)
                if os.path.isfile(fpath) and fname != self.health_check_file and not fname.startswith('temp_'):
                    files.append(fname)
            return sorted(files)
        except Exception as e:
            self.logger.warning(f"Failed to scan local storage: {e}")
            # Fallback to in-memory tracking if scan fails
            return sorted(list(self.files))

    def shutdown(self):
        """Graceful shutdown"""
        self._shutdown = True
        # Send a final offline heartbeat to controller so it shows immediately as offline.
        # Try multiple possible controller endpoints to handle port changes/restarts.
        endpoints = []
        # Current configured
        if isinstance(self.controller_address, str) and ':' in self.controller_address:
            endpoints.append(self.controller_address)
        # Discovered from controller_endpoint.json
        try:
            discovered = self._try_load_discovered_controller()
            if discovered:
                endpoints.append(discovered)
        except Exception:
            pass
        # 127.0.0.1/localhost swaps
        expanded = []
        for addr in endpoints:
            try:
                host, port = addr.split(':', 1)
                if host == '127.0.0.1':
                    expanded.append(f'localhost:{port}')
                elif host.lower() == 'localhost':
                    expanded.append(f'127.0.0.1:{port}')
            except Exception:
                continue
        endpoints.extend(expanded)
        # Deduplicate while preserving order
        seen = set()
        unique_endpoints = []
        for e in endpoints:
            if e and e not in seen:
                seen.add(e)
                unique_endpoints.append(e)
        # Attempt heartbeat to each endpoint (fallback to HTTP removal if gRPC schema mismatch)
        sent = False
        hb_req = None
        try:
            hb_req = controller_pb2.HeartbeatRequest(ip=self.ip, port=self.port, is_active=False)
        except AttributeError:
            hb_req = None
        for addr in unique_endpoints:
            if hb_req is None:
                break
            try:
                stub, ch = self._build_stub_for(addr, ready_timeout=3)
                try:
                    stub.Heartbeat(hb_req, timeout=3)
                    sent = True
                    break
                finally:
                    ch.close()
            except Exception:
                continue
        # If gRPC heartbeat could not be sent, try HTTP management API to remove the node
        if not sent:
            try:
                base = self._get_management_api_base()
                node_id = f"{self.ip}:{self.port}"
                url = f"{base}/nodes/remove?id={urllib.parse.quote(node_id)}"
                req = urllib.request.Request(url, method='POST', data=b'')
                with urllib.request.urlopen(req, timeout=3) as _:
                    pass
            except Exception:
                pass
        if self.server:
            self.server.stop(5)
        self._reset_controller_channel()

def main():
    print("Node server starting...")
    parser = argparse.ArgumentParser(description='Distributed Storage Node')
    parser.add_argument('--ip', required=True, help='Node IP address')
    parser.add_argument('--mac', required=True, help='Node MAC address')
    parser.add_argument('--ram', type=int, required=True, help='RAM in MB')
    parser.add_argument('--cpu', required=True, help='CPU info')
    parser.add_argument('--bandwidth', type=int, required=True, help='Bandwidth in Mbps')
    parser.add_argument('--storage', type=int, required=True, help='Storage capacity in GB')
    parser.add_argument('--hdd', required=True, choices=['SSD', 'HDD'], help='Storage type')
    parser.add_argument('--port', type=int, required=True, help='Port for this node')
    parser.add_argument('--controller', required=True, help='Controller address (host:port)')
    
    args = parser.parse_args()
    
    # Always use 127.0.0.1 for local IP
    ip = args.ip if str(args.ip).lower() != "localhost" else "127.0.0.1"
    controller_addr = args.controller
    if controller_addr.startswith("localhost:"):
        controller_addr = controller_addr.replace("localhost:", "127.0.0.1:")

    node = None
    try:
        node = NodeService(
            ip=ip,
            mac=args.mac,
            ram=args.ram,
            cpu=args.cpu,
            bandwidth=args.bandwidth,
            storage=args.storage,
            hdd=args.hdd,
            port=args.port,
            controller_address=controller_addr
        )
        print(f"Node server started on port {args.port}")
        print(f"Target controller: {node.controller_address}")
        
        # Wait a moment for initial registration
        time.sleep(2)
        
    except Exception as e:
        print(f"Failed to start node: {str(e)}")
        import traceback
        traceback.print_exc()
        return

    print("\nNode ready. Commands:")
    print("create <filename> <size_mb> - Create a file locally")
    print("upload <filename> - Upload a file to the cloud")
    print("download <filename> - Download a file from the cloud")
    print("list - List available files")
    print("nodes - List connected nodes")
    print("status - Show node connection status")
    print("exit - Shutdown node")

    while True:
        try:
            cmd = input("> ").strip().split()
            if not cmd:
                continue
                
            if cmd[0] == "create" and len(cmd) == 3:
                filename = cmd[1]
                try:
                    size_mb = int(cmd[2])
                    size_bytes = size_mb * 1024 * 1024
                    file_path = os.path.join(node.storage_path, filename)
                    if os.path.exists(file_path):
                        print(f"File '{filename}' already exists in local storage.")
                    else:
                        with open(file_path, "wb") as f:
                            chunk_size = 1024 * 1024
                            remaining = size_bytes
                            while remaining > 0:
                                write_size = min(chunk_size, remaining)
                                f.write(b'A' * write_size)
                                remaining -= write_size
                        if node:
                            node.files.add(filename)
                        print(f"File '{filename}' of size {size_mb} MB created in local storage.")
                except ValueError:
                    print("Size must be an integer (MB).")
                except Exception as e:
                    print(f"Error creating file: {str(e)}")
                    
            elif cmd[0] == "create" and len(cmd) != 3:
                print("Usage: create <filename> <size_mb>")
                continue
                
            elif cmd[0] == "upload" and len(cmd) == 2:
                arg = cmd[1]
                file_path = arg
                if not os.path.isabs(file_path) and not os.path.exists(file_path):
                    candidate = os.path.join(node.storage_path, arg)
                    if os.path.exists(candidate):
                        file_path = candidate
                if not os.path.exists(file_path):
                    print(f"File not found: {arg}")
                    continue
                start = time.time()
                success = node.upload_file(file_path)
                end = time.time()
                if success:
                    print(f"File '{os.path.basename(file_path)}' uploaded to cloud. Upload time: {end-start:.2f} seconds.")
                else:
                    print(f"Failed to upload '{os.path.basename(file_path)}'.")
                    
            elif cmd[0] == "upload" and len(cmd) != 2:
                print("Usage: upload <filename>")
                continue
                
            elif cmd[0] == "download" and len(cmd) == 2:
                filename = cmd[1]
                start = time.time()
                success = node.download_file(filename, node.storage_path)
                end = time.time()
                if success:
                    print(f"File '{filename}' downloaded to local storage. Download time: {end-start:.2f} seconds.")
                else:
                    print(f"Failed to download '{filename}'.")
                    
            elif cmd[0] == "download" and len(cmd) != 2:
                print("Usage: download <filename>")
                continue
                
            elif cmd[0] == "list":
                cloud_files = node.list_files()
                local_files = node.get_local_files() if node else []
                print("\nFiles available in the cloud:")
                for f in cloud_files:
                    print(f"- {f}")
                print("\nFiles stored locally on this node:")
                for f in local_files:
                    print(f"- {f}")
                    
            elif cmd[0] == "nodes":
                nodes_info = node.get_cloud_nodes()
                print("\nNodes in the cloud:")
                if not nodes_info:
                    print("(none)")
                for n in nodes_info:
                    status = "Online" if n.get("online") else "Offline"
                    nid = n.get("id", "")
                    cpu = n.get("cpu", "")
                    ram = n.get("ram", 0)
                    print(f"- {nid} [{status}] {cpu} | {ram}MB RAM")
            elif cmd[0] == "status":
                if node.registered:
                    print(f"✓ Connected to controller: {node.controller_address}")
                else:
                    print(f"✗ Not connected to controller: {node.controller_address}")
                    
            elif cmd[0] == "exit":
                print("Shutting down node...")
                break
                
            elif cmd[0] == "set-controller" and len(cmd) == 2:
                new_controller = cmd[1]
                if node:
                    node.controller_address = node._normalize_address(new_controller)
                    node._reset_controller_channel()
                    node.registered = False
                    node._register_with_controller()
                    if node.registered:
                        print(f"Node successfully registered with new controller: {node.controller_address}")
                    else:
                        print(f"Failed to register with controller: {node.controller_address}")
                else:
                    print("Node instance not found.")
            else:
                print("Invalid command. Available commands: create, upload, download, list, status, exit, set-controller <host:port>")
                
        except KeyboardInterrupt:
            print("\nShutting down node...")
            break
        except Exception as e:
            print(f"CLI error: {str(e)}")

    # Graceful shutdown
    if node:
        node.shutdown()

if __name__ == '__main__':
    main()