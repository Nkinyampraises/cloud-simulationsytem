import os
import hashlib
import time
import logging
from functools import wraps

def setup_logging(name):
    """Setup logging configuration"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def calculate_checksum(data):
    """Calculate MD5 checksum for data"""
    return hashlib.md5(data).hexdigest()

def split_file(file_path, chunk_size=1024*1024):  # 1MB chunks
    """Split file into chunks with checksums"""
    chunks = []
    total_chunks = 0
    
    with open(file_path, 'rb') as f:
        chunk_number = 0
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            checksum = calculate_checksum(data)
            chunks.append((chunk_number, data, checksum))
            chunk_number += 1
        total_chunks = chunk_number
    
    return chunks, total_chunks

def reconstruct_file(chunks, output_path):
    """Reconstruct file from chunks"""
    # Sort chunks by chunk number
    chunks.sort(key=lambda x: x[0])
    
    with open(output_path, 'wb') as f:
        for chunk_number, data, checksum in chunks:
            # Verify checksum
            if calculate_checksum(data) != checksum:
                raise ValueError(f"Checksum mismatch for chunk {chunk_number}")
            f.write(data)

def timeit(func):
    """Decorator to measure execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper