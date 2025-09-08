#!/usr/bin/env python3
"""
startup.py - Run both FastAPI server and Kafka worker
"""
import os
import sys
import signal
import subprocess
import time
import requests
from multiprocessing import Process

def start_fastapi():
    """Start the FastAPI server"""
    print("[STARTUP] Starting FastAPI server...")
    subprocess.run([
        "uvicorn", "app.main:app", 
        "--host", "0.0.0.0", 
        "--port", "8000"
    ])

def start_worker():
    """Start the Kafka worker"""
    print("[STARTUP] Starting Kafka worker...")
    subprocess.run([sys.executable, "worker/worker.py"])

def wait_for_fastapi():
    """Wait for FastAPI to be ready"""
    print("[STARTUP] Waiting for FastAPI to start...")
    for i in range(60):  # 60 second timeout
        try:
            response = requests.get("http://localhost:8000/health", timeout=1)
            if response.status_code == 200:
                print("[STARTUP] FastAPI is ready!")
                return True
        except:
            pass
        time.sleep(1)
    
    print("[STARTUP] ERROR: FastAPI failed to start within 60 seconds")
    return False

def main():
    print("[STARTUP] Starting inference service...")
    
    # Start FastAPI in a separate process
    fastapi_process = Process(target=start_fastapi)
    fastapi_process.start()
    
    # Wait for FastAPI to be ready
    if not wait_for_fastapi():
        fastapi_process.terminate()
        sys.exit(1)
    
    # Start Kafka worker in a separate process
    worker_process = Process(target=start_worker)
    worker_process.start()
    
    print(f"[STARTUP] Both services started. FastAPI PID: {fastapi_process.pid}, Worker PID: {worker_process.pid}")
    
    def signal_handler(signum, frame):
        print(f"[STARTUP] Received signal {signum}, shutting down...")
        worker_process.terminate()
        fastapi_process.terminate()
        worker_process.join(timeout=5)
        fastapi_process.join(timeout=5)
        sys.exit(0)
    
    # Set up signal handling
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Wait for processes
        fastapi_process.join()
        worker_process.join()
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    main()