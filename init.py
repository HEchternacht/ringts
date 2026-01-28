import os
import sys
import time
import subprocess
import psutil
import requests


def run_uvicorn_with_monitor():
    """
    Run fastapi_app.py with uvicorn and monitor memory usage via /memusage endpoint. Restart if usage > 450MB.
    """
    while True:
        # Start the server process
        process = subprocess.Popen([
            sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
            '--host', '0.0.0.0', '--port', '5000', '--log-level', 'info'
        ])
        print(f"[INIT] Started uvicorn server (PID: {process.pid})")
        try:
            while True:
                time.sleep(60)
                try:
                    # Query the FastAPI /memusage endpoint for memory usage
                    resp = requests.get('http://127.0.0.1:5000/memusage', timeout=10)
                    if resp.status_code == 200:
                        mem_data = resp.json()
                        mem_mb = mem_data['process']['rss_mb']
                        print(f"[INIT] Server memory usage (via /memusage): {mem_mb:.1f} MB")
                        if mem_mb > 450:
                            print("[INIT] Memory usage exceeded 450MB, restarting server...")
                            process.terminate()
                            process.wait(timeout=10)
                            break
                    else:
                        print(f"[INIT] /memusage endpoint returned status {resp.status_code}")
                except requests.RequestException as e:
                    print(f"[INIT] Error querying /memusage: {e}")
                except Exception as e:
                    print(f"[INIT] Unexpected error: {e}")
                # Also check if process is still alive
                if process.poll() is not None:
                    print("[INIT] Server process ended.")
                    break
        except Exception as e:
            print(f"[INIT] Monitor error: {e}")
            process.terminate()
            process.wait(timeout=10)
        time.sleep(2)  # Short delay before restart

if __name__ == "__main__":
    run_uvicorn_with_monitor()
