import os
import sys
import time
import subprocess
import psutil
import requests

PORT=8969

def run_uvicorn_with_monitor():
    """
    Run fastapi_app.py with uvicorn and monitor memory usage via /memusage endpoint. Restart if usage > 450MB.
    """
    while True:
        # Start the server process in a new terminal on Linux, normal on Windows
        if sys.platform.startswith('linux'):
            # Try common terminal emulators
            terminal_cmds = [
                ['x-terminal-emulator', '-e'],
                ['gnome-terminal', '--'],
                ['konsole', '-e'],
                ['xterm', '-e']
            ]
            uvicorn_cmd = [sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
                          '--host', '0.0.0.0', '--port', str(PORT), '--log-level', 'info']
            for term in terminal_cmds:
                try:
                    process = subprocess.Popen(term + uvicorn_cmd)
                    print(f"[INIT] Started uvicorn server in new terminal (PID: {process.pid})")
                    break
                except FileNotFoundError:
                    continue
            else:
                print("[INIT] No supported terminal emulator found. Starting in current process.")
                process = subprocess.Popen(uvicorn_cmd)
        else:
            process = subprocess.Popen([
                sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
                '--host', '0.0.0.0', '--port', str(PORT), '--log-level', 'info'
            ])
            print(f"[INIT] Started uvicorn server (PID: {process.pid})")
        print(f"[INIT] Started uvicorn server (PID: {process.pid})")
        try:
            while True:
                time.sleep(60)
                health_failed = False
                # Check /healthz endpoint
                try:
                    health_resp = requests.get(f'http://127.0.0.1:{PORT}/healthz', timeout=10)
                    if health_resp.status_code != 200:
                        print(f"[INIT] /healthz returned status {health_resp.status_code}, restarting server...")
                        health_failed = True
                    else:
                        health_data = health_resp.json()
                        if health_data.get('status') not in ['healthy', 'degraded']:
                            print(f"[INIT] /healthz unhealthy: {health_data}")
                            health_failed = True
                except requests.RequestException as e:
                    print(f"[INIT] Error querying /healthz: {e}")
                    health_failed = True
                except Exception as e:
                    print(f"[INIT] Unexpected error on /healthz: {e}")
                    health_failed = True

                if health_failed:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except Exception:
                        pass
                    if process.poll() is None:
                        process.kill()
                    break

                # Query the FastAPI /memusage endpoint for memory usage
                try:
                    resp = requests.get(f'http://127.0.0.1:{PORT}/memusage', timeout=10)
                    if resp.status_code == 200:
                        mem_data = resp.json()
                        mem_mb = mem_data['process']['rss_mb']
                        print(f"[INIT] Server memory usage (via /memusage): {mem_mb:.1f} MB")
                        if mem_mb > 450:
                            print("[INIT] Memory usage exceeded 450MB, restarting server...")
                            process.terminate()
                            try:
                                process.wait(timeout=10)
                            except Exception:
                                pass
                            if process.poll() is None:
                                process.kill()
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
            try:
                process.wait(timeout=10)
            except Exception:
                pass
            if process.poll() is None:
                process.kill()
        time.sleep(2)  # Short delay before restart

if __name__ == "__main__":
    run_uvicorn_with_monitor()
