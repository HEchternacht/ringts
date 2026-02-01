import os
import sys
import time
import subprocess
import psutil
import requests

PORT=8939

def run_uvicorn_with_monitor():
    """
    Run fastapi_app.py with uvicorn and monitor memory usage via /memusage endpoint. Restart if usage > 450MB.
    """
    while True:
        uvicorn_pid = None
        # Start the server process in a new terminal on Linux, normal on Windows
        if sys.platform.startswith('linux'):
            ###############################################################################################################
            # Try common terminal emulators, keep terminal open after process exits
            uvicorn_cmd_str = f"{sys.executable} -m uvicorn fastapi_app:app --host 0.0.0.0 --port {PORT} --log-level info"
            terminal_cmds = [
                ['x-terminal-emulator', '-e', 'bash', '-c', f"{uvicorn_cmd_str}; exec bash"],
                ['gnome-terminal', '--', 'bash', '-c', f"{uvicorn_cmd_str}; exec bash"],
                ['konsole', '-e', 'bash', '-c', f"{uvicorn_cmd_str}; exec bash"],
                ['xterm', '-e', 'bash', '-c', f"{uvicorn_cmd_str}; exec bash"]
            ]
            for term in terminal_cmds:
                try:
                    process = subprocess.Popen(term)
                    print(f"[INIT] Started uvicorn server in new terminal (Terminal PID: {process.pid})")
                    # Wait a bit for uvicorn to start, then find its PID
                    time.sleep(3)
                    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                        try:
                            cmdline = proc.info['cmdline']
                            if cmdline and 'uvicorn' in ' '.join(cmdline) and str(PORT) in ' '.join(cmdline):
                                uvicorn_pid = proc.info['pid']
                                print(f"[INIT] Tracked uvicorn process PID: {uvicorn_pid}")
                                break
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue
                    break
                except FileNotFoundError:
                    continue
            else:
                print("[INIT] No supported terminal emulator found. Starting in current process.")
                process = subprocess.Popen([sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
                                           '--host', '0.0.0.0', '--port', str(PORT), '--log-level', 'info'])
                uvicorn_pid = process.pid
        else:
            process = subprocess.Popen([
                sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
                '--host', '0.0.0.0', '--port', str(PORT), '--log-level', 'info'
            ])
            uvicorn_pid = process.pid
            print(f"[INIT] Started uvicorn server (PID: {process.pid})")
        print(f"[INIT] Monitoring uvicorn PID: {uvicorn_pid}")
            ###############################################################################################################

        try:
            while True:
                time.sleep(60)
                health_failed = False
                force_kill = False
                # Check /healthz endpoint
                ###############################################################################################################
                try:
                    health_resp = requests.get(f'http://127.0.0.1:{PORT}/healthz', timeout=10)
                    if health_resp.status_code == 500:
                        print(f"[INIT] /healthz returned 500 - forcefully killing server...")
                        health_failed = True
                        force_kill = True
                    elif health_resp.status_code != 200:
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
                    if force_kill and sys.platform.startswith('linux') and uvicorn_pid:
                        # Forcefully kill the tracked uvicorn process on Linux
                        try:
                            print(f"[INIT] Force killing uvicorn process {uvicorn_pid}")
                            proc = psutil.Process(uvicorn_pid)
                            proc.kill()  # SIGKILL
                            proc.wait(timeout=5)
                            print(f"[INIT] Successfully killed uvicorn process {uvicorn_pid}")
                        except psutil.NoSuchProcess:
                            print(f"[INIT] Process {uvicorn_pid} already terminated")
                        except Exception as e:
                            print(f"[INIT] Error killing process {uvicorn_pid}: {e}")
                    
                    # Also terminate the terminal process
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except Exception:
                        pass
                    if process.poll() is None:
                        process.kill()
                    break
                        ###############################################################################################################

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
