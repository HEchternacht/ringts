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
        terminal_pid = None
        # Start the server process in a new terminal on Linux, normal on Windows
        if sys.platform.startswith('linux'):
            ###############################################################################################################
            # Try common terminal emulators, keep terminal open after process exits
            uvicorn_cmd_str = f"{sys.executable} -m uvicorn fastapi_app:app --host 0.0.0.0 --port {PORT} --log-level info"
            terminal_cmds = [
                ['x-terminal-emulator', '-e', 'bash', '-c', f"{uvicorn_cmd_str}"],
                ['gnome-terminal', '--', 'bash', '-c', f"{uvicorn_cmd_str}; exec bash"],
                ['konsole', '-e', 'bash', '-c', f"{uvicorn_cmd_str}; exec bash"],
                ['xterm', '-e', 'bash', '-c', f"{uvicorn_cmd_str}; exec bash"]
            ]
            for term in terminal_cmds:
                try:
                    terminal_process = subprocess.Popen(term)
                    terminal_pid = terminal_process.pid
                    print(f"[INIT] Started uvicorn server in new terminal (Terminal PID: {terminal_pid})")
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
                terminal_process = subprocess.Popen([sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
                                           '--host', '0.0.0.0', '--port', str(PORT), '--log-level', 'info'])
                uvicorn_pid = terminal_process.pid
                terminal_pid = terminal_process.pid
        else:
            terminal_process = subprocess.Popen([
                sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
                '--host', '0.0.0.0', '--port', str(PORT), '--log-level', 'info'
            ])
            uvicorn_pid = terminal_process.pid
            terminal_pid = terminal_process.pid
            print(f"[INIT] Started uvicorn server (PID: {terminal_process.pid})")
        print(f"[INIT] Monitoring uvicorn PID: {uvicorn_pid}")
            ###############################################################################################################

        try:
            while True:
                time.sleep(60)
                
                # Check if uvicorn process is still alive
                if uvicorn_pid:
                    try:
                        uvicorn_proc = psutil.Process(uvicorn_pid)
                        if not uvicorn_proc.is_running():
                            print(f"[INIT] Uvicorn process {uvicorn_pid} died, restarting...")
                            break
                    except psutil.NoSuchProcess:
                        print(f"[INIT] Uvicorn process {uvicorn_pid} no longer exists, restarting...")
                        break
                
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
                    # Kill the uvicorn process
                    if uvicorn_pid:
                        try:
                            print(f"[INIT] {'Force ' if force_kill else ''}Killing uvicorn process {uvicorn_pid}")
                            proc = psutil.Process(uvicorn_pid)
                            if force_kill:
                                proc.kill()  # SIGKILL
                            else:
                                proc.terminate()  # SIGTERM
                            proc.wait(timeout=5)
                            print(f"[INIT] Successfully killed uvicorn process {uvicorn_pid}")
                        except psutil.NoSuchProcess:
                            print(f"[INIT] Process {uvicorn_pid} already terminated")
                        except psutil.TimeoutExpired:
                            print(f"[INIT] Process {uvicorn_pid} didn't terminate, force killing...")
                            proc.kill()
                            proc.wait(timeout=5)
                        except Exception as e:
                            print(f"[INIT] Error killing process {uvicorn_pid}: {e}")
                    
                    # Also terminate the terminal process if different
                    if terminal_pid and terminal_pid != uvicorn_pid:
                        try:
                            terminal_proc = psutil.Process(terminal_pid)
                            terminal_proc.terminate()
                            terminal_proc.wait(timeout=5)
                        except Exception:
                            pass
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
                            # Kill the uvicorn process
                            if uvicorn_pid:
                                try:
                                    proc = psutil.Process(uvicorn_pid)
                                    proc.terminate()
                                    proc.wait(timeout=10)
                                    if proc.is_running():
                                        proc.kill()
                                except psutil.NoSuchProcess:
                                    pass
                                except Exception as e:
                                    print(f"[INIT] Error terminating process: {e}")
                            
                            # Also terminate terminal if different
                            if terminal_pid and terminal_pid != uvicorn_pid:
                                try:
                                    terminal_proc = psutil.Process(terminal_pid)
                                    terminal_proc.terminate()
                                except Exception:
                                    pass
                            break
                    else:
                        print(f"[INIT] /memusage endpoint returned status {resp.status_code}")
                except requests.RequestException as e:
                    print(f"[INIT] Error querying /memusage: {e}")
                except Exception as e:
                    print(f"[INIT] Unexpected error: {e}")
          
        except Exception as e:
            print(f"[INIT] Monitor error: {e}")
            # Kill uvicorn process on error
            if uvicorn_pid:
                try:
                    proc = psutil.Process(uvicorn_pid)
                    proc.terminate()
                    proc.wait(timeout=10)
                    if proc.is_running():
                        proc.kill()
                except Exception:
                    pass
            
            # Kill terminal if different
            if terminal_pid and terminal_pid != uvicorn_pid:
                try:
                    terminal_proc = psutil.Process(terminal_pid)
                    terminal_proc.terminate()
                except Exception:
                    pass
        time.sleep(2)  # Short delay before restart

if __name__ == "__main__":
    run_uvicorn_with_monitor()
