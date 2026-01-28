import os
import sys
import time
import subprocess
import psutil

def run_uvicorn_with_monitor():
    """
    Run fastapi_app.py with uvicorn and monitor memory usage. Restart if usage > 450MB.
    """
    while True:
        # Start the server process
        process = subprocess.Popen([
            sys.executable, '-m', 'uvicorn', 'fastapi_app:app',
            '--host', '0.0.0.0', '--port', '5000', '--log-level', 'info'
        ])
        print("[INIT] Started uvicorn server (PID: %d)" % process.pid)
        try:
            while True:
                time.sleep(60)
                try:
                    p = psutil.Process(process.pid)
                    mem_mb = p.memory_info().rss / 1024 / 1024
                    print(f"[INIT] Server memory usage: {mem_mb:.1f} MB")
                    if mem_mb > 450:
                        print("[INIT] Memory usage exceeded 450MB, restarting server...")
                        process.terminate()
                        process.wait(timeout=10)
                        break
                except psutil.NoSuchProcess:
                    print("[INIT] Server process ended.")
                    break
        except Exception as e:
            print(f"[INIT] Monitor error: {e}")
            process.terminate()
            process.wait(timeout=10)
        time.sleep(2)  # Short delay before restart

if __name__ == "__main__":
    run_uvicorn_with_monitor()
"""
RINGTS V2 - Tibia Character Tracking System
Simplified, Reliable, and Straightforward
"""

__version__ = "2.0.0"
__author__ = "RINGTS Team"

from .database import Database
from .scraper import scrape_character, scrape_status
from .data_processor import process_character
from .analytics import (
    get_top_xp_players,
    get_top_online_players,
    get_top_killers,
    get_most_deaths,
    get_character_summary,
    get_top_xp_delta_players,
    get_top_online_delta_players,
    get_character_xp_history,
    get_character_online_history,
    get_character_delta_summary,
    export_to_csv
)

__all__ = [
    'Database',
    'scrape_character',
    'scrape_status',
    'process_character',
    'get_top_xp_players',
    'get_top_online_players',
    'get_top_killers',
    'get_most_deaths',
    'get_character_summary',
    'get_top_xp_delta_players',
    'get_top_online_delta_players',
    'get_character_xp_history',
    'get_character_online_history',
    'get_character_delta_summary',
    'export_to_csv',
]
