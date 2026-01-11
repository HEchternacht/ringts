import os
import sys
import threading
import time
import queue
import json
import gc
from io import StringIO
from fastapi import FastAPI, HTTPException, Request, Query, File, UploadFile, Form, Path as PathParam
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from typing import Optional, List
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import bs4
import traceback
import httpx
from pebble import ThreadPool
from concurrent.futures import TimeoutError, as_completed
import psutil
import asyncio
from pydantic import BaseModel
from database_sqlalchemy import SQLAlchemyDatabase
# Configure aggressive garbage collection for memory efficiency
gc.set_threshold(700, 10, 5)
gc.enable()

app = FastAPI(title="Ring TS API", version="2.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Templates MUST be initialized before mounting static files
templates = Jinja2Templates(directory="templates")

# Mount static files immediately after templates
app.mount("/static", StaticFiles(directory="static"), name="static")

# Configuration from environment variables
UPLOAD_PASSWORD = os.environ.get('UPLOAD_PASSWORD', 'Rollabostx1234')
DEFAULT_WORLD = os.environ.get('DEFAULT_WORLD', 'Auroria')
DEFAULT_GUILD = os.environ.get('DEFAULT_GUILD', 'Ascended Auroria')
DATA_FOLDER = os.environ.get('DATA_FOLDER', 'var/data')
TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
DAILY_RESET_HOUR = int(os.environ.get('DAILY_RESET_HOUR', '10'))
DAILY_RESET_MINUTE = int(os.environ.get('DAILY_RESET_MINUTE', '2'))
MAX_MEMORY_MB = 350

FORCE_PROXY = True if os.environ.get('FORCE_PROXY', None) == 'true' else False

# Pydantic models for request/response
class GraphRequest(BaseModel):
    names: List[str]
    datetime1: Optional[str] = None
    datetime2: Optional[str] = None

class StatsRequest(BaseModel):
    names: List[str]
    datetime1: Optional[str] = None
    datetime2: Optional[str] = None

class RankingsRequest(BaseModel):
    datetime1: Optional[str] = None
    datetime2: Optional[str] = None

class ScrapingConfigUpdate(BaseModel):
    password: str
    config: List[dict]

class VIPAdd(BaseModel):
    name: str
    world: str

class VIPRemove(BaseModel):
    name: str
    world: str

class VIPGraphRequest(BaseModel):
    name: str
    world: str

pp = ['http://103.155.62.141:8081',
      'http://45.177.16.137:999',
      'http://190.242.157.215:8080',
      'http://187.102.219.64:999',
      'http://41.128.72.147:1981',
      'http://62.113.119.14:8080',
      'http://59.6.25.118:3128',
      'http://101.47.16.15:7890',
      'http://154.3.236.202:3128',
      'http://194.26.141.202:3128',
      'http://205.164.192.115:999']


def get_multiple(url: str, proxies: list):
    tic = time.time()
    success_flag = threading.Event()
    pool = None

    def get_resp(url, proxy):
        if success_flag.is_set():
            print(f"Skipping {proxy} - already got success")
            return None

        try:
            with httpx.Client(proxy=proxy) as client:
                tic_req = time.time()
                print(f"Sending request via proxy: {proxy}")

                if success_flag.is_set():
                    return None

                response = client.get(url, timeout=30)

                if success_flag.is_set():
                    return None

                toc_req = time.time()
                print(f"Response time via proxy {proxy}: {toc_req - tic_req:.2f}s")
                print(f"Received response via proxy: {proxy} with status code {response.status_code}")

                return {
                    "object": response,
                    'status_code': response.status_code,
                    'proxy': proxy,
                    'time': toc_req - tic_req
                }
        except Exception as e:
            if not success_flag.is_set():
                print(f"Error with {proxy}: {str(e)}")
            return None

    pool = ThreadPool(max_workers=40)

    try:
        futures = [pool.schedule(get_resp, args=(url, proxy)) for proxy in proxies]

        for future in as_completed(futures):
            if success_flag.is_set():
                break

            try:
                result = future.result(timeout=0.01)

                if result and isinstance(result, dict) and result.get('status_code') == 200:
                    toc = time.time()
                    print(f"\n✓ SUCCESS! Total time: {toc - tic:.2f}s")
                    print(f"✓ Successful response via proxy: {result['proxy']}")

                    success_flag.set()
                    gc.collect()
                    return result['object']

            except TimeoutError:
                pass
            except Exception as e:
                pass

    finally:
        if pool is not None:
            pool.close()
            del pool
            gc.collect()
    return None


# Console log queue for real-time display
console_queue = queue.Queue()
delta_queue = queue.Queue()
scraper_running = False
scraper_state = "idle"
scraper_lock = threading.Lock()
last_status_check = None
scraper_thread = None


class Database:
    """Database abstraction layer for storing player EXP data."""

    def __init__(self, folder=None):
        if folder is None:
            folder = DATA_FOLDER
        self.folder = folder
        self.exps_file = f"{folder}/exps.csv"
        self.deltas_file = f"{folder}/deltas.csv"
        self.reset_date_file = f"{folder}/last_reset.txt"
        self.status_data_file = f"{folder}/status_data.json"
        self.scraping_data_file = f"{folder}/scraping_data.json"
        self.vips_file = f"{folder}/vips.txt"
        self.vipsdata_file = f"{folder}/vipsdata.csv"
        self.deltavip_file = f"{folder}/deltavip.csv"
        self.lock = threading.Lock()
        self.reset_done_today = False

        if not os.path.exists(folder):
            os.makedirs(folder)

        self._initialize_scraping_config()
        self._initialize_vip_files()

    def _read_exps(self):
        try:
            df = pd.read_csv(self.exps_file, dtype={'name': str, 'exp': 'int64', 'world': str, 'guild': str},
                             parse_dates=['last update'])
            if 'exp' not in df.select_dtypes(include=['int']).columns:
                df['exp'] = df['exp'].astype('int64')

            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                log_console(f"Migrated exps: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                log_console(f"Migrated exps: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
                self._write_exps(df)

            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'exp', 'last update', 'world', 'guild'])

    def _read_deltas(self):
        try:
            df = pd.read_csv(self.deltas_file,
                             dtype={'name': str, 'deltaexp': 'int64', 'world': str, 'guild': str},
                             parse_dates=['update time'])
            if 'deltaexp' not in df.select_dtypes(include=['int']).columns:
                df['deltaexp'] = df['deltaexp'].astype('int64')

            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                log_console(f"Migrated deltas: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                log_console(f"Migrated deltas: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
                self._write_deltas(df)

            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'deltaexp', 'update time', 'world', 'guild'])

    def _write_exps(self, df):
        df.to_csv(self.exps_file, index=False)

    def _write_deltas(self, df):
        df.to_csv(self.deltas_file, index=False)

    def _read_status_data(self):
        try:
            with open(self.status_data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    def _write_status_data(self, data):
        with open(self.status_data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

    def _initialize_scraping_config(self):
        if not os.path.exists(self.scraping_data_file):
            default_config = [
                {
                    'world': DEFAULT_WORLD,
                    'guilds': [DEFAULT_GUILD]
                }
            ]
            with open(self.scraping_data_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2)
            log_console(f"Initialized scraping config with default: {DEFAULT_WORLD} - {DEFAULT_GUILD}", "INFO")

    def _initialize_vip_files(self):
        if not os.path.exists(self.vips_file):
            with open(self.vips_file, 'w', encoding='utf-8') as f:
                json.dump([], f)
            log_console("Initialized vips.txt", "INFO")

        if not os.path.exists(self.vipsdata_file):
            pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online']).to_csv(self.vipsdata_file,
                                                                                         index=False)
            log_console("Initialized vipsdata.csv", "INFO")

        if not os.path.exists(self.deltavip_file):
            pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time']).to_csv(
                self.deltavip_file, index=False)
            log_console("Initialized deltavip.csv", "INFO")

    def get_exps(self):
        with self.lock:
            return self._read_exps()

    def get_deltas(self):
        with self.lock:
            return self._read_deltas()

    def get_status_data(self):
        with self.lock:
            return self._read_status_data()

    def get_scraping_config(self):
        try:
            with open(self.scraping_data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return [{'world': DEFAULT_WORLD, 'guilds': [DEFAULT_GUILD]}]

    def save_scraping_config(self, config):
        with open(self.scraping_data_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

    def get_vips(self):
        try:
            with open(self.vips_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    return []
                
                # Try JSON first
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    # Fall back to CSV format (legacy)
                    log_console("Migrating VIPs from CSV to JSON format", "INFO")
                    vips = []
                    for line in content.split('\n'):
                        line = line.strip()
                        if line and ',' in line:
                            parts = line.split(',', 1)
                            if len(parts) == 2:
                                vips.append({'name': parts[0].strip(), 'world': parts[1].strip()})
                    
                    # Save in JSON format
                    if vips:
                        with open(self.vips_file, 'w', encoding='utf-8') as fw:
                            json.dump(vips, fw, indent=2)
                    return vips
        except FileNotFoundError:
            return []

    def add_vip(self, name, world):
        vips = self.get_vips()
        if any(v['name'] == name and v['world'] == world for v in vips):
            return False
        vips.append({'name': name, 'world': world})
        with open(self.vips_file, 'w', encoding='utf-8') as f:
            json.dump(vips, f, indent=2)
        return True

    def remove_vip(self, name, world):
        vips = self.get_vips()
        new_vips = [v for v in vips if not (v['name'] == name and v['world'] == world)]
        if len(new_vips) == len(vips):
            return False
        with open(self.vips_file, 'w', encoding='utf-8') as f:
            json.dump(new_vips, f, indent=2)
        return True

    def get_vipsdata(self):
        try:
            return pd.read_csv(self.vipsdata_file)
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])

    def get_deltavip(self):
        try:
            df = pd.read_csv(self.deltavip_file, parse_dates=['update_time'])
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])

    def update_vipdata(self, name, world, today_exp, today_online):
        vipsdata = self.get_vipsdata()
        mask = (vipsdata['name'] == name) & (vipsdata['world'] == world)
        if mask.any():
            vipsdata.loc[mask, 'today_exp'] = today_exp
            vipsdata.loc[mask, 'today_online'] = today_online
        else:
            new_row = pd.DataFrame([{'name': name, 'world': world, 'today_exp': today_exp, 'today_online': today_online}])
            vipsdata = pd.concat([vipsdata, new_row], ignore_index=True)
        vipsdata.to_csv(self.vipsdata_file, index=False)

    def add_vip_delta(self, name, world, date, delta_exp, delta_online, update_time):
        deltavip = self.get_deltavip()
        new_row = pd.DataFrame([{
            'name': name,
            'world': world,
            'date': date,
            'delta_exp': delta_exp,
            'delta_online': delta_online,
            'update_time': update_time
        }])
        deltavip = pd.concat([deltavip, new_row], ignore_index=True)
        deltavip.to_csv(self.deltavip_file, index=False)
        log_console(f"VIP delta: {name} ({world}) +{delta_exp} exp, +{delta_online} online", "INFO")

    def save_status_data(self, data):
        """Save status data with lock"""
        with self.lock:
            self._write_status_data(data)

    def load(self, folder=None):
        """Initialize database"""
        if folder:
            self.folder = folder
            self.exps_file = f"{folder}/exps.csv"
            self.deltas_file = f"{folder}/deltas.csv"
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        with self.lock:
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            if not deltas.empty:
                duplicates = deltas[deltas.duplicated(subset=['name', 'update time'], keep=False)]
                if not duplicates.empty:
                    original_count = len(deltas)
                    deltas = deltas.drop_duplicates(subset=['name', 'update time'], keep='last')
                    removed = original_count - len(deltas)
                    log_console(f"Found and removed {removed} duplicate deltas on load", "WARNING")
                    self._write_deltas(deltas)
        
        log_console(f"Database initialized: {len(exps)} players, {len(deltas)} deltas")

    def save(self):
        """Save database"""
        log_console("Database persisted to CSV files")

    def update(self, df, update_time):
        """Update player EXP data and record deltas"""
        with self.lock:
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            if not deltas.empty:
                distinct_times = deltas['update time'].unique()
                prev_times = [t for t in distinct_times if t < update_time]
                prev_update_time = max(prev_times) if prev_times else update_time
            else:
                prev_update_time = update_time

            exps_dict = exps.set_index('name')[['exp']].to_dict('index')
            deltas_set = set(zip(deltas['name'], deltas['update time']))
            
            new_deltas = []
            new_exps = []
            exps_updates = {}
            deltas_updates = {}

            for row in df.itertuples(index=False):
                name = row.name
                exp = int(row.exp)
                last_update = getattr(row, 'last_update', getattr(row, '_2', None))
                world = getattr(row, 'world', DEFAULT_WORLD)
                guild = getattr(row, 'guild', DEFAULT_GUILD)

                if name in exps_dict:
                    prev_exp = exps_dict[name]['exp']
                    deltaexp = exp - prev_exp
                    if deltaexp != 0:
                        delta_key = (name, update_time)
                        if delta_key not in deltas_set:
                            new_deltas.append({
                                'name': name, 
                                'deltaexp': deltaexp, 
                                'update time': update_time,
                                'world': world,
                                'guild': guild
                            })
                            log_console(f"EXP gain: {name} +{deltaexp} ({world} - {guild})")
                        else:
                            deltas_updates[delta_key] = deltaexp
                            log_console(f"Updated duplicate for {name} at {update_time} (latest)", "INFO")
                        
                        delta_queue.put({
                            'name': name,
                            'deltaexp': int(deltaexp),
                            'update_time': update_time.isoformat(),
                            'prev_update_time': prev_update_time.isoformat(),
                            'world': world,
                            'guild': guild
                        })
                    
                    exps_updates[name] = {'exp': exp, 'last update': last_update, 'world': world, 'guild': guild}
                else:
                    new_exps.append({
                        'name': name, 
                        'exp': exp, 
                        'last update': last_update,
                        'world': world,
                        'guild': guild
                    })
                    
                    delta_key = (name, update_time)
                    if delta_key not in deltas_set:
                        new_deltas.append({
                            'name': name, 
                            'deltaexp': exp, 
                            'update time': update_time,
                            'world': world,
                            'guild': guild
                        })
                        log_console(f"New player: {name} with {exp} EXP ({world} - {guild})")
                    else:
                        deltas_updates[delta_key] = exp
                        log_console(f"Updated duplicate for new player {name} at {update_time} (latest)", "INFO")
                    
                    delta_queue.put({
                        'name': name,
                        'deltaexp': int(exp),
                        'update_time': update_time.isoformat(),
                        'prev_update_time': prev_update_time.isoformat(),
                        'world': world,
                        'guild': guild
                    })
            
            del df
            
            for name, updates in exps_updates.items():
                mask = exps['name'] == name
                for col, val in updates.items():
                    exps.loc[mask, col] = val
            
            for (name, time), deltaexp in deltas_updates.items():
                mask = (deltas['name'] == name) & (deltas['update time'] == time)
                deltas.loc[mask, 'deltaexp'] = deltaexp
            
            if new_exps:
                exps = pd.concat([exps, pd.DataFrame(new_exps)], ignore_index=True)
            if new_deltas:
                deltas = pd.concat([deltas, pd.DataFrame(new_deltas)], ignore_index=True)
            
            self._write_exps(exps)
            self._write_deltas(deltas)
            
            del exps_dict, deltas_set, new_deltas, new_exps, exps_updates, deltas_updates, exps, deltas
            clean_memory()


def log_console(message: str, level: str = "INFO"):
    timestamp = (datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    print(log_entry)
    try:
        console_queue.put_nowait(log_entry)
    except queue.Full:
        pass


def clean_memory():
    """Check memory usage and trigger garbage collection if exceeds MAX_MEMORY_MB"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        current_mb = mem_info.rss / (1024 * 1024)
        
        if current_mb > MAX_MEMORY_MB:
            log_console(f"Memory usage ({current_mb:.2f}MB) exceeds limit ({MAX_MEMORY_MB}MB), triggering garbage collection", "WARNING")
            gc.collect()
            
            mem_info_after = process.memory_info()
            after_mb = mem_info_after.rss / (1024 * 1024)
            freed_mb = current_mb - after_mb
            log_console(f"Garbage collection completed. Freed {freed_mb:.2f}MB. Current: {after_mb:.2f}MB", "INFO")
            return True
        return False
    except Exception as e:
        log_console(f"Error in clean_memory: {str(e)}", "ERROR")
        return False


def extract_tables(soup):
    """Extract all tables from BeautifulSoup object"""
    dataframes = []
    table_elements = soup.find_all('table')

    for i, table in enumerate(table_elements):
        headers = []
        rows = []

        header_elements = table.find_all('th')
        if header_elements:
            headers = [header.get_text(strip=True) for header in header_elements]
        else:
            first_row = table.find('tr')
            if first_row:
                first_row_cells = first_row.find_all(['td', 'th'])
                headers = [cell.get_text(strip=True) for cell in first_row_cells]

        row_elements = table.find_all('tr')
        for row in row_elements:
            cells = row.find_all(['td', 'th'])
            if cells:
                row_data = [cell.get_text(strip=True) for cell in cells]
                if row_data != headers:
                    rows.append(row_data)

        if rows:
            if headers and len(headers) == len(rows[0]):
                df = pd.DataFrame(rows, columns=headers)
            else:
                df = pd.DataFrame(rows)
            df.attrs['table_index'] = i
            dataframes.append(df)

    del rows, headers
    clean_memory()
    return dataframes


def parse_datetime(date_str):
    """Parse datetime from Brazilian format"""
    import re
    if "Hoje" in date_str:
        time_part = re.search(r'(\d{2}:\d{2})', date_str)
        if time_part:
            time_str = time_part.group(1)
            now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
            date_time = datetime.strptime(f"{now.date()} {time_str}", "%Y-%m-%d %H:%M")
            return pd.to_datetime(date_time)
        else:
            yesterday = (datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)) - timedelta(days=1)
            return pd.to_datetime(datetime.strptime(f"{yesterday.date()} 00:00", "%Y-%m-%d %H:%M"))
    return None


def scrape_player_data(player_name):
    """Complete pipeline to scrape player data from rubinothings.com.br"""
    result = {
        'name': player_name,
        'tables': [],
        'response_status': None,
        'success': False
    }
    
    url = "https://rubinothings.com.br/player"
    params = {"name": player_name}
    
    if not FORCE_PROXY:
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed for '{player_name}' with status {response.status_code}, trying proxies...", "WARNING")
                url_with_params = f"{url}?name={player_name.replace(' ', '+')}"
                response = get_multiple(url_with_params, pp)
                log_console(f"Proxy fetch response for '{player_name}': {response}", "INFO")
        except Exception as e:
            log_console(f"Direct fetch failed for '{player_name}': {str(e)}, trying proxies...", "WARNING")
            url_with_params = f"{url}?name={player_name.replace(' ', '+')}"
            response = get_multiple(url_with_params, pp)
            log_console(f"Proxy fetch response for '{player_name}': {response}", "INFO")
    else:
        url_with_params = f"{url}?name={player_name.replace(' ', '+')}"
        response = get_multiple(url_with_params, pp)
        log_console(f"Proxy fetch response for '{player_name}': {response}", "INFO")

    if not response:
        log_console(f"All requests failed for '{player_name}'", "ERROR")
        return result
    
    try:
        result['response_status'] = response.status_code if hasattr(response, 'status_code') else 200
        
        if result['response_status'] == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = extract_tables(soup)
            
            tables_dict = []
            for df in tables:
                tables_dict.append({
                    'columns': df.columns.tolist(),
                    'data': df.values.tolist()
                })
            
            result['tables'] = tables_dict
            result['success'] = True
            log_console(f"Successfully scraped data for '{player_name}' - Found {len(tables)} tables", "INFO")
            
            del soup, tables, tables_dict
        else:
            log_console(f"Request failed for '{player_name}' with status code: {result['response_status']}", "ERROR")
            
    except Exception as e:
        log_console(f"Error parsing player data for '{player_name}': {str(e)}", "ERROR")
    
    del response
    gc.collect()
    return result


def get_ranking(world=None, guildname=None):
    """Get ranking from website"""
    if world is None:
        world = DEFAULT_WORLD
    if guildname is None:
        guildname = DEFAULT_GUILD
    url = f"https://rubinothings.com.br/guild.php?guild={guildname.replace(' ', '+')}&world={world}"

    if not FORCE_PROXY:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
                response = get_multiple(url, pp)
        except Exception as e:
            log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
            response = get_multiple(url, pp)
    else:
        response = get_multiple(url, pp)
    
    if not response:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    return extract_tables(soup)


def get_last_status_updates(world=None):
    """Get status updates to determine correct scraping timestamp"""
    if world is None:
        world = DEFAULT_WORLD
    url = "https://rubinothings.com.br/status"
    
    if not FORCE_PROXY:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
                response = get_multiple(url, pp)
        except Exception as e:
            log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
            response = get_multiple(url, pp)
    else:
        response = get_multiple(url, pp)
    
    if not response:
        return None
    soup = bs4.BeautifulSoup(response.text, 'html.parser')

    r = extract_tables(soup)
    split_tables = []

    for df in r:
        mask = df.apply(lambda row: all((str(x).strip() == '' or pd.isna(x)) for x in row), axis=1)
        split_indices = mask[mask].index.tolist()
        prev = 0
        for idx in split_indices:
            part = df.iloc[prev:idx]
            if not part.empty:
                split_tables.append(part.reset_index(drop=True))
            prev = idx + 1
        part = df.iloc[prev:]
        if not part.empty:
            split_tables.append(part.reset_index(drop=True))

    tables_dict = {}
    for table in split_tables:
        if not table.empty:
            table_name = str(table.iloc[0, 0]).strip()
            if "Status" in table_name:
                table_name = table_name.split("Status")[0].strip()
            new_table = table.iloc[1:].reset_index(drop=True)
            tables_dict[table_name] = new_table

    for key in tables_dict:
        df = tables_dict[key]
        if not df.empty:
            df.iloc[:, 0] = (
                df.iloc[:, 0]
                .str.replace("Rotina de coleta", "")
                .str.replace(r"[^\w\s,.:-]", "", regex=True)
            )
            tables_dict[key] = df
            if df.shape[1] >= 4:
                df.columns = ["rotina", "last update", "time_outdated", "status"]
                tables_dict[key] = df

    for key in tables_dict:
        df = tables_dict[key]
        if 'last update' in df.columns:
            df['last update'] = df['last update'].apply(parse_datetime)
            tables_dict[key] = df

    del soup, r, split_tables, response
    gc.collect()
    return tables_dict


def return_last_update(world=None, save_all_data=True, database=None):
    """Get the last update time and optionally save all worlds data to JSON"""
    if world is None:
        world = DEFAULT_WORLD
    if save_all_data and database:
        all_status = get_last_status_updates(world)
        
        if all_status:
            json_data = {
                "fetch_time": datetime.now().isoformat(),
                "worlds": {}
            }
            
            for world_name, df in all_status.items():
                if not df.empty and 'last update' in df.columns:
                    world_data = df.to_dict('records')
                    for record in world_data:
                        if 'last update' in record and pd.notna(record['last update']):
                            dt = pd.to_datetime(record['last update']) - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                            record['last update'] = dt.isoformat()
                        else:
                            record['last update'] = None
                    json_data["worlds"][world_name] = world_data
            
            database.save_status_data(json_data)
            log_console(f"Status data saved for {len(json_data['worlds'])} worlds", "INFO")
    
    status_data = get_last_status_updates(world)
    if status_data and world in status_data:
        df = status_data[world]
        update_time = str(df[df['rotina'] == 'Daily Raw Ranking']['last update'].values[0])
        return pd.to_datetime(update_time)
    return None


def parse_to_db_formatted(df, last_update, world=None, guild=None):
    """Parse ranking data to database format"""
    if world is None:
        world = DEFAULT_WORLD
    if guild is None:
        guild = DEFAULT_GUILD
    new_df = pd.DataFrame()
    new_df['name'] = df['Jogador']
    new_df['exp'] = df['RAW no período'].str.replace(',', '').str.replace('.', '').astype(int)
    new_df['last update'] = last_update
    new_df['world'] = world
    new_df['guild'] = guild
    return new_df


def parse_online_time_to_minutes(time_str):
    """Parse online time string to total minutes"""
    if not time_str or time_str == "0:00":
        return 0
    
    total_minutes = 0
    time_str = time_str.strip()
    
    if 'h' in time_str:
        parts = time_str.split('h')
        hours = int(parts[0].strip())
        total_minutes += hours * 60
        remaining = parts[1].strip() if len(parts) > 1 else ""
    else:
        remaining = time_str
    
    if 'm' in remaining:
        minutes_str = remaining.split('m')[0].strip()
        if minutes_str:
            minutes = int(minutes_str)
            total_minutes += minutes
    
    return total_minutes


def scrape_single_vip(database, name, world):
    """Scrape a single VIP player and update their data"""
    try:
        result = scrape_player_data(name)
        if result['success'] and result['tables']:
            today_exp = 0
            today_online = 0
            
            for table in result['tables']:
                columns = table['columns']
                data = table['data']
                
                if 'Raw XP no dia' in columns and data:
                    idx = columns.index('Raw XP no dia')
                    raw_value = data[0][idx] if len(data[0]) > idx else "0"
                    today_exp = int(raw_value.replace(',', '').replace('.', ''))
                
                if 'Online time' in columns and data:
                    idx = columns.index('Online time')
                    online_time_str = data[0][idx] if len(data[0]) > idx else "0:00"
                    today_online = parse_online_time_to_minutes(online_time_str)
            
            vipsdata = database.get_vipsdata()
            existing_vip = vipsdata[(vipsdata['name'] == name) & (vipsdata['world'] == world)]
            
            if not existing_vip.empty:
                old_exp = existing_vip['today_exp'].values[0]
                old_online = existing_vip['today_online'].values[0]
                delta_exp = today_exp - old_exp
                delta_online = today_online - old_online
                
                now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                today_date = now.strftime("%Y-%m-%d")
                
                if delta_exp != 0:
                    database.add_vip_delta(name, world, today_date, delta_exp, delta_online, now)
                    database.update_vipdata(name, world, today_exp, today_online)
                    log_console(f"VIP {name} ({world}): {today_exp} exp, {today_online} min online", "INFO")
            else:
                now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                today_date = now.strftime("%Y-%m-%d")
                database.add_vip_delta(name, world, today_date, 0, 0, now)
                database.update_vipdata(name, world, today_exp, today_online)
                log_console(f"VIP delta: {name} ({world}) +0 exp, +0 online (initial baseline)", "INFO")
            
            return True
        else:
            log_console(f"Failed to scrape VIP {name} ({world})", "WARNING")
            return False
    except Exception as e:
        log_console(f"Error scraping VIP {name} ({world}): {str(e)}", "ERROR")
        return False


def get_delta_between(datetime1, datetime2, database):
    """Filter deltas between two datetimes"""
    table = database.get_deltas()
    datetime1 = pd.to_datetime(datetime1)
    datetime2 = pd.to_datetime(datetime2)
    mask = (table['update time'] >= datetime1) & (table['update time'] <= datetime2)
    return table[mask]


def preprocess_vis_data(all_update_times, all_player_data, names_list):
    """Preprocess visualization data to compress consecutive zero periods"""
    num_times = len(all_update_times)
    
    sorted_indices = sorted(range(num_times), key=lambda i: pd.to_datetime(all_update_times[i]))
    
    all_update_times = [all_update_times[i] for i in sorted_indices]
    all_player_data = {name: [all_player_data[name][i] for i in sorted_indices] for name in names_list}
    
    all_zero_positions = []
    for i in range(num_times):
        if all(all_player_data[name][i] == 0 for name in names_list):
            all_zero_positions.append(i)
    
    zero_groups = []
    if all_zero_positions:
        start = all_zero_positions[0]
        for i in range(1, len(all_zero_positions)):
            if all_zero_positions[i] != all_zero_positions[i-1] + 1:
                if all_zero_positions[i-1] - start >= 1:
                    zero_groups.append((start, all_zero_positions[i-1]))
                start = all_zero_positions[i]
        if all_zero_positions[-1] - start >= 1:
            zero_groups.append((start, all_zero_positions[-1]))
    
    compressed_times = []
    compressed_data = {name: [] for name in names_list}
    label_metadata = []
    
    prev_time = None
    i = 0
    while i < num_times:
        in_zero_group = False
        for start, end in zero_groups:
            if i == start:
                if start > 0:
                    start_time = pd.to_datetime(all_update_times[start - 1])
                else:
                    start_time = pd.to_datetime(all_update_times[start])
                end_time = pd.to_datetime(all_update_times[end])
                
                start_date = start_time.date()
                end_date = end_time.date()
                
                if start_date == end_date:
                    short_label = f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}"
                    full_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%H:%M')}"
                else:
                    short_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%d/%m/%Y %H:%M')}"
                    full_label = short_label
                
                label_metadata.append((short_label, full_label, len(compressed_times)))
                compressed_times.append(short_label)
                prev_time = end_time
                
                for name in names_list:
                    compressed_data[name].append(0)
                
                i = end + 1
                in_zero_group = True
                break
        
        if not in_zero_group:
            time_obj = pd.to_datetime(all_update_times[i])
            current_date = time_obj.date()
            
            if prev_time is None:
                short_label = time_obj.strftime('%H:%M')
                full_label = time_obj.strftime('%d/%m/%Y %H:%M')
            else:
                start_date = prev_time.date()
                
                if start_date == current_date:
                    short_label = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%H:%M')}"
                    full_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%H:%M')}"
                else:
                    short_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
                    full_label = short_label
            
            label_metadata.append((short_label, full_label, len(compressed_times)))
            compressed_times.append(short_label)
            prev_time = time_obj
            
            for name in names_list:
                compressed_data[name].append(all_player_data[name][i])
            i += 1
    
    from collections import Counter
    label_counts = Counter(compressed_times)
    for short_label, full_label, idx in label_metadata:
        if label_counts[short_label] > 1:
            compressed_times[idx] = full_label

    del all_zero_positions, zero_groups, label_metadata, label_counts
    gc.collect()
    return compressed_times, compressed_data


def create_interactive_graph(names, database, datetime1=None, datetime2=None):
    """Create interactive Plotly graph for player EXP gains"""
    theme_colors = [
        '#C21500', '#FFC500', '#FF6B35', '#FFE156',
        '#B81400', '#E6A900', '#FF8F66', '#FFD966',
    ]
    
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    names_list = [names] if isinstance(names, str) else names

    all_update_times = sorted(table['update time'].unique())

    all_player_data = {}
    for name in names_list:
        player_data = table[table['name'] == name]
        if not player_data.empty:
            player_deltas = dict(zip(player_data['update time'], player_data['deltaexp']))
            standardized_exps = [player_deltas.get(update_time, 0) for update_time in all_update_times]
            all_player_data[name] = standardized_exps

    if not all_player_data:
        fig = go.Figure()
        fig.update_layout(title='No data available')
        return fig.to_json()

    compressed_times, compressed_data = preprocess_vis_data(all_update_times, all_player_data, names_list)

    fig = go.Figure()

    for idx, name in enumerate(names_list):
        base_color = theme_colors[idx % len(theme_colors)]
        
        hex_color = base_color.lstrip('#')
        r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
        
        fig.add_trace(go.Bar(
            x=compressed_times,
            y=compressed_data[name],
            name=name,
            marker=dict(
                color=compressed_data[name],
                colorscale=[
                    [0, f'rgba({r},{g},{b},0.2)'],
                    [0.5, f'rgba({r},{g},{b},0.6)'],
                    [1, f'rgba({r},{g},{b},1)']
                ],
                showscale=False,
                line=dict(width=0)
            ),
            text=[str(int(exp)) if exp > 0 else '' for exp in compressed_data[name]],
            textposition='outside',
            textangle=0,
            hovertemplate='<b>%{x}</b><br>EXP: %{y:,.0f}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=compressed_times,
            y=compressed_data[name],
            name=f'{name} (trend)',
            mode='lines',
            line=dict(color='#3498db', width=3, shape='spline'),
            showlegend=False,
            hoverinfo='skip'
        ))
        
    fig.update_layout(
        title=f'EXP Gain Over Time',
        xaxis_title='Update Time',
        yaxis_title='Delta EXP',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        showlegend=True,
        barmode='group',
        bargap=0,
        bargroupgap=0,
        xaxis=dict(
            type='category',
            categoryorder='array',
            categoryarray=compressed_times,
            tickangle=-45,
            tickmode='auto',
            nticks=20
        ),
        colorway=theme_colors
    )
    fig.update_xaxes(
        categoryorder='array',
        categoryarray=compressed_times
    )

    result = fig.to_json()
    del table, all_update_times, all_player_data, compressed_times, compressed_data, fig
    gc.collect()
    return result


def get_player_stats(names, database, datetime1=None, datetime2=None):
    """Get statistics table for players"""
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    if names:
        names_list = [names] if isinstance(names, str) else names
        table = table[table['name'].isin(names_list)]

    stats = table.groupby('name').agg({
        'deltaexp': ['sum', 'mean', 'count', 'max', 'min']
    }).round(2)

    stats.columns = ['Total EXP', 'Average EXP', 'Updates', 'Max EXP', 'Min EXP']
    stats = stats.sort_values('Total EXP', ascending=False)
    stats = stats.reset_index()

    result = stats.to_dict('records')
    del table, stats
    gc.collect()
    return result



# Initialize database
db = SQLAlchemyDatabase()

# Exception handlers
@app.exception_handler(400)
async def bad_request_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=400,
        content={
            'error': 'Bad Request',
            'message': str(exc.detail),
            'status': 400
        }
    )


@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=404,
        content={
            'error': 'Not Found',
            'message': str(exc.detail),
            'status': 404
        }
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: Exception):
    log_console(f"Internal server error: {str(exc)}", "ERROR")
    return JSONResponse(
        status_code=500,
        content={
            'error': 'Internal Server Error',
            'message': str(exc),
            'status': 500
        }
    )


# API Endpoints

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main page"""
    return templates.TemplateResponse("index_fast.html", {"request": request})


@app.get("/api/players")
def get_players(world: Optional[str] = Query(None), guild: Optional[str] = Query(None)):
    """Get list of all players"""
    deltas = db.get_deltas()

    if world:
        deltas = deltas[deltas['world'] == world]
    if guild:
        deltas = deltas[deltas['guild'] == guild]

    players = sorted(deltas['name'].unique().tolist())
    return players


@app.get("/api/date-range")
def get_date_range(world: Optional[str] = Query(None), guild: Optional[str] = Query(None)):
    """Get available date range"""
    deltas = db.get_deltas()

    if world:
        deltas = deltas[deltas['world'] == world]
    if guild:
        deltas = deltas[deltas['guild'] == guild]

    if not deltas.empty:
        min_date = deltas['update time'].min()
        max_date = deltas['update time'].max()
        return {
            'min': min_date.isoformat(),
            'max': max_date.isoformat()
        }
    return {'min': None, 'max': None}


@app.post("/api/graph")
def get_graph(request: GraphRequest):
    """Generate interactive graph with stats and comparison data"""
    names = request.names
    datetime1 = request.datetime1
    datetime2 = request.datetime2

    if not names:
        raise HTTPException(status_code=400, detail='No players selected')

    try:
        graph_json = create_interactive_graph(names, db, datetime1, datetime2)
        stats = get_player_stats(names, db, datetime1, datetime2)

        deltas_table = db.get_deltas()
        exps_table = db.get_exps()

        if datetime1 and datetime2:
            deltas_table = get_delta_between(datetime1, datetime2, db)

        all_rankings = deltas_table.groupby('name')['deltaexp'].sum().sort_values(ascending=False)

        comparison = []
        for name in names:
            if name in all_rankings.index:
                rank = list(all_rankings.index).index(name) + 1
                total_exp = int(all_rankings[name])
                percentile = (1 - (rank / len(all_rankings))) * 100

                current_exp = 0
                if name in exps_table['name'].values:
                    current_exp = int(exps_table[exps_table['name'] == name]['exp'].values[0])

                comparison.append({
                    'name': name,
                    'rank': rank,
                    'total_players': len(all_rankings),
                    'percentile': round(percentile, 1),
                    'total_exp_period': total_exp,
                    'current_total_exp': current_exp
                })

        return {
            'graph': graph_json,
            'stats': stats,
            'comparison': comparison
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/stats")
def get_stats(request: StatsRequest):
    """Get player statistics"""
    names = request.names
    datetime1 = request.datetime1
    datetime2 = request.datetime2

    try:
        stats = get_player_stats(names, db, datetime1, datetime2)
        return {'stats': stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-players")
def get_top_players(limit: int = Query(10), datetime1: Optional[str] = Query(None),
                          datetime2: Optional[str] = Query(None)):
    """Get top players by total EXP"""
    table = db.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, db)

    top = table.groupby('name')['deltaexp'].sum().sort_values(ascending=False).head(limit)

    result = [{'name': name, 'total_exp': int(exp)} for name, exp in top.items()]
    return result


@app.get("/api/recent-updates")
def get_recent_updates(limit: int = Query(20)):
    """Get recent EXP updates"""
    deltas = db.get_deltas()
    recent = deltas.sort_values('update time', ascending=False).head(limit)
    result = recent.to_dict('records')

    for item in result:
        item['update time'] = item['update time'].isoformat()

    return result


@app.post("/api/rankings-table")
def get_rankings_table(request: RankingsRequest):
    """Get grouped rankings table with filters and sorting"""
    datetime1 = request.datetime1
    datetime2 = request.datetime2

    try:
        table = db.get_deltas()

        if datetime1 and datetime2:
            table = get_delta_between(datetime1, datetime2, db)

        grouped = table.groupby('name').agg({
            'deltaexp': list,
            'update time': list
        })

        grouped['sum'] = grouped['deltaexp'].apply(sum)
        grouped['number of updates'] = grouped['deltaexp'].str.len()
        grouped['avg'] = grouped['sum'] / grouped['number of updates']
        grouped['max'] = grouped['deltaexp'].apply(max)
        grouped['min'] = grouped['deltaexp'].apply(min)

        result = [
            {
                'name': name,
                'total_exp': int(row['sum']),
                'updates': int(row['number of updates']),
                'avg_exp': round(row['avg'], 2),
                'max_exp': int(row['max']),
                'min_exp': int(row['min'])
            }
            for name, row in grouped.iterrows()
        ]

        del table, grouped
        gc.collect()
        return {'rankings': result}
    except Exception as e:
        log_console(f"Error in rankings table: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/console-stream")
def console_stream():
    """Server-Sent Events stream for console logs"""

    def generate():
        yield f"data: [CONNECTED] Console stream started\n\n"

        while True:
            try:
                log = console_queue.get(timeout=1)
                yield f"data: {log}\n\n"
            except queue.Empty:
                yield f": keepalive\n\n"
                time.sleep(1)

    return StreamingResponse(generate(), media_type='text/event-stream')


@app.get("/api/scraper-status")
async def get_scraper_status():
    """Get scraper status"""
    global last_status_check
    last_status_check = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)

    deltas = db.get_deltas()
    with scraper_lock:
        state = scraper_state

    return {
        'running': scraper_running,
        'state': state,
        'last_update': deltas['update time'].max().isoformat() if not deltas.empty else None,
        'last_check': last_status_check.isoformat()
    }


@app.get("/healthz")
def healthz():
    """Health check endpoint"""
    try:
        health_status = {
            'status': 'healthy',
            'checks': {}
        }

        thread_alive = scraper_thread is not None and scraper_thread.is_alive()
        health_status['checks']['scraper_thread_alive'] = thread_alive

        try:
            deltas = db.get_deltas()
            exps = db.get_exps()
            db_accessible = True
            health_status['checks']['database_accessible'] = True
            health_status['checks']['total_players'] = len(exps) if not exps.empty else 0
            health_status['checks']['total_deltas'] = len(deltas) if not deltas.empty else 0
        except Exception as e:
            db_accessible = False
            health_status['checks']['database_accessible'] = False
            health_status['checks']['database_error'] = str(e)

        if db_accessible and not deltas.empty:
            last_update = deltas['update time'].max()
            current_time = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
            time_since_update = current_time - last_update
            minutes_since_update = time_since_update.total_seconds() / 60
            health_status['checks']['last_update'] = last_update.isoformat()
            health_status['checks']['minutes_since_last_update'] = round(minutes_since_update, 2)
            recent_update = minutes_since_update < 120
            health_status['checks']['recent_update'] = recent_update
        else:
            health_status['checks']['recent_update'] = False
            health_status['checks']['last_update'] = None

        with scraper_lock:
            current_state = scraper_state
        health_status['checks']['scraper_state'] = current_state
        health_status['checks']['scraper_running_flag'] = scraper_running

        if not thread_alive:
            health_status['status'] = 'unhealthy'
            health_status['reason'] = 'Scraper thread is not running'
        elif not db_accessible:
            health_status['status'] = 'unhealthy'
            health_status['reason'] = 'Database is not accessible'
        elif not health_status['checks'].get('recent_update', False):
            health_status['status'] = 'degraded'
            health_status['reason'] = 'No recent updates (>2 hours)'

        status_code = 200 if health_status['status'] in ['healthy', 'degraded'] else 503
        return JSONResponse(content=health_status, status_code=status_code)

    except Exception as e:
        return JSONResponse(
            content={
                'status': 'unhealthy',
                'error': str(e),
                'reason': 'Health check failed with exception'
            },
            status_code=503
        )


@app.get("/memusage")
async def memusage():
    """Memory usage endpoint"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        system_mem = psutil.virtual_memory()

        return {
            'process': {
                'rss_bytes': mem_info.rss,
                'rss_mb': round(mem_info.rss / (1024 * 1024), 2),
                'vms_bytes': mem_info.vms,
                'vms_mb': round(mem_info.vms / (1024 * 1024), 2),
                'percent': round(process.memory_percent(), 2)
            },
            'system': {
                'total_bytes': system_mem.total,
                'total_mb': round(system_mem.total / (1024 * 1024), 2),
                'total_gb': round(system_mem.total / (1024 * 1024 * 1024), 2),
                'available_bytes': system_mem.available,
                'available_mb': round(system_mem.available / (1024 * 1024), 2),
                'available_gb': round(system_mem.available / (1024 * 1024 * 1024), 2),
                'used_bytes': system_mem.used,
                'used_mb': round(system_mem.used / (1024 * 1024), 2),
                'used_gb': round(system_mem.used / (1024 * 1024 * 1024), 2),
                'percent': system_mem.percent
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Failed to retrieve memory usage: {str(e)}')


@app.get("/api/download/deltas")
async def download_deltas():
    """Download deltas.csv file"""
    try:
        return FileResponse(db.deltas_file, filename='deltas.csv', media_type='text/csv')
    except Exception as e:
        log_console(f"Error downloading deltas.csv: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/download/exps")
async def download_exps():
    """Download exps.csv file"""
    try:
        return FileResponse(db.exps_file, filename='exps.csv', media_type='text/csv')
    except Exception as e:
        log_console(f"Error downloading exps.csv: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/upload/deltas")
def upload_deltas(password: str = Form(...), file: UploadFile = File(...)):
    """Upload deltas.csv file"""
    try:
        if password != UPLOAD_PASSWORD:
            raise HTTPException(status_code=401, detail='Invalid password')

        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail='Only CSV files are allowed')

        contents = file.read()
        df = pd.read_csv(StringIO(contents.decode('utf-8')),
                         dtype={'name': str, 'deltaexp': 'int64', 'world': str, 'guild': str},
                         parse_dates=['update time'])

        required_columns = ['name', 'deltaexp', 'update time']
        if not all(col in df.columns for col in required_columns):
            raise HTTPException(status_code=400, detail=f'CSV must have columns: {required_columns}')

        import shutil
        if os.path.exists(db.deltas_file):
            backup_file = db.deltas_file.replace('.csv', '_backup.csv')
            shutil.copy(db.deltas_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")

        records_count = len(df)
        df.to_csv(db.deltas_file, index=False)
        log_console(f"Uploaded deltas.csv with {records_count} records", "SUCCESS")

        del df
        gc.collect()
        return {'success': True, 'records': records_count}
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error uploading deltas.csv: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/upload/exps")
def upload_exps(password: str = Form(...), file: UploadFile = File(...)):
    """Upload exps.csv file"""
    try:
        if password != UPLOAD_PASSWORD:
            raise HTTPException(status_code=401, detail='Invalid password')

        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail='Only CSV files are allowed')

        contents = file.read()
        df = pd.read_csv(StringIO(contents.decode('utf-8')),
                         dtype={'name': str, 'exp': 'int64', 'world': str, 'guild': str},
                         parse_dates=['last update'])

        required_columns = ['name', 'exp', 'last update']
        if not all(col in df.columns for col in required_columns):
            raise HTTPException(status_code=400, detail=f'CSV must have columns: {required_columns}')

        import shutil
        if os.path.exists(db.exps_file):
            backup_file = db.exps_file.replace('.csv', '_backup.csv')
            shutil.copy(db.exps_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")

        records_count = len(df)
        df.to_csv(db.exps_file, index=False)
        log_console(f"Uploaded exps.csv with {records_count} records", "SUCCESS")

        del df
        gc.collect()
        return {'success': True, 'records': records_count}
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error uploading exps.csv: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/player-graph/{player_name}")
def get_player_graph(player_name: str = PathParam(...), datetime1: Optional[str] = Query(None),
                           datetime2: Optional[str] = Query(None)):
    """Get individual player graph data"""
    try:
        graph_json = create_interactive_graph(player_name, db, datetime1, datetime2)
        return {'graph': graph_json, 'player': player_name}
    except Exception as e:
        log_console(f"Error generating player graph for {player_name}: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/player-details/{player_name}")
def get_player_details(player_name: str = PathParam(...)):
    """Get detailed player data"""
    try:
        player_data = scrape_player_data(player_name)

        if player_data.get('success') and player_data.get('tables'):
            vips = db.get_vips()
            matching_vip = next((v for v in vips if v['name'] == player_name), None)

            if matching_vip:
                world = matching_vip['world']
                today_exp = 0
                today_online = 0

                for table in player_data['tables']:
                    columns = table['columns']
                    data = table['data']

                    if 'Raw XP no dia' in columns and data:
                        idx = columns.index('Raw XP no dia')
                        raw_value = data[0][idx] if len(data[0]) > idx else "0"
                        today_exp = int(raw_value.replace(',', '').replace('.', ''))

                    if 'Online time' in columns and data:
                        idx = columns.index('Online time')
                        online_time_str = data[0][idx] if len(data[0]) > idx else "0:00"
                        today_online = parse_online_time_to_minutes(online_time_str)

                vipsdata = db.get_vipsdata()
                existing_vip = vipsdata[(vipsdata['name'] == player_name) & (vipsdata['world'] == world)]

                if not existing_vip.empty:
                    old_exp = existing_vip['today_exp'].values[0]
                    old_online = existing_vip['today_online'].values[0]
                    delta_exp = today_exp - old_exp
                    delta_online = today_online - old_online

                    if delta_exp != 0:
                        now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                        today_date = now.strftime("%Y-%m-%d")
                        db.add_vip_delta(player_name, world, today_date, delta_exp, delta_online, now)
                        log_console(f"VIP delta processed: {player_name} +{delta_exp} exp, +{delta_online} online",
                                    "INFO")
                        db.update_vipdata(player_name, world, today_exp, today_online)

        return player_data
    except Exception as e:
        log_console(f"Error getting player details for {player_name}: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/delta")
def get_deltas(limit: int = Query(100), world: Optional[str] = Query(None),
                     guild: Optional[str] = Query(None)):
    """Get recent delta updates"""
    try:
        all_deltas = db.get_deltas()

        if all_deltas.empty:
            return {'deltas': []}

        if world:
            all_deltas = all_deltas[all_deltas['world'] == world]
        if guild:
            all_deltas = all_deltas[all_deltas['guild'] == guild]

        if all_deltas.empty:
            return {'deltas': []}

        recent_deltas = all_deltas.sort_values(['update time', 'name'], ascending=[False, True]).head(limit)

        distinct_times_list = sorted(all_deltas['update time'].unique())

        prev_time_map = {}
        for i, current_time in enumerate(distinct_times_list):
            if i > 0:
                prev_time_map[current_time] = distinct_times_list[i - 1]
            else:
                prev_time_map[current_time] = current_time

        deltas = []
        for row in recent_deltas.itertuples(index=False):
            current_time = getattr(row, 'update_time', row[2])
            prev_update_time = prev_time_map.get(current_time, current_time)

            deltas.append({
                'name': row.name,
                'deltaexp': int(row.deltaexp),
                'update_time': current_time.isoformat(),
                'prev_update_time': prev_update_time.isoformat(),
                'world': getattr(row, 'world', DEFAULT_WORLD),
                'guild': getattr(row, 'guild', DEFAULT_GUILD)
            })

        del all_deltas, recent_deltas, distinct_times_list, prev_time_map
        gc.collect()
        return {'deltas': deltas}
    except Exception as e:
        log_console(f"Error getting deltas: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status-data")
async def get_status_data():
    """Get stored status data"""
    try:
        status_data = db.get_status_data()
        if status_data:
            return status_data
        else:
            raise HTTPException(status_code=404, detail='No status data available yet')
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error getting status data: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scraping-config")
async def get_scraping_config():
    """Get scraping configuration"""
    try:
        config = db.get_scraping_config()
        return config
    except Exception as e:
        log_console(f"Error getting scraping config: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraping-config")
async def update_scraping_config(request: ScrapingConfigUpdate):
    """Update scraping configuration"""
    try:
        if request.password != UPLOAD_PASSWORD:
            raise HTTPException(status_code=401, detail='Invalid password')

        config = request.config
        if not isinstance(config, list):
            raise HTTPException(status_code=400, detail='Config must be an array')

        for item in config:
            if 'world' not in item or 'guilds' not in item:
                raise HTTPException(status_code=400, detail='Each config item must have "world" and "guilds" fields')
            if not isinstance(item['guilds'], list):
                raise HTTPException(status_code=400, detail='"guilds" must be an array')

        db.save_scraping_config(config)
        log_console(f"Scraping configuration updated via API", "SUCCESS")

        return {'success': True, 'config': config}
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error updating scraping config: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/manual-update")
def manual_update():
    """Manually trigger a ranking update"""
    global scraper_state

    with scraper_lock:
        current_state = scraper_state

    if current_state in ['checking', 'scraping']:
        return JSONResponse(
            content={
                'success': False,
                'message': 'Scraper is already running',
                'state': current_state
            },
            status_code=409
        )

    try:
        log_console("Manual update triggered", "INFO")

        with scraper_lock:
            scraper_state = "checking"

        scraping_config = db.get_scraping_config()
        first_world = scraping_config[0]['world'] if scraping_config else DEFAULT_WORLD

        current_update = return_last_update(first_world, save_all_data=True, database=db)

        if current_update is None:
            raise Exception("Failed to get update time")

        with scraper_lock:
            scraper_state = "scraping"

        all_players = []
        for config_item in scraping_config:
            world = config_item['world']
            guilds = config_item['guilds']

            for guild in guilds:
                try:
                    log_console(f"Manual scraping {world} - {guild}", "INFO")
                    r = get_ranking(world=world, guildname=guild)

                    if r is None or len(r) < 2:
                        log_console(f"No data for {world} - {guild}", "WARNING")
                    else:
                        rankings = r[1]
                        rankparsed = parse_to_db_formatted(rankings, current_update, world=world, guild=guild)
                        all_players.append(rankparsed)
                except Exception as e:
                    log_console(f"Error scraping {world} - {guild}: {str(e)}", "ERROR")

        if all_players:
            combined_df = pd.concat(all_players, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['name'], keep='first')
            db.update(combined_df, current_update)
            db.save()
            log_console(f"Manual update: {len(combined_df)} total players", "SUCCESS")
        else:
            raise Exception("No player data collected from any world/guild")

        with scraper_lock:
            scraper_state = "idle"

        log_console(f"Manual update completed at {current_update}", "SUCCESS")
        return {
            'success': True,
            'message': 'Update completed successfully',
            'update_time': current_update.isoformat()
        }
    except Exception as e:
        error_msg = str(e)
        log_console(f"Manual update failed: {error_msg}", "ERROR")

        with scraper_lock:
            scraper_state = "idle"

        raise HTTPException(status_code=500, detail=f'Update failed: {error_msg}')


@app.get("/vip", response_class=HTMLResponse)
async def vip_page(request: Request):
    """VIP tracking page"""
    return templates.TemplateResponse("vip_fast.html", {"request": request})


@app.get("/api/vip/list")
async def get_vip_list():
    """Get list of VIP players"""
    try:
        vips = db.get_vips()
        return {'vips': vips}
    except Exception as e:
        log_console(f"Error getting VIP list: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/vip/add")
def add_vip(request: VIPAdd):
    """Add a VIP player"""
    try:
        if not request.name or not request.world:
            raise HTTPException(status_code=400, detail='Name and world are required')

        success = db.add_vip(request.name, request.world)
        if success:
            log_console(f"Immediately scraping new VIP: {request.name} ({request.world})", "INFO")
            scrape_single_vip(db, request.name, request.world)
            return {'success': True}
        else:
            raise HTTPException(status_code=400, detail='VIP already exists')
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error adding VIP: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/vip/remove")
async def remove_vip(request: VIPRemove):
    """Remove a VIP player"""
    try:
        if not request.name or not request.world:
            raise HTTPException(status_code=400, detail='Name and world are required')

        success = db.remove_vip(request.name, request.world)
        if success:
            return {'success': True}
        else:
            raise HTTPException(status_code=404, detail='VIP not found')
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error removing VIP: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/vip/deltas")
async def get_vip_deltas(limit: int = Query(100), name: Optional[str] = Query(None),
                         world: Optional[str] = Query(None)):
    """Get VIP delta history"""
    try:
        deltavip = db.get_deltavip()

        if deltavip.empty:
            return {'deltas': []}

        original_deltavip = deltavip.copy()

        if name:
            deltavip = deltavip[deltavip['name'] == name]
            original_deltavip = original_deltavip[original_deltavip['name'] == name]
        if world:
            deltavip = deltavip[deltavip['world'] == world]
            original_deltavip = original_deltavip[original_deltavip['world'] == world]

        deltavip = deltavip[deltavip['delta_exp'] != 0]

        if deltavip.empty:
            return {'deltas': []}

        recent_deltas = deltavip.sort_values('update_time', ascending=False).head(limit)

        deltas = []
        for row in recent_deltas.itertuples(index=False):
            current_time = row[5]
            current_name = row.name
            current_world = row.world

            vip_history = original_deltavip[
                (original_deltavip['name'] == current_name) &
                (original_deltavip['world'] == current_world) &
                (original_deltavip['update_time'] < current_time)
                ].sort_values('update_time', ascending=False)

            if not vip_history.empty:
                last_entry = vip_history.iloc[0]
                if last_entry['delta_exp'] == 0:
                    prev_update_time = last_entry['update_time']
                else:
                    prev_update_time = last_entry['update_time']
            else:
                prev_update_time = current_time

            deltas.append({
                'name': current_name,
                'world': current_world,
                'delta_exp': int(row.delta_exp),
                'delta_online': int(row.delta_online),
                'update_time': current_time.isoformat(),
                'prev_update_time': prev_update_time.isoformat(),
                'date': row.date
            })

        del deltavip, original_deltavip, recent_deltas
        gc.collect()
        return {'deltas': deltas}
    except Exception as e:
        log_console(f"Error getting VIP deltas: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/vip/graph")
def get_vip_graph(request: VIPGraphRequest):
    """Generate VIP graph with exp and online time"""
    try:
        name = request.name
        world = request.world

        if not name or not world:
            raise HTTPException(status_code=400, detail='Name and world are required')

        deltavip = db.get_deltavip()
        vip_data = deltavip[(deltavip['name'] == name) & (deltavip['world'] == world)]

        if vip_data.empty:
            raise HTTPException(status_code=404, detail='No data available for this VIP')

        vip_data = vip_data.sort_values('update_time')

        all_update_times = vip_data['update_time'].tolist()
        exp_values = vip_data['delta_exp'].tolist()
        online_values = vip_data['delta_online'].tolist()

        all_zero_positions = []
        for i in range(len(exp_values)):
            if exp_values[i] == 0 and online_values[i] == 0:
                all_zero_positions.append(i)

        zero_groups = []
        if all_zero_positions:
            start = all_zero_positions[0]
            for i in range(1, len(all_zero_positions)):
                if all_zero_positions[i] != all_zero_positions[i - 1] + 1:
                    if all_zero_positions[i - 1] - start >= 1:
                        zero_groups.append((start, all_zero_positions[i - 1]))
                    start = all_zero_positions[i]
            if all_zero_positions[-1] - start >= 1:
                zero_groups.append((start, all_zero_positions[-1]))

        time_labels = []
        compressed_exp = []
        compressed_online = []
        compressed_online_display = []
        time_diffs = []
        label_metadata = []

        prev_time = None
        prev_timestamp = None
        i = 0
        while i < len(all_update_times):
            in_zero_group = False
            for start, end in zero_groups:
                if i == start:
                    if start > 0:
                        start_time = pd.to_datetime(all_update_times[start - 1])
                    else:
                        start_time = pd.to_datetime(all_update_times[start])
                    end_time = pd.to_datetime(all_update_times[end])

                    start_date = start_time.date()
                    end_date = end_time.date()

                    if start_date == end_date:
                        short_label = f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}"
                        full_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%H:%M')}"
                    else:
                        short_label = f"{start_time.strftime('%d/%m/%Y %H:%M')}-{end_time.strftime('%d/%m/%Y %H:%M')}"
                        full_label = short_label

                    label_metadata.append((short_label, full_label, len(time_labels)))
                    time_labels.append(short_label)
                    compressed_exp.append(0)
                    compressed_online.append(0)
                    compressed_online_display.append("0 / 0 min")
                    time_diffs.append(0)
                    prev_time = end_time
                    prev_timestamp = end_time

                    i = end + 1
                    in_zero_group = True
                    break

            if not in_zero_group:
                time_obj = pd.to_datetime(all_update_times[i])
                current_date = time_obj.date()

                if prev_timestamp is not None:
                    time_diff_minutes = int((time_obj - prev_timestamp).total_seconds() / 60)
                else:
                    time_diff_minutes = 0

                if prev_time is None:
                    short_label = time_obj.strftime('%H:%M')
                    full_label = time_obj.strftime('%d/%m/%Y %H:%M')
                else:
                    start_date = prev_time.date()
                    if start_date == current_date:
                        short_label = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%H:%M')}"
                        full_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%H:%M')}"
                    else:
                        short_label = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
                        full_label = short_label

                label_metadata.append((short_label, full_label, len(time_labels)))
                time_labels.append(short_label)
                compressed_exp.append(exp_values[i])

                online_val = online_values[i]
                if online_val == 0 and exp_values[i] > 0:
                    compressed_online.append(None)
                else:
                    compressed_online.append(online_val)

                display_label = f"{online_val} / {time_diff_minutes} min"
                compressed_online_display.append(display_label)
                time_diffs.append(time_diff_minutes)

                prev_time = time_obj
                prev_timestamp = time_obj
                i += 1

        from collections import Counter
        label_counts = Counter(time_labels)
        for short_label, full_label, idx in label_metadata:
            if label_counts[short_label] > 1:
                time_labels[idx] = full_label

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=time_labels,
            y=compressed_exp,
            name='EXP Gain',
            marker_color='#C21500',
            text=[str(int(exp)) if exp > 0 else '' for exp in compressed_exp],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>EXP: %{y:,.0f}<extra></extra>',
            yaxis='y'
        ))

        fig.add_trace(go.Scatter(
            x=time_labels,
            y=compressed_exp,
            name='EXP Trend',
            mode='lines',
            line=dict(color='#C21500', width=2, shape='spline'),
            showlegend=False,
            hoverinfo='skip',
            yaxis='y'
        ))

        fig.add_trace(go.Scatter(
            x=time_labels,
            y=compressed_online,
            name='Online Time (min)',
            mode='lines+markers',
            line=dict(color='#3498db', width=2, shape='spline'),
            marker=dict(size=8, symbol='circle'),
            text=compressed_online_display,
            hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>',
            connectgaps=True,
            yaxis='y2'
        ))

        fig.update_layout(
            title=f'🌟 {name} - VIP Stats ({world})',
            xaxis_title='Update Time',
            yaxis=dict(
                title=dict(text='Delta EXP', font=dict(color='#C21500')),
                tickfont=dict(color='#C21500')
            ),
            yaxis2=dict(
                title=dict(text='Online Time (minutes)', font=dict(color='#3498db')),
                tickfont=dict(color='#3498db'),
                overlaying='y',
                side='right'
            ),
            template='plotly_white',
            height=500,
            xaxis=dict(tickangle=-45),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode='x unified'
        )

        result = {
            'success': True,
            'graph_data': fig.to_json(),
            'stats': {
                'total_exp': int(vip_data['delta_exp'].sum()),
                'avg_exp': float(vip_data['delta_exp'].mean()),
                'max_exp': int(vip_data['delta_exp'].max()),
                'total_online': int(vip_data['delta_online'].sum()),
                'avg_online': float(vip_data['delta_online'].mean()),
                'updates': len(vip_data)
            }
        }
        del vip_data, fig, time_labels, compressed_exp, compressed_online, compressed_online_display, time_diffs
        gc.collect()
        return result
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error generating VIP graph: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# Scraper thread functions
def scrape_vip_data(database, world):
    """Scrape VIP player data for a specific world"""
    vips = database.get_vips()
    if not vips:
        return
    
    world_vips = [v for v in vips if v['world'] == world]
    if not world_vips:
        return
    
    log_console(f"Scraping {len(world_vips)} VIP players for {world}...", "INFO")
    for vip in world_vips:
        scrape_single_vip(database, vip['name'], vip['world'])


def loop_get_rankings(database, debug=False):
    """Background loop to continuously fetch rankings"""
    database.load(DATA_FOLDER)
    global scraper_running, scraper_state
    scraper_running = True
    
    last_updates = {}
    scraping_config = database.get_scraping_config()
    log_console(f"Starting ranking scraper for {len(scraping_config)} world(s)")
    
    ignore_updates = []
    while scraper_running:
        try:
            with scraper_lock:
                scraper_state = "checking"
            
            all_status = get_last_status_updates()
            
            if not all_status:
                log_console("Failed to get status data, retrying...", "WARNING")
                with scraper_lock:
                    scraper_state = "sleeping"
                time.sleep(60)
                continue
            
            json_data = {
                "fetch_time": datetime.now().isoformat(),
                "worlds": {}
            }
            
            for world_name, df in all_status.items():
                if not df.empty and 'last update' in df.columns:
                    world_data = df.to_dict('records')
                    for record in world_data:
                        if 'last update' in record and pd.notna(record['last update']):
                            dt = pd.to_datetime(record['last update']) - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                            record['last update'] = dt.isoformat()
                        else:
                            record['last update'] = None
                    json_data["worlds"][world_name] = world_data
            
            database.save_status_data(json_data)
            
            worlds_to_scrape = []
            for config_item in scraping_config:
                world = config_item['world']
                
                if world not in all_status:
                    log_console(f"World '{world}' not found in status data, skipping", "WARNING")
                    continue
                
                df = all_status[world]
                if 'rotina' not in df.columns or 'last update' not in df.columns:
                    log_console(f"Invalid data structure for world '{world}', skipping", "WARNING")
                    continue
                
                daily_raw = df[df['rotina'] == 'Daily Raw Ranking']
                if daily_raw.empty:
                    log_console(f"No 'Daily Raw Ranking' data for world '{world}', skipping", "WARNING")
                    continue
                
                current_update = pd.to_datetime(daily_raw['last update'].values[0])
                
                if pd.isna(current_update):
                    log_console(f"Invalid update time (NaT) for world '{world}', skipping", "WARNING")
                    continue
                
                if world not in last_updates or last_updates[world] != current_update:
                    if current_update not in ignore_updates:
                        worlds_to_scrape.append({
                            'config': config_item,
                            'update_time': current_update
                        })
                        log_console(f"New update for {world}: {last_updates.get(world, 'na')} -> {current_update}", "INFO")
            
            if not worlds_to_scrape:
                if debug:
                    log_console("No new updates found for any world, sleeping 60s", "DEBUG")
                with scraper_lock:
                    scraper_state = "sleeping"
                time.sleep(60)
            else:
                with scraper_lock:
                    scraper_state = "scraping"
                
                worlds_updated = 0
                for item in worlds_to_scrape:
                    config_item = item['config']
                    update_time = item['update_time']
                    world = config_item['world']
                    guilds = config_item['guilds']
                    
                    log_console(f"Processing world: {world} at {update_time}", "INFO")
                    
                    world_players = []
                    for guild in guilds:
                        try:
                            log_console(f"Scraping {world} - {guild}", "INFO")
                            r = get_ranking(world=world, guildname=guild)
                            
                            if r is None or len(r) < 2:
                                log_console(f"No data for {world} - {guild}", "WARNING")
                            else:
                                rankings = r[1]
                                rankparsed = parse_to_db_formatted(rankings, update_time, world=world, guild=guild)
                                world_players.append(rankparsed)
                                log_console(f"Got {len(rankparsed)} players from {world} - {guild}", "SUCCESS")
                        except Exception as e:
                            log_console(f"Error scraping {world} - {guild}: {str(e)}", "ERROR")

                    log_console("Scraping VIP data...", "INFO")
                    scrape_vip_data(database, world)
                    
                    if world_players:
                        combined_df = pd.concat(world_players, ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=['name'], keep='first')
                        
                        database.update(combined_df, update_time)
                        database.save()
                        log_console(f"Updated {len(combined_df)} players for {world} at {update_time}", "SUCCESS")
                        
                        del combined_df
                        clean_memory()
                        
                        last_updates[world] = update_time
                        if update_time not in ignore_updates:
                            ignore_updates.append(update_time)
                        
                        worlds_updated += 1
                    else:
                        log_console(f"No player data collected for {world}", "WARNING")
                
                if worlds_updated > 0:
                    log_console(f"Successfully updated {worlds_updated} world(s)", "SUCCESS")
                else:
                    log_console("No worlds were updated", "WARNING")
                
                with scraper_lock:
                    scraper_state = "idle"
        except Exception as e:
            log_console(f"Error in scraper: {str(e)}", "ERROR")
            traceback.print_exc()
            with scraper_lock:
                scraper_state = "sleeping"
            time.sleep(10)


def start_scraper_thread(database):
    """Start the scraper in a background thread with auto-restart"""
    global scraper_thread
    
    def run_with_restart():
        while True:
            try:
                log_console("Starting scraper thread...")
                loop_get_rankings(database, debug=True)
            except Exception as e:
                log_console(f"Scraper crashed: {str(e)}. Restarting in 1s...", "ERROR")
                time.sleep(1)

    scraper_thread = threading.Thread(target=run_with_restart, daemon=True)
    scraper_thread.start()
    log_console("Scraper thread started with auto-restart enabled")


# Initialize database and start scraper
db.load()
start_scraper_thread(db)


if __name__ == '__main__':
    import uvicorn

    log_console("Starting FastAPI server with Uvicorn on 0.0.0.0:5000", "INFO")
    uvicorn.run(app, host='0.0.0.0', port=5000, log_level="info")
