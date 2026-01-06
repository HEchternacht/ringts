import os
import sys
import threading
import time
import queue
import json
from io import StringIO
from flask import Flask, render_template, jsonify, request, Response
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import bs4
import werkzeug.exceptions
import traceback

app = Flask(__name__)

# Configuration from environment variables
UPLOAD_PASSWORD = os.environ.get('UPLOAD_PASSWORD', 'Rollabostx1234')
DEFAULT_WORLD = os.environ.get('DEFAULT_WORLD', 'Auroria')
DEFAULT_GUILD = os.environ.get('DEFAULT_GUILD', 'Ascended Auroria')
DATA_FOLDER = os.environ.get('DATA_FOLDER', '/var/data')
TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
DAILY_RESET_HOUR = int(os.environ.get('DAILY_RESET_HOUR', '10'))
DAILY_RESET_MINUTE = int(os.environ.get('DAILY_RESET_MINUTE', '2'))


# Error Handlers
@app.errorhandler(400)
def handle_bad_request(e):
    """Handle 400 Bad Request errors"""
    return jsonify({
        'error': 'Bad Request',
        'message': str(e),
        'status': 400
    }), 400

@app.errorhandler(404)
def handle_not_found(e):
    """Handle 404 Not Found errors"""
    return jsonify({
        'error': 'Not Found',
        'message': str(e),
        'status': 404
    }), 404

@app.errorhandler(405)
def handle_method_not_allowed(e):
    """Handle 405 Method Not Allowed errors"""
    return jsonify({
        'error': 'Method Not Allowed',
        'message': str(e),
        'status': 405
    }), 405

@app.errorhandler(500)
def handle_internal_error(e):
    """Handle 500 Internal Server errors"""
    error_msg = str(e)
    log_console(f"Internal server error: {error_msg}", "ERROR")
    return jsonify({
        'error': 'Internal Server Error',
        'message': error_msg,
        'status': 500
    }), 500

@app.errorhandler(Exception)
def handle_exception(e):
    """Handle all unhandled exceptions"""
    error_msg = str(e)
    error_type = type(e).__name__
    
    # Log the full traceback for debugging
    log_console(f"Unhandled exception ({error_type}): {error_msg}", "ERROR")
    
    # Return a clean JSON error response
    return jsonify({
        'error': error_type,
        'message': error_msg,
        'status': 500
    }), 500
pp=['http://103.155.62.141:8081',
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

import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_resp(url, proxy):
    try:
        with httpx.Client(proxy=proxy) as client:
            response = client.get(url, timeout=10)
        return response
    except Exception as e:
        return str(e)


def get_multiple(url: str, proxies: list):
    results = []
    # Use ThreadPoolExecutor with up to 4 workers
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit tasks
        future_to_proxy = {executor.submit(get_resp, url, proxy): proxy for proxy in proxies}

        for future in as_completed(future_to_proxy):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append(str(e))

    # Filter successful responses
    rsuccess = [x for x in results if not isinstance(x, str)]
    if rsuccess:
        return rsuccess[0]
    return None





# Console log queue for real-time display
console_queue = queue.Queue()
delta_queue = queue.Queue()
scraper_running = False
scraper_state = "idle"  # idle, checking, scraping, sleeping
scraper_lock = threading.Lock()
last_status_check = None



class Database:
    """
    Database abstraction layer for storing player EXP data.
    Currently uses CSV files, designed to be easily swappable with SQLite.
    """
    def __init__(self, folder=None):
        if folder is None:
            folder = DATA_FOLDER
        self.folder = folder
        self.exps_file = f"{folder}/exps.csv"
        self.deltas_file = f"{folder}/deltas.csv"
        self.reset_date_file = f"{folder}/last_reset.txt"
        self.status_data_file = f"{folder}/status_data.json"
        self.scraping_data_file = f"{folder}/scraping_data.json"
        self.lock = threading.Lock()
        self.reset_done_today = False  # Flag to avoid multiple reset checks
        
        # Ensure data directory exists
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        # Initialize scraping config if it doesn't exist
        self._initialize_scraping_config()
    
    def _read_exps(self):
        """Read exps table from storage"""
        try:
            df = pd.read_csv(self.exps_file)
            df['last update'] = pd.to_datetime(df['last update'])
            df['exp'] = df['exp'].astype(int)
            
            # Migrate legacy data: add world and guild columns if missing
            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                log_console(f"Migrated exps: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                log_console(f"Migrated exps: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
                # Save migrated data
                self._write_exps(df)
            
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'exp', 'last update', 'world', 'guild'])
    
    def _read_deltas(self):
        """Read deltas table from storage"""
        try:
            df = pd.read_csv(self.deltas_file)
            df['update time'] = pd.to_datetime(df['update time'])
            df['deltaexp'] = df['deltaexp'].astype(int)
            
            # Migrate legacy data: add world and guild columns if missing
            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                log_console(f"Migrated deltas: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                log_console(f"Migrated deltas: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
                # Save migrated data
                self._write_deltas(df)
            
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'deltaexp', 'update time', 'world', 'guild'])
    
    def _write_exps(self, df):
        """Write exps table to storage"""
        df.to_csv(self.exps_file, index=False)
    
    def _write_deltas(self, df):
        """Write deltas table to storage"""
        df.to_csv(self.deltas_file, index=False)
    
    def _read_status_data(self):
        """Read status data from JSON file"""
        try:
            with open(self.status_data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return None
    
    def _write_status_data(self, data):
        """Write status data to JSON file"""
        with open(self.status_data_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def get_status_data(self):
        """Get status data with lock"""
        with self.lock:
            return self._read_status_data()
    
    def save_status_data(self, data):
        """Save status data with lock"""
        with self.lock:
            self._write_status_data(data)
    
    def _initialize_scraping_config(self):
        """Initialize scraping config with default if not exists"""
        if not os.path.exists(self.scraping_data_file):
            default_config = [
                {
                    "world": DEFAULT_WORLD,
                    "guilds": [DEFAULT_GUILD]
                }
            ]
            with open(self.scraping_data_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            log_console(f"Created default scraping config: {default_config}", "INFO")
    
    def get_scraping_config(self):
        """Get scraping configuration"""
        try:
            with open(self.scraping_data_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # Validate config structure
                if not isinstance(config, list):
                    raise ValueError("Config must be an array")
                for item in config:
                    if 'world' not in item or 'guilds' not in item:
                        raise ValueError("Each config item must have 'world' and 'guilds' fields")
                    if not isinstance(item['guilds'], list):
                        raise ValueError("'guilds' must be an array")
                return config
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            log_console(f"Error reading scraping config: {str(e)}, using default", "WARNING")
            self._initialize_scraping_config()
            return self.get_scraping_config()
    
    def save_scraping_config(self, config):
        """Save scraping configuration"""
        with open(self.scraping_data_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        log_console(f"Scraping config updated: {len(config)} world(s)", "INFO")

    def load(self, folder=None):
        """Initialize database (for compatibility, now reads on-demand)"""
        if folder:
            self.folder = folder
            self.exps_file = f"{folder}/exps.csv"
            self.deltas_file = f"{folder}/deltas.csv"
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        with self.lock:
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            # Check and fix duplicates in deltas
            if not deltas.empty:
                duplicates = deltas[deltas.duplicated(subset=['name', 'update time'], keep=False)]
                if not duplicates.empty:
                    original_count = len(deltas)
                    # Keep last occurrence (most recent in file)
                    deltas = deltas.drop_duplicates(subset=['name', 'update time'], keep='last')
                    removed = original_count - len(deltas)
                    log_console(f"Found and removed {removed} duplicate deltas on load", "WARNING")
                    # Save cleaned data
                    self._write_deltas(deltas)
        
        log_console(f"Database initialized: {len(exps)} players, {len(deltas)} deltas")

    def save(self, folder=None):
        """Save database (for compatibility, now writes happen immediately)"""
        log_console("Database persisted to CSV files")

    def update(self, df, update_time):
        """Update player EXP data and record deltas"""
        with self.lock:
            # Read current data
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            # Calculate previous update time once using distinct update times
            if not deltas.empty:
                distinct_times = deltas['update time'].unique()
                prev_times = [t for t in distinct_times if t < update_time]
                prev_update_time = max(prev_times) if prev_times else update_time
            else:
                prev_update_time = update_time

            # Process each player
            for index, row in df.iterrows():
                name = row['name']
                exp = int(row['exp'])
                last_update = row['last update']
                world = row.get('world', DEFAULT_WORLD)
                guild = row.get('guild', DEFAULT_GUILD)

                if name in exps['name'].values:
                    # Existing player - calculate delta
                    prev_exp = exps[exps['name'] == name]['exp'].values[0]
                    deltaexp = exp - prev_exp
                    if deltaexp != 0:
                        # Check if delta already exists for this player and update time
                        existing_delta_mask = (deltas['name'] == name) & (deltas['update time'] == update_time)
                        existing_delta = deltas[existing_delta_mask]
                        
                        if existing_delta.empty:
                            new_delta = {
                                'name': name, 
                                'deltaexp': deltaexp, 
                                'update time': update_time,
                                'world': world,
                                'guild': guild
                            }
                            log_console(f"EXP gain: {name} +{deltaexp} ({world} - {guild})")
                            deltas.loc[len(deltas)] = new_delta
                        else:
                            # Duplicate found - update with latest value
                            existing_exp = existing_delta['deltaexp'].values[0]
                            deltas.loc[existing_delta_mask, 'deltaexp'] = deltaexp
                            log_console(f"Updated duplicate for {name} at {update_time}: {existing_exp} -> {deltaexp} (latest)", "INFO")
                        
                        # Broadcast to delta stream
                        delta_queue.put({
                            'name': name,
                            'deltaexp': int(deltaexp),
                            'update_time': update_time.isoformat(),
                            'prev_update_time': prev_update_time.isoformat(),
                            'world': world,
                            'guild': guild
                        })
                    
                    # Update existing player
                    exps.loc[exps['name'] == name, 'exp'] = exp
                    exps.loc[exps['name'] == name, 'last update'] = last_update
                    exps.loc[exps['name'] == name, 'world'] = world
                    exps.loc[exps['name'] == name, 'guild'] = guild
                else:
                    # New player
                    new_entry = {
                        'name': name, 
                        'exp': exp, 
                        'last update': last_update,
                        'world': world,
                        'guild': guild
                    }
                    exps.loc[len(exps)] = new_entry
                    
                    # Check if delta already exists for this player and update time
                    existing_delta_mask = (deltas['name'] == name) & (deltas['update time'] == update_time)
                    existing_delta = deltas[existing_delta_mask]
                    
                    if existing_delta.empty:
                        new_delta = {
                            'name': name, 
                            'deltaexp': exp, 
                            'update time': update_time,
                            'world': world,
                            'guild': guild
                        }
                        deltas.loc[len(deltas)] = new_delta
                        log_console(f"New player: {name} with {exp} EXP ({world} - {guild})")
                    else:
                        # Duplicate found - update with latest value
                        existing_exp = existing_delta['deltaexp'].values[0]
                        deltas.loc[existing_delta_mask, 'deltaexp'] = exp
                        log_console(f"Updated duplicate for new player {name} at {update_time}: {existing_exp} -> {exp} (latest)", "INFO")
                    
                    # Broadcast to delta stream
                    delta_queue.put({
                        'name': name,
                        'deltaexp': int(exp),
                        'update_time': update_time.isoformat(),
                        'prev_update_time': prev_update_time.isoformat(),
                        'world': world,
                        'guild': guild
                    })
            
            # Write changes to storage immediately
            self._write_exps(exps)
            self._write_deltas(deltas)
    
    def get_exps(self):
        """Get all player EXP data"""
        with self.lock:
            return self._read_exps()
    
    def get_deltas(self):
        """Get all delta records"""
        with self.lock:
            return self._read_deltas()
    
    def check_and_reset_daily(self, update_time=None):
        """Check if daily reset is needed before first valid update after 10:02 AM"""
        # Quick check: if we already reset today, skip all file I/O
        if self.reset_done_today:
            return False
        
        # If no update_time provided, just check without resetting
        if update_time is None:
            return False
        
        with self.lock:
            now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)  # Apply timezone offset
            today_str = now.strftime("%Y-%m-%d")
            
            # Convert update_time to datetime if it's not already
            if isinstance(update_time, str):
                update_time = pd.to_datetime(update_time)
            
            # Check if update is after daily reset time
            reset_time = now.replace(hour=DAILY_RESET_HOUR, minute=DAILY_RESET_MINUTE, second=0, microsecond=0)
            update_datetime = update_time
            if isinstance(update_datetime, pd.Timestamp):
                update_datetime = update_datetime.to_pydatetime()
            
            # If update is before 10:02, no reset needed
            if update_datetime.time() < reset_time.time():
                return False
            
            # Check last reset date
            try:
                with open(self.reset_date_file, 'r') as f:
                    last_reset = f.read().strip()
                if last_reset == today_str:
                    self.reset_done_today = True  # Mark flag
                    return False  # Already reset today
            except FileNotFoundError:
                pass  # No previous reset file, proceed with reset
            
            # Perform reset before processing this update
            log_console("Daily ranking reset triggered before first update after 10:02 AM", "INFO")
            exps = self._read_exps()
            deltas = self._read_deltas()
            
            if not exps.empty:
                # Create final deltas for today before reset to preserve history
                reset_timestamp = pd.to_datetime(reset_time)
           
                
                # Reset all EXP values to 0
                exps['exp'] = 0
                exps['last update'] = reset_timestamp
                
                # Save changes
                self._write_exps(exps)
                self._write_deltas(deltas)
                
                log_console(f"Reset {len(exps)} players' EXP to 0. Historical data preserved.", "SUCCESS")
            
            # Update last reset date
            with open(self.reset_date_file, 'w') as f:
                f.write(today_str)
            
            self.reset_done_today = True  # Mark flag after successful reset
            return True


# Logging function
def log_console(message, level="INFO"):
    timestamp = (datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    print(log_entry)
    console_queue.put(log_entry)



















# Scraper functions from notebook
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

    return dataframes


def scrape_player_data(name):
    """Complete pipeline to scrape player data from rubinothings.com.br"""
    result = {
        'name': name,
        'tables': [],
        'response_status': None,
        'success': False
    }
    
    url = "https://rubinothings.com.br/player"
    params = {"name": name}
    
    # Try direct fetch first
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            log_console(f"Direct fetch failed for '{name}' with status {response.status_code}, trying proxies...", "WARNING")
            # Build URL with params for proxy attempt
            url_with_params = f"{url}?name={name.replace(' ', '+')}"
            response = get_multiple(url_with_params, pp)
    except Exception as e:
        log_console(f"Direct fetch failed for '{name}': {str(e)}, trying proxies...", "WARNING")
        url_with_params = f"{url}?name={name.replace(' ', '+')}"
        response = get_multiple(url_with_params, pp)
    
    if not response:
        log_console(f"All requests failed for '{name}'", "ERROR")
        return result
    
    try:
        result['response_status'] = response.status_code if hasattr(response, 'status_code') else 200
        
        if result['response_status'] == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = extract_tables(soup)
            
            # Convert DataFrames to dict format for JSON serialization
            tables_dict = []
            for df in tables:
                tables_dict.append({
                    'columns': df.columns.tolist(),
                    'data': df.values.tolist()
                })
            
            result['tables'] = tables_dict
            result['success'] = True
            log_console(f"Successfully scraped data for '{name}' - Found {len(tables)} tables", "INFO")
        else:
            log_console(f"Request failed for '{name}' with status code: {result['response_status']}", "ERROR")
            
    except Exception as e:
        log_console(f"Error parsing player data for '{name}': {str(e)}", "ERROR")
    
    return result


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


def get_ranking(world=None, guildname=None):
    """Get ranking from website"""
    if world is None:
        world = DEFAULT_WORLD
    if guildname is None:
        guildname = DEFAULT_GUILD
    url = f"https://rubinothings.com.br/guild.php?guild={guildname.replace(' ', '+')}&world={world}"

    # Try direct fetch first
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
            response = get_multiple(url, pp)
    except Exception as e:
        log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
        response = get_multiple(url, pp)
    
    if not response:
        return None
    print(response)

    soup = BeautifulSoup(response.text, 'html.parser')
    return extract_tables(soup)


def get_last_status_updates(world=None):
    """Get status updates to determine correct scraping timestamp"""
    if world is None:
        world = DEFAULT_WORLD
    url = "https://rubinothings.com.br/status"
    
    # Try direct fetch first
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
            response = get_multiple(url, pp)
    except Exception as e:
        log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
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

    return tables_dict


def return_last_update(world=None, save_all_data=True, database=None):
    """Get the last update time and optionally save all worlds data to JSON"""
    if world is None:
        world = DEFAULT_WORLD
    if save_all_data and database:
        # Fetch all status data
        all_status = get_last_status_updates(world)
        
        if all_status:
            # Convert to JSON-serializable format
            json_data = {
                "fetch_time": datetime.now().isoformat(),
                "worlds": {}
            }
            
            for world_name, df in all_status.items():
                if not df.empty and 'last update' in df.columns:
                    # Convert DataFrame to dict and handle datetime
                    world_data = df.to_dict('records')
                    for record in world_data:
                        if 'last update' in record and pd.notna(record['last update']):
                            record['last update'] = pd.to_datetime(record['last update']).isoformat()
                        else:
                            record['last update'] = None
                    json_data["worlds"][world_name] = world_data
            
            # Save to JSON file
            database.save_status_data(json_data)
            log_console(f"Status data saved for {len(json_data['worlds'])} worlds", "INFO")
    
    # Get the specific world's update time
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


def loop_get_rankings(database, debug=False):
    """Background loop to continuously fetch rankings from all configured worlds and guilds"""
    database.load()
    global scraper_running, scraper_state
    scraper_running = True
    
    # Track last update per world (worlds update independently)
    last_updates = {}
    
    # Get scraping configuration
    scraping_config = database.get_scraping_config()
    log_console(f"Starting ranking scraper for {len(scraping_config)} world(s)")
    
    ignore_updates=[]
    while scraper_running:
        try:
            with scraper_lock:
                scraper_state = "checking"
            
            # Get status data for all worlds to check what's available
            all_status = get_last_status_updates()
            
            if not all_status:
                log_console("Failed to get status data, retrying...", "WARNING")
                with scraper_lock:
                    scraper_state = "sleeping"
                time.sleep(60)
                continue
            
            # Save all status data to JSON
            json_data = {
                "fetch_time": datetime.now().isoformat(),
                "worlds": {}
            }
            
            for world_name, df in all_status.items():
                if not df.empty and 'last update' in df.columns:
                    world_data = df.to_dict('records')
                    for record in world_data:
                        if 'last update' in record and pd.notna(record['last update']):
                            record['last update'] = pd.to_datetime(record['last update']).isoformat()
                        else:
                            record['last update'] = None
                    json_data["worlds"][world_name] = world_data
            
            database.save_status_data(json_data)
            
            # Check each configured world for updates
            worlds_to_scrape = []
            for config_item in scraping_config:
                world = config_item['world']
                
                # Check if world exists in status data
                if world not in all_status:
                    log_console(f"World '{world}' not found in status data, skipping", "WARNING")
                    continue
                
                # Get update time for this specific world
                df = all_status[world]
                if 'rotina' not in df.columns or 'last update' not in df.columns:
                    log_console(f"Invalid data structure for world '{world}', skipping", "WARNING")
                    continue
                
                daily_raw = df[df['rotina'] == 'Daily Raw Ranking']
                if daily_raw.empty:
                    log_console(f"No 'Daily Raw Ranking' data for world '{world}', skipping", "WARNING")
                    continue
                
                current_update = pd.to_datetime(daily_raw['last update'].values[0])
                
                # Check if this world has a new update
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
                # Process EACH WORLD separately with its own timestamp
                with scraper_lock:
                    scraper_state = "scraping"
                
                worlds_updated = 0
                for item in worlds_to_scrape:
                    config_item = item['config']
                    update_time = item['update_time']
                    world = config_item['world']
                    guilds = config_item['guilds']
                    
                    log_console(f"Processing world: {world} at {update_time}", "INFO")
                    
                    # Check for daily reset before processing this world
                    database.check_and_reset_daily(update_time)
                    
                    # Collect all players from all guilds in THIS world
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
                    
                    # Update database for THIS WORLD ONLY with ITS timestamp
                    if world_players:
                        combined_df = pd.concat(world_players, ignore_index=True)
                        # Remove duplicates (same player in multiple guilds - keep first)
                        combined_df = combined_df.drop_duplicates(subset=['name'], keep='first')
                        
                        # Update database with THIS world's specific timestamp
                        database.update(combined_df, update_time)
                        database.save()
                        log_console(f"Updated {len(combined_df)} players for {world} at {update_time}", "SUCCESS")
                        
                        # Mark this world as updated and add to ignore list
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
            time.sleep(10)  # Wait before retry


def start_scraper_thread(database):
    """Start the scraper in a background thread with auto-restart"""
    def run_with_restart():
        while True:
            try:
                log_console("Starting scraper thread...")
                loop_get_rankings(database, debug=True)
            except Exception as e:
                log_console(f"Scraper crashed: {str(e)}. Restarting in 1s...", "ERROR")
                time.sleep(1)

    thread = threading.Thread(target=run_with_restart, daemon=True)
    thread.start()
    log_console("Scraper thread started with auto-restart enabled")

# Initialize database
db = Database()
db.load()

# Start background scraper
start_scraper_thread(db)


def get_delta_between(datetime1, datetime2, database):
    """Filter deltas between two datetimes"""
    table = database.get_deltas()
    datetime1 = pd.to_datetime(datetime1)
    datetime2 = pd.to_datetime(datetime2)
    mask = (table['update time'] >= datetime1) & (table['update time'] <= datetime2)
    return table[mask]


def preprocess_vis_data(all_update_times, all_player_data, names_list):
    """
    Preprocess visualization data to compress consecutive zero periods.
    Returns compressed times and player data with zeros grouped.
    """
    num_times = len(all_update_times)
    
    # Identify positions where ALL players have zero
    all_zero_positions = []
    for i in range(num_times):
        if all(all_player_data[name][i] == 0 for name in names_list):
            all_zero_positions.append(i)
    
    # Group consecutive zero positions (only if there are 2+ consecutive zeros)
    zero_groups = []
    if all_zero_positions:
        start = all_zero_positions[0]
        for i in range(1, len(all_zero_positions)):
            if all_zero_positions[i] != all_zero_positions[i-1] + 1:
                # End of a consecutive group
                if all_zero_positions[i-1] - start >= 1:  # At least 2 consecutive zeros
                    zero_groups.append((start, all_zero_positions[i-1]))
                start = all_zero_positions[i]
        # Don't forget the last group
        if all_zero_positions[-1] - start >= 1:
            zero_groups.append((start, all_zero_positions[-1]))
    
    # Build compressed timeline - convert everything to strings with smart date display
    compressed_times = []
    compressed_data = {name: [] for name in names_list}
    
    prev_date = None
    i = 0
    while i < num_times:
        # Check if this position starts a zero group
        in_zero_group = False
        for start, end in zero_groups:
            if i == start:
                # Create label for zero period
                start_time = pd.to_datetime(all_update_times[start])
                end_time = pd.to_datetime(all_update_times[end])
                
                # Format with smart date display
                start_date = start_time.date()
                end_date = end_time.date()
                
                if start_date == end_date:
                    # Same day - show date once
                    if start_date != prev_date:
                        label = f"{start_time.strftime('%d/%m/%Y %H:%M')}->{end_time.strftime('%H:%M')}"
                    else:
                        label = f"{start_time.strftime('%H:%M')}->{end_time.strftime('%H:%M')}"
                else:
                    # Different days - show both dates
                    if start_date != prev_date:
                        label = f"{start_time.strftime('%d/%m/%Y %H:%M')}->{end_time.strftime('%d/%m/%Y %H:%M')}"
                    else:
                        label = f"{start_time.strftime('%H:%M')}->{end_time.strftime('%d/%m/%Y %H:%M')}"
                
                compressed_times.append(label)
                prev_date = end_date
                
                # Add single zero for each player for this period
                for name in names_list:
                    compressed_data[name].append(0)
                
                i = end + 1
                in_zero_group = True
                break
        
        if not in_zero_group:
            # Regular data point - convert to string with smart date display
            time_obj = pd.to_datetime(all_update_times[i])
            current_date = time_obj.date()
            
            if current_date != prev_date:
                # Date changed - show full date
                time_str = time_obj.strftime('%d/%m/%Y %H:%M')
            else:
                # Same day - show only time
                time_str = time_obj.strftime('%H:%M')
            
            compressed_times.append(time_str)
            prev_date = current_date
            
            for name in names_list:
                compressed_data[name].append(all_player_data[name][i])
            i += 1
    
    return compressed_times, compressed_data


def create_interactive_graph(names, database, datetime1=None, datetime2=None):
    """Create interactive Plotly graph for player EXP gains"""
    # Custom color palette based on theme colors
    theme_colors = [
        '#C21500',  # Primary red-orange
        '#FFC500',  # Primary golden yellow
        '#FF6B35',  # Complementary orange
        '#FFE156',  # Light yellow
        '#B81400',  # Darker red
        '#E6A900',  # Darker yellow
        '#FF8F66',  # Light orange
        '#FFD966',  # Pale yellow
    ]
    
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    # Handle single name or list of names
    names_list = [names] if isinstance(names, str) else names

    # Get all unique update times across all data (standardized timeline)
    all_update_times = sorted(table['update time'].unique())

    # Build data for all players first
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

    # Preprocess data to compress zero periods
    compressed_times, compressed_data = preprocess_vis_data(all_update_times, all_player_data, names_list)

    # Create plotly figure
    fig = go.Figure()

    # Build traces with compressed data
    for idx, name in enumerate(names_list):
        color = theme_colors[idx % len(theme_colors)]
        fig.add_trace(go.Bar(
            x=compressed_times,
            y=compressed_data[name],
            name=name,
            marker_color=color,
            text=[str(int(exp)) if exp > 0 else '' for exp in compressed_data[name]],
            textposition='outside',
            textangle=0,
            hovertemplate='<b>%{x}</b><br>EXP: %{y:,.0f}<extra></extra>'
        ))

    fig.update_layout(
        title=f'EXP Gain Over Time',
        xaxis_title='Update Time',
        yaxis_title='Delta EXP',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        showlegend=True,
        barmode='group',  # Group bars side by side for multiple players
        bargap=0,  # No gap between bars
        bargroupgap=0,  # No gap between bar groups
        xaxis=dict(
            type='category',  # Treat x-axis as categorical (string bins)
            tickangle=-45,
            tickmode='auto',
            nticks=20
        ),
        colorway=theme_colors  # Set default color palette
    )

    return fig.to_json()


def get_player_stats(names, database, datetime1=None, datetime2=None):
    """Get statistics table for players"""
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    # Filter by names if specified
    if names:
        names_list = [names] if isinstance(names, str) else names
        table = table[table['name'].isin(names_list)]

    # Group and calculate stats
    stats = table.groupby('name').agg({
        'deltaexp': ['sum', 'mean', 'count', 'max', 'min']
    }).round(2)

    stats.columns = ['Total EXP', 'Average EXP', 'Updates', 'Max EXP', 'Min EXP']
    stats = stats.sort_values('Total EXP', ascending=False)
    stats = stats.reset_index()

    return stats.to_dict('records')


@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')


@app.route('/api/players')
def get_players():
    """Get list of all players"""
    deltas = db.get_deltas()
    world = request.args.get('world')
    guild = request.args.get('guild')
    
    # Filter by world and guild if specified
    if world:
        deltas = deltas[deltas['world'] == world]
    if guild:
        deltas = deltas[deltas['guild'] == guild]
    
    players = sorted(deltas['name'].unique().tolist())
    return jsonify(players)


@app.route('/api/date-range')
def get_date_range():
    """Get available date range"""
    deltas = db.get_deltas()
    world = request.args.get('world')
    guild = request.args.get('guild')
    
    # Filter by world and guild if specified
    if world:
        deltas = deltas[deltas['world'] == world]
    if guild:
        deltas = deltas[deltas['guild'] == guild]
    
    if not deltas.empty:
        min_date = deltas['update time'].min()
        max_date = deltas['update time'].max()
        return jsonify({
            'min': min_date.isoformat(),
            'max': max_date.isoformat()
        })
    return jsonify({'min': None, 'max': None})


@app.route('/api/graph', methods=['POST'])
def get_graph():
    """Generate interactive graph with stats and comparison data"""
    data = request.json
    names = data.get('names', [])
    datetime1 = data.get('datetime1')
    datetime2 = data.get('datetime2')

    if not names:
        return jsonify({'error': 'No players selected'}), 400

    try:
        # Generate individual graph for each player (for carousel)
        # But send the same combined graph data to maintain compatibility
        graph_json = create_interactive_graph(names, db, datetime1, datetime2)

        # Get stats for selected players
        stats = get_player_stats(names, db, datetime1, datetime2)

        # Get comparison data (all players in same time period)
        deltas_table = db.get_deltas()
        exps_table = db.get_exps()

        if datetime1 and datetime2:
            deltas_table = get_delta_between(datetime1, datetime2, db)

        # Calculate overall rankings
        all_rankings = deltas_table.groupby('name')['deltaexp'].sum().sort_values(ascending=False)

        # Get rank for each selected player
        comparison = []
        for name in names:
            if name in all_rankings.index:
                rank = list(all_rankings.index).index(name) + 1
                total_exp = int(all_rankings[name])
                percentile = (1 - (rank / len(all_rankings))) * 100

                # Get current exp from exps table
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

        return jsonify({
            'graph': graph_json,
            'stats': stats,
            'comparison': comparison
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats', methods=['POST'])
def get_stats():
    """Get player statistics (deprecated - use /api/graph instead)"""
    data = request.json
    names = data.get('names', [])
    datetime1 = data.get('datetime1')
    datetime2 = data.get('datetime2')

    try:
        stats = get_player_stats(names, db, datetime1, datetime2)
        return jsonify({'stats': stats})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/top-players', methods=['GET'])
def get_top_players():
    """Get top players by total EXP"""
    limit = request.args.get('limit', 10, type=int)
    datetime1 = request.args.get('datetime1')
    datetime2 = request.args.get('datetime2')

    table = db.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, db)

    # Calculate top players
    top = table.groupby('name')['deltaexp'].sum().sort_values(ascending=False).head(limit)

    result = [{'name': name, 'total_exp': int(exp)} for name, exp in top.items()]
    return jsonify(result)


@app.route('/api/recent-updates', methods=['GET'])
def get_recent_updates():
    """Get recent EXP updates"""
    limit = request.args.get('limit', 20, type=int)

    deltas = db.get_deltas()
    recent = deltas.sort_values('update time', ascending=False).head(limit)
    result = recent.to_dict('records')

    # Convert datetime to string
    for item in result:
        item['update time'] = item['update time'].isoformat()

    return jsonify(result)


@app.route('/api/rankings-table', methods=['POST'])
def get_rankings_table():
    """Get grouped rankings table with filters and sorting"""
    data = request.json
    datetime1 = data.get('datetime1')
    datetime2 = data.get('datetime2')

    try:
        table = db.get_deltas()

        # If dates provided, filter; otherwise show all-time data
        if datetime1 and datetime2:
            table = get_delta_between(datetime1, datetime2, db)

        # Group by name and aggregate
        grouped = table.groupby('name').agg({
            'deltaexp': list,
            'update time': list
        })

        grouped['sum'] = grouped['deltaexp'].apply(sum)
        grouped['number of updates'] = grouped['deltaexp'].apply(len)
        grouped['avg'] = grouped['deltaexp'].apply(lambda x: sum(x) / len(x) if len(x) > 0 else 0)
        grouped['max'] = grouped['deltaexp'].apply(lambda x: max(x) if len(x) > 0 else 0)
        grouped['min'] = grouped['deltaexp'].apply(lambda x: min(x) if len(x) > 0 else 0)

        # Convert to records
        result = []
        for name, row in grouped.iterrows():
            result.append({
                'name': name,
                'total_exp': int(row['sum']),
                'updates': int(row['number of updates']),
                'avg_exp': round(row['avg'], 2),
                'max_exp': int(row['max']),
                'min_exp': int(row['min'])
            })

        return jsonify({'rankings': result})
    except Exception as e:
        log_console(f"Error in rankings table: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/console-stream')
def console_stream():
    """Server-Sent Events stream for console logs"""
    def generate():
        # Send initial connection message
        yield f"data: [CONNECTED] Console stream started\n\n"

        while True:
            try:
                # Get log from queue with timeout
                log = console_queue.get(timeout=1)
                yield f"data: {log}\n\n"
            except queue.Empty:
                # Send keepalive
                yield f": keepalive\n\n"

    return Response(generate(), mimetype='text/event-stream')


@app.route('/api/scraper-status')
def get_scraper_status():
    """Get scraper status"""
    global last_status_check
    last_status_check = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
    
    deltas = db.get_deltas()
    with scraper_lock:
        state = scraper_state
    
    return jsonify({
        'running': scraper_running,
        'state': state,  # idle, checking, scraping, sleeping
        'last_update': deltas['update time'].max().isoformat() if not deltas.empty else None,
        'last_check': last_status_check.isoformat()
    })


@app.route('/api/download/deltas')
def download_deltas():
    """Download deltas.csv file"""
    from flask import send_file
    try:
        return send_file(db.deltas_file, as_attachment=True, download_name='deltas.csv')
    except Exception as e:
        log_console(f"Error downloading deltas.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/download/exps')
def download_exps():
    """Download exps.csv file"""
    from flask import send_file
    try:
        return send_file(db.exps_file, as_attachment=True, download_name='exps.csv')
    except Exception as e:
        log_console(f"Error downloading exps.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/deltas', methods=['POST'])
def upload_deltas():
    """Upload deltas.csv file"""
    try:
        # Check password
        password = request.form.get('password')
        if password != UPLOAD_PASSWORD:
            return jsonify({'error': 'Invalid password'}), 401
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Only CSV files are allowed'}), 400
        
        # Read and validate the CSV
        df = pd.read_csv(file)
        required_columns = ['name', 'deltaexp', 'update time']
        if not all(col in df.columns for col in required_columns):
            return jsonify({'error': f'CSV must have columns: {required_columns}'}), 400
        
        # Backup existing file
        import shutil
        if os.path.exists(db.deltas_file):
            backup_file = db.deltas_file.replace('.csv', '_backup.csv')
            shutil.copy(db.deltas_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")
        
        # Save the uploaded file
        df.to_csv(db.deltas_file, index=False)
        log_console(f"Uploaded deltas.csv with {len(df)} records", "SUCCESS")
        
        return jsonify({'success': True, 'records': len(df)})
    except Exception as e:
        log_console(f"Error uploading deltas.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/upload/exps', methods=['POST'])
def upload_exps():
    """Upload exps.csv file"""
    try:
        # Check password
        password = request.form.get('password')
        if password != UPLOAD_PASSWORD:
            return jsonify({'error': 'Invalid password'}), 401
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Only CSV files are allowed'}), 400
        
        # Read and validate the CSV
        df = pd.read_csv(file)
        required_columns = ['name', 'exp', 'last update']
        if not all(col in df.columns for col in required_columns):
            return jsonify({'error': f'CSV must have columns: {required_columns}'}), 400
        
        # Backup existing file
        import shutil
        if os.path.exists(db.exps_file):
            backup_file = db.exps_file.replace('.csv', '_backup.csv')
            shutil.copy(db.exps_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")
        
        # Save the uploaded file
        df.to_csv(db.exps_file, index=False)
        log_console(f"Uploaded exps.csv with {len(df)} records", "SUCCESS")
        
        return jsonify({'success': True, 'records': len(df)})
    except Exception as e:
        log_console(f"Error uploading exps.csv: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/player-graph/<player_name>', methods=['GET'])
def get_player_graph(player_name):
    """Get individual player graph data"""
    datetime1 = request.args.get('datetime1')
    datetime2 = request.args.get('datetime2')

    try:
        graph_json = create_interactive_graph(player_name, db, datetime1, datetime2)
        return jsonify({'graph': graph_json, 'player': player_name})
    except Exception as e:
        log_console(f"Error generating player graph for {player_name}: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/player-details/<player_name>', methods=['GET'])
def get_player_details(player_name):
    """Get detailed player data from rubinothings.com.br"""
    try:
        player_data = scrape_player_data(player_name)
        return jsonify(player_data)
    except Exception as e:
        log_console(f"Error getting player details for {player_name}: {str(e)}", "ERROR")
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/delta', methods=['GET'])
def get_deltas():
    """Get recent delta updates for polling"""
    try:
        limit = request.args.get('limit', 100, type=int)
        world = request.args.get('world')
        guild = request.args.get('guild')
        
        all_deltas = db.get_deltas()
        
        if all_deltas.empty:
            return jsonify({'deltas': []})
        
        # Filter by world and guild if specified
        if world:
            all_deltas = all_deltas[all_deltas['world'] == world]
        if guild:
            all_deltas = all_deltas[all_deltas['guild'] == guild]
        
        if all_deltas.empty:
            return jsonify({'deltas': []})
        
        recent_deltas = all_deltas.sort_values(['update time', 'name'], ascending=[False, True]).head(limit)

        # Get distinct update times for efficient lookup
        distinct_times = sorted(all_deltas['update time'].unique())
        distinct_times_list = list(distinct_times)

        # Create a mapping of update_time -> prev_update_time
        prev_time_map = {}
        for i, current_time in enumerate(distinct_times_list):
            if i > 0:
                prev_time_map[current_time] = distinct_times_list[i - 1]
            else:
                prev_time_map[current_time] = current_time

        # Build delta list with calculated previous update times
        deltas = []
        for idx, row in recent_deltas.iterrows():
            current_time = row['update time']
            prev_update_time = prev_time_map.get(current_time, current_time)

            deltas.append({
                'name': row['name'],
                'deltaexp': int(row['deltaexp']),
                'update_time': current_time.isoformat(),
                'prev_update_time': prev_update_time.isoformat(),
                'world': row.get('world', DEFAULT_WORLD),
                'guild': row.get('guild', DEFAULT_GUILD)
            })
        
        return jsonify({'deltas': deltas})
    except Exception as e:
        log_console(f"Error getting deltas: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/status-data')
def get_status_data():
    """Get the stored status data from all worlds"""
    try:
        status_data = db.get_status_data()
        if status_data:
            return jsonify(status_data)
        else:
            return jsonify({
                'error': 'No status data available yet',
                'message': 'Status data will be available after the first update'
            }), 404
    except Exception as e:
        log_console(f"Error getting status data: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scraping-config', methods=['GET'])
def get_scraping_config():
    """Get the scraping configuration"""
    try:
        config = db.get_scraping_config()
        return jsonify(config)
    except Exception as e:
        log_console(f"Error getting scraping config: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scraping-config', methods=['POST'])
def update_scraping_config():
    """Update the scraping configuration"""
    try:
        # Check password
        password = request.json.get('password')
        if password != UPLOAD_PASSWORD:
            return jsonify({'error': 'Invalid password'}), 401
        
        config = request.json.get('config')
        if not config:
            return jsonify({'error': 'No config provided'}), 400
        
        # Validate config structure
        if not isinstance(config, list):
            return jsonify({'error': 'Config must be an array'}), 400
        
        for item in config:
            if 'world' not in item or 'guilds' not in item:
                return jsonify({'error': 'Each config item must have "world" and "guilds" fields'}), 400
            if not isinstance(item['guilds'], list):
                return jsonify({'error': '"guilds" must be an array'}), 400
        
        # Save the configuration
        db.save_scraping_config(config)
        log_console(f"Scraping configuration updated via API", "SUCCESS")
        
        return jsonify({'success': True, 'config': config})
    except Exception as e:
        log_console(f"Error updating scraping config: {str(e)}", "ERROR")
        return jsonify({'error': str(e)}), 500


@app.route('/api/manual-update', methods=['POST'])
def manual_update():
    """Manually trigger a ranking update"""
    global scraper_state
    
    # Check if scraper is already active (not sleeping or idle)
    with scraper_lock:
        current_state = scraper_state
    
    if current_state in ['checking', 'scraping']:
        return jsonify({
            'success': False,
            'message': 'Scraper is already running',
            'state': current_state
        }), 409
    
    try:
        log_console("Manual update triggered", "INFO")
        
        with scraper_lock:
            scraper_state = "checking"
        
        # Get scraping configuration
        scraping_config = db.get_scraping_config()
        first_world = scraping_config[0]['world'] if scraping_config else DEFAULT_WORLD
        
        current_update = return_last_update(first_world, save_all_data=True, database=db)
        
        if current_update is None:
            raise Exception("Failed to get update time")
        
        with scraper_lock:
            scraper_state = "scraping"
        
        # Scrape all worlds and guilds
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
        
        # Combine all players and update database
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
        
        log_console(f"Manual update completed successfully at {current_update}", "SUCCESS")
        return jsonify({
            'success': True,
            'message': 'Update completed successfully',
            'update_time': current_update.isoformat()
        })
    except Exception as e:
        error_msg = str(e)
        log_console(f"Manual update failed: {error_msg}", "ERROR")
        
        with scraper_lock:
            scraper_state = "idle"
        
        return jsonify({
            'success': False,
            'message': f'Update failed: {error_msg}'
        }), 500



if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
