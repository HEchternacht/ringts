"""
Database abstraction layer for storing player EXP data.
Currently uses CSV files, designed to be easily swappable with SQLite.
"""
import os
import threading
import json
from datetime import datetime, timedelta
import pandas as pd


# Configuration from environment variables
DEFAULT_WORLD = os.environ.get('DEFAULT_WORLD', 'Auroria')
DEFAULT_GUILD = os.environ.get('DEFAULT_GUILD', 'Ascended Auroria')
DATA_FOLDER = os.environ.get('DATA_FOLDER', '/var/data')
TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
DAILY_RESET_HOUR = int(os.environ.get('DAILY_RESET_HOUR', '10'))
DAILY_RESET_MINUTE = int(os.environ.get('DAILY_RESET_MINUTE', '2'))


class Database:
    """
    Database abstraction layer for storing player EXP data.
    Currently uses CSV files, designed to be easily swappable with SQLite.
    """
    def __init__(self, folder=None, log_func=None):
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
        self.reset_done_today = False  # Flag to avoid multiple reset checks
        self.skip_next_deltas = False  # Flag to skip first delta after daily reset
        self.log_func = log_func  # Function for logging
        
        # Ensure data directory exists
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        # Initialize scraping config if it doesn't exist
        self._initialize_scraping_config()
        self._initialize_vip_files()
    
    def _log(self, message, level="INFO"):
        """Log message using provided log function"""
        if self.log_func:
            self.log_func(message, level)
    
    def _read_exps(self):
        """Read exps table from storage"""
        try:
            df = pd.read_csv(self.exps_file, dtype={'name': str, 'exp': 'int64', 'world': str, 'guild': str}, parse_dates=['last update'])
            if 'exp' not in df.select_dtypes(include=['int']).columns:
                df['exp'] = df['exp'].astype('int64')
            
            # Migrate legacy data: add world and guild columns if missing
            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                self._log(f"Migrated exps: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                self._log(f"Migrated exps: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
                # Save migrated data
                self._write_exps(df)
            
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'exp', 'last update', 'world', 'guild'])
    
    def _read_deltas(self):
        """Read deltas table from storage"""    
        try:
            df = pd.read_csv(self.deltas_file, dtype={'name': str, 'deltaexp': 'int64', 'world': str, 'guild': str}, parse_dates=['update time'])
            if 'deltaexp' not in df.select_dtypes(include=['int']).columns:
                df['deltaexp'] = df['deltaexp'].astype('int64')
            
            # Migrate legacy data: add world and guild columns if missing
            if 'world' not in df.columns:
                df['world'] = DEFAULT_WORLD
                self._log(f"Migrated deltas: added 'world' column with default '{DEFAULT_WORLD}'", "INFO")
            if 'guild' not in df.columns:
                df['guild'] = DEFAULT_GUILD
                self._log(f"Migrated deltas: added 'guild' column with default '{DEFAULT_GUILD}'", "INFO")
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
            self._log(f"Created default scraping config: {default_config}", "INFO")
    
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
            self._log(f"Error reading scraping config: {str(e)}, using default", "WARNING")
            self._initialize_scraping_config()
            return self.get_scraping_config()
    
    def save_scraping_config(self, config):
        """Save scraping configuration"""
        with open(self.scraping_data_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        self._log(f"Scraping config updated: {len(config)} world(s)", "INFO")

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
                    self._log(f"Found and removed {removed} duplicate deltas on load", "WARNING")
                    # Save cleaned data
                    self._write_deltas(deltas)
        
        self._log(f"Database initialized: {len(exps)} players, {len(deltas)} deltas")

    def save(self, folder=None):
        """Save database (for compatibility, now writes happen immediately)"""
        self._log("Database persisted to CSV files")

    def update(self, df, update_time, delta_queue=None):
        """Update player EXP data and record deltas"""
        import gc
        
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

            # Create index for faster lookups
            exps_dict = exps.set_index('name')[['exp']].to_dict('index')
            deltas_set = set(zip(deltas['name'], deltas['update time']))
            
            # Collect new rows instead of appending one-by-one
            new_deltas = []
            new_exps = []
            exps_updates = {}  # name -> updates dict
            deltas_updates = {}  # (name, time) -> deltaexp

            # Process each player
            for row in df.itertuples(index=False):
                name = row.name
                exp = int(row.exp)
                # Column 'last update' has a space, access with getattr or position
                last_update = getattr(row, 'last_update', getattr(row, '_2', None))
                world = getattr(row, 'world', DEFAULT_WORLD)
                guild = getattr(row, 'guild', DEFAULT_GUILD)

                if name in exps_dict:
                    # Existing player - calculate delta
                    prev_exp = exps_dict[name]['exp']
                    deltaexp = exp - prev_exp
                    if deltaexp != 0 and not self.skip_next_deltas:
                        delta_key = (name, update_time)
                        if delta_key not in deltas_set:
                            new_deltas.append({
                                'name': name, 
                                'deltaexp': deltaexp, 
                                'update time': update_time,
                                'world': world,
                                'guild': guild
                            })
                            self._log(f"EXP gain: {name} +{deltaexp} ({world} - {guild})")
                        else:
                            # Duplicate found - mark for update
                            deltas_updates[delta_key] = deltaexp
                            self._log(f"Updated duplicate for {name} at {update_time} (latest)", "INFO")
                        
                        # Broadcast to delta stream
                        if delta_queue:
                            delta_queue.put({
                                'name': name,
                                'deltaexp': int(deltaexp),
                                'update_time': update_time.isoformat(),
                                'prev_update_time': prev_update_time.isoformat(),
                                'world': world,
                                'guild': guild
                            })
                    elif deltaexp != 0 and self.skip_next_deltas:
                        self._log(f"Skipping first delta after reset for {name}: {deltaexp} ({world} - {guild})", "INFO")
                    
                    # Mark player for update
                    exps_updates[name] = {'exp': exp, 'last update': last_update, 'world': world, 'guild': guild}
                else:
                    # New player
                    new_exps.append({
                        'name': name, 
                        'exp': exp, 
                        'last update': last_update,
                        'world': world,
                        'guild': guild
                    })
                    
                    # Skip delta for new players if we just reset
                    if not self.skip_next_deltas:
                        delta_key = (name, update_time)
                        if delta_key not in deltas_set:
                            new_deltas.append({
                                'name': name, 
                                'deltaexp': exp, 
                                'update time': update_time,
                                'world': world,
                                'guild': guild
                            })
                            self._log(f"New player: {name} with {exp} EXP ({world} - {guild})")
                        else:
                            deltas_updates[delta_key] = exp
                            self._log(f"Updated duplicate for new player {name} at {update_time} (latest)", "INFO")
                        
                        # Broadcast to delta stream
                        if delta_queue:
                            delta_queue.put({
                                'name': name,
                                'deltaexp': int(exp),
                                'update_time': update_time.isoformat(),
                                'prev_update_time': prev_update_time.isoformat(),
                                'world': world,
                                'guild': guild
                            })
                    else:
                        self._log(f"Skipping first delta after reset for new player {name}: {exp} ({world} - {guild})", "INFO")
            
            # Free df after processing all rows
            del df
            
            # Apply updates efficiently
            for name, updates in exps_updates.items():
                mask = exps['name'] == name
                for col, val in updates.items():
                    exps.loc[mask, col] = val
            
            for (name, time), deltaexp in deltas_updates.items():
                mask = (deltas['name'] == name) & (deltas['update time'] == time)
                deltas.loc[mask, 'deltaexp'] = deltaexp
            
            # Add new rows in batch
            if new_exps:
                exps = pd.concat([exps, pd.DataFrame(new_exps)], ignore_index=True)
            if new_deltas:
                deltas = pd.concat([deltas, pd.DataFrame(new_deltas)], ignore_index=True)
            
            # Reset the skip flag after processing all players in this update
            if self.skip_next_deltas:
                self.skip_next_deltas = False
                self._log("Reset skip_next_deltas flag - next update will record deltas normally", "INFO")
            
            # Write changes to storage immediately
            self._write_exps(exps)
            self._write_deltas(deltas)
            
            # Free memory
            del exps_dict, deltas_set, new_deltas, new_exps, exps_updates, deltas_updates, exps, deltas
            gc.collect()
    
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
            self._log("Daily ranking reset triggered before first update after 10:02 AM", "INFO")
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
                
                self._log(f"Reset {len(exps)} players' EXP to 0. Historical data preserved.", "SUCCESS")
            
            # Update last reset date
            with open(self.reset_date_file, 'w') as f:
                f.write(today_str)
            
            self.reset_done_today = True  # Mark flag after successful reset
            self.skip_next_deltas = True  # Skip the first delta after reset
            self._log("Set skip_next_deltas flag - next update will skip recording deltas", "INFO")
            
            # Also reset VIP data
            self._reset_vip_daily()
            
            return True
    
    def _initialize_vip_files(self):
        """Initialize VIP tracking files if they don't exist"""
        # Initialize vips.txt
        if not os.path.exists(self.vips_file):
            with open(self.vips_file, 'w', encoding='utf-8') as f:
                f.write("")
            self._log("Created vips.txt", "INFO")
        
        # Initialize vipsdata.csv
        if not os.path.exists(self.vipsdata_file):
            df = pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
            df.to_csv(self.vipsdata_file, index=False)
            self._log("Created vipsdata.csv", "INFO")
        
        # Initialize deltavip.csv
        if not os.path.exists(self.deltavip_file):
            df = pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])
            df.to_csv(self.deltavip_file, index=False)
            self._log("Created deltavip.csv", "INFO")
    
    def get_vips(self):
        """Get list of VIP players"""
        with self.lock:
            try:
                with open(self.vips_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                vips = []
                for line in lines:
                    line = line.strip()
                    if line and ',' in line:
                        name, world = line.split(',', 1)
                        vips.append({'name': name.strip(), 'world': world.strip()})
                return vips
            except FileNotFoundError:
                return []
    
    def add_vip(self, name, world):
        """Add a VIP player"""
        with self.lock:
            # Read VIPs directly to avoid nested lock
            try:
                with open(self.vips_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                vips = []
                for line in lines:
                    line = line.strip()
                    if line and ',' in line:
                        vip_name, vip_world = line.split(',', 1)
                        vips.append({'name': vip_name.strip(), 'world': vip_world.strip()})
            except FileNotFoundError:
                vips = []
            
            # Check if already exists
            for vip in vips:
                if vip['name'] == name and vip['world'] == world:
                    return False
            # Add new VIP
            with open(self.vips_file, 'a', encoding='utf-8') as f:
                f.write(f"{name},{world}\n")
            self._log(f"Added VIP: {name} ({world})", "SUCCESS")
            return True
    
    def remove_vip(self, name, world):
        """Remove a VIP player"""
        with self.lock:
            # Read VIPs directly to avoid nested lock
            try:
                with open(self.vips_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                vips = []
                for line in lines:
                    line = line.strip()
                    if line and ',' in line:
                        vip_name, vip_world = line.split(',', 1)
                        vips.append({'name': vip_name.strip(), 'world': vip_world.strip()})
            except FileNotFoundError:
                vips = []
            
            new_vips = [v for v in vips if not (v['name'] == name and v['world'] == world)]
            if len(new_vips) == len(vips):
                return False
            # Write back
            with open(self.vips_file, 'w', encoding='utf-8') as f:
                for vip in new_vips:
                    f.write(f"{vip['name']},{vip['world']}\n")
            self._log(f"Removed VIP: {name} ({world})", "SUCCESS")
            return True
    
    def get_vipsdata(self):
        """Get current VIP data"""
        with self.lock:
            try:
                df = pd.read_csv(self.vipsdata_file, dtype={'name': str, 'world': str, 'today_exp': 'int64', 'today_online': 'int64'})
                return df
            except (FileNotFoundError, pd.errors.EmptyDataError):
                return pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
    
    def update_vipdata(self, name, world, today_exp, today_online):
        """Update VIP today's data"""
        with self.lock:
            # Read VIP data directly to avoid nested lock
            try:
                df = pd.read_csv(self.vipsdata_file, dtype={'name': str, 'world': str, 'today_exp': 'int64', 'today_online': 'int64'})
            except (FileNotFoundError, pd.errors.EmptyDataError):
                df = pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
            
            # Check if entry exists
            mask = (df['name'] == name) & (df['world'] == world)
            if mask.any():
                df.loc[mask, 'today_exp'] = today_exp
                df.loc[mask, 'today_online'] = today_online
            else:
                new_row = pd.DataFrame([{
                    'name': name,
                    'world': world,
                    'today_exp': today_exp,
                    'today_online': today_online
                }])
                df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(self.vipsdata_file, index=False)
    
    def get_deltavip(self):
        """Get VIP delta history"""
        with self.lock:
            try:
                df = pd.read_csv(self.deltavip_file, 
                                dtype={'name': str, 'world': str, 'date': str, 'delta_exp': 'int64', 'delta_online': 'int64'},
                                parse_dates=['update_time'])
                return df
            except (FileNotFoundError, pd.errors.EmptyDataError):
                return pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])
    
    def add_vip_delta(self, name, world, date, delta_exp, delta_online, update_time):
        """Add VIP delta record"""
        with self.lock:
            # Read VIP delta data directly to avoid nested lock
            try:
                df = pd.read_csv(self.deltavip_file,
                                dtype={'name': str, 'world': str, 'date': str, 'delta_exp': 'int64', 'delta_online': 'int64'},
                                parse_dates=['update_time'])
            except (FileNotFoundError, pd.errors.EmptyDataError):
                df = pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])
            
            new_row = pd.DataFrame([{
                'name': name,
                'world': world,
                'date': date,
                'delta_exp': delta_exp,
                'delta_online': delta_online,
                'update_time': update_time
            }])
            df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(self.deltavip_file, index=False)
            self._log(f"VIP delta: {name} ({world}) +{delta_exp} exp, +{delta_online} online", "INFO")
    
    def _reset_vip_daily(self):
        """Reset VIP data at daily reset"""
        with self.lock:
            # Clear today's data
            df = pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
            df.to_csv(self.vipsdata_file, index=False)
            self._log("Reset VIP daily data", "INFO")
