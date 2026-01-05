import os
import sys
import threading
import time
import queue
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
    def __init__(self, folder="/var/data"):
        self.folder = folder
        self.exps_file = f"{folder}/exps.csv"
        self.deltas_file = f"{folder}/deltas.csv"
        self.reset_date_file = f"{folder}/last_reset.txt"
        self.lock = threading.Lock()
        self.reset_done_today = False  # Flag to avoid multiple reset checks
        
        # Ensure data directory exists
        if not os.path.exists(folder):
            os.makedirs(folder)
    
    def _read_exps(self):
        """Read exps table from storage"""
        try:
            df = pd.read_csv(self.exps_file)
            df['last update'] = pd.to_datetime(df['last update'])
            df['exp'] = df['exp'].astype(int)
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'exp', 'last update'])
    
    def _read_deltas(self):
        """Read deltas table from storage"""
        try:
            df = pd.read_csv(self.deltas_file)
            df['update time'] = pd.to_datetime(df['update time'])
            df['deltaexp'] = df['deltaexp'].astype(int)
            return df
        except FileNotFoundError:
            return pd.DataFrame(columns=['name', 'deltaexp', 'update time'])
    
    def _write_exps(self, df):
        """Write exps table to storage"""
        df.to_csv(self.exps_file, index=False)
    
    def _write_deltas(self, df):
        """Write deltas table to storage"""
        df.to_csv(self.deltas_file, index=False)

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

                if name in exps['name'].values:
                    # Existing player - calculate delta
                    prev_exp = exps[exps['name'] == name]['exp'].values[0]
                    deltaexp = exp - prev_exp
                    if deltaexp != 0:
                        new_delta = {'name': name, 'deltaexp': deltaexp, 'update time': update_time}
                        log_console(f"EXP gain: {name} +{deltaexp}")
                        deltas.loc[len(deltas)] = new_delta
                        
                        # Broadcast to delta stream
                        delta_queue.put({
                            'name': name,
                            'deltaexp': int(deltaexp),
                            'update_time': update_time.isoformat(),
                            'prev_update_time': prev_update_time.isoformat()
                        })
                    
                    # Update existing player
                    exps.loc[exps['name'] == name, 'exp'] = exp
                    exps.loc[exps['name'] == name, 'last update'] = last_update
                else:
                    # New player
                    new_entry = {'name': name, 'exp': exp, 'last update': last_update}
                    exps.loc[len(exps)] = new_entry
                    new_delta = {'name': name, 'deltaexp': exp, 'update time': update_time}
                    deltas.loc[len(deltas)] = new_delta
                    log_console(f"New player: {name} with {exp} EXP")
                    
                    # Broadcast to delta stream
                    delta_queue.put({
                        'name': name,
                        'deltaexp': int(exp),
                        'update_time': update_time.isoformat(),
                        'prev_update_time': prev_update_time.isoformat()
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
    
    def check_and_reset_daily(self):
        """Check if daily reset is needed (at 10:02 AM) and reset EXP if necessary"""
        # Quick check: if we already reset today, skip all file I/O
        if self.reset_done_today:
            return False
        
        with self.lock:
            now = datetime.now() - timedelta(hours=3)  # Apply timezone offset
            today_str = now.strftime("%Y-%m-%d")
            
            # Check if it's past 10:02 AM
            reset_time = now.replace(hour=10, minute=2, second=0, microsecond=0)
            if now < reset_time:
                return False  # Not yet time for reset today
            
            # Check last reset date
            try:
                with open(self.reset_date_file, 'r') as f:
                    last_reset = f.read().strip()
                if last_reset == today_str:
                    self.reset_done_today = True  # Mark flag
                    return False  # Already reset today
            except FileNotFoundError:
                pass  # No previous reset file, proceed with reset
            
            # Perform reset
            log_console("Daily ranking reset triggered at 10:02 AM", "INFO")
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
    timestamp = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
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


def parse_datetime(date_str):
    """Parse datetime from Brazilian format"""
    import re
    if "Hoje" in date_str:
        time_part = re.search(r'(\d{2}:\d{2})', date_str)
        if time_part:
            time_str = time_part.group(1)
            now = datetime.now() - timedelta(hours=3)
            date_time = datetime.strptime(f"{now.date()} {time_str}", "%Y-%m-%d %H:%M")
            return pd.to_datetime(date_time)
        else:
            yesterday = (datetime.now() - timedelta(hours=3)) - timedelta(days=1)
            return pd.to_datetime(datetime.strptime(f"{yesterday.date()} 00:00", "%Y-%m-%d %H:%M"))
    return None


def get_ranking(world="Auroria", guildname="Ascended Auroria"):
    """Get ranking from website"""
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


def get_last_status_updates(world="Auroria"):
    """Get status updates to determine correct scraping timestamp"""
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


def return_last_update(world="Auroria"):
    """Get the last update time"""
    df = get_last_status_updates(world)[world]
    update_time = str(df[df['rotina'] == 'Daily Raw Ranking']['last update'].values[0])
    return pd.to_datetime(update_time)


def parse_to_db_formatted(df, last_update):
    """Parse ranking data to database format"""
    new_df = pd.DataFrame()
    new_df['name'] = df['Jogador']
    new_df['exp'] = df['RAW no período'].str.replace(',', '').str.replace('.', '').astype(int)
    new_df['last update'] = last_update
    return new_df


def loop_get_rankings(database, world="Auroria", debug=False):
    """Background loop to continuously fetch rankings"""
    database.load()
    global scraper_running, scraper_state
    scraper_running = True
    last_update = 'na'
    log_console(f"Starting ranking scraper for {world}")

    while scraper_running:
        try:
            # Check for daily reset before processing updates
            database.check_and_reset_daily()
            
            with scraper_lock:
                scraper_state = "checking"
            
            current_update = return_last_update(world)
            
            if last_update == current_update:
                if debug:
                    log_console("No new update found, sleeping 10s", "DEBUG")
                with scraper_lock:
                    scraper_state = "sleeping"
                time.sleep(60)
            else:
                log_console(f"New update detected: {last_update} -> {current_update}")
                with scraper_lock:
                    scraper_state = "scraping"
                
                rankings = get_ranking()[1]
                rankparsed = parse_to_db_formatted(rankings, current_update)
                database.update(rankparsed, current_update)
                database.save()
                last_update = current_update
                log_console(f"Rankings updated successfully at {current_update}", "SUCCESS")
                
                with scraper_lock:
                    scraper_state = "idle"
        except Exception as e:
            log_console(f"Error in scraper: {str(e)}", "ERROR")
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


def create_interactive_graph(names, database, datetime1=None, datetime2=None):
    """Create interactive Plotly graph for player EXP gains"""
    table = database.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, database)

    # Handle single name or list of names
    names_list = [names] if isinstance(names, str) else names

    # Get all unique update times across all data (standardized timeline)
    all_update_times = sorted(table['update time'].unique())

    # Create plotly figure
    fig = go.Figure()

    for name in names_list:
        # Get player's deltas
        player_data = table[table['name'] == name]

        if not player_data.empty:
            # Create a dictionary of time -> deltaexp for this player
            player_deltas = dict(zip(player_data['update time'], player_data['deltaexp']))

            # Fill in missing times with 0
            standardized_times = []
            standardized_exps = []

            for update_time in all_update_times:
                standardized_times.append(update_time)
                standardized_exps.append(player_deltas.get(update_time, 0))

            fig.add_trace(go.Scatter(
                x=standardized_times,
                y=standardized_exps,
                mode='lines+markers+text',
                name=name,
                text=[str(exp) if exp > 0 else '' for exp in standardized_exps],
                textposition='top center',
                marker=dict(size=10),
                line=dict(width=2)
            ))

    fig.update_layout(
        title=f'EXP Gain Over Time',
        xaxis_title='Update Time',
        yaxis_title='Delta EXP',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        showlegend=True
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
    players = sorted(deltas['name'].unique().tolist())
    return jsonify(players)


@app.route('/api/date-range')
def get_date_range():
    """Get available date range"""
    deltas = db.get_deltas()
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
        # Generate graph
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
    last_status_check = datetime.now() - timedelta(hours=3)
    
    deltas = db.get_deltas()
    with scraper_lock:
        state = scraper_state
    
    return jsonify({
        'running': scraper_running,
        'state': state,  # idle, checking, scraping, sleeping
        'last_update': deltas['update time'].max().isoformat() if not deltas.empty else None,
        'last_check': last_status_check.isoformat()
    })


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


@app.route('/api/delta', methods=['GET'])
def get_deltas():
    """Get recent delta updates for polling"""
    try:
        limit = request.args.get('limit', 100, type=int)
        
        all_deltas = db.get_deltas()
        
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
                'prev_update_time': prev_update_time.isoformat()
            })
        
        return jsonify({'deltas': deltas})
    except Exception as e:
        log_console(f"Error getting deltas: {str(e)}", "ERROR")
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
        
        current_update = return_last_update()
        
        with scraper_lock:
            scraper_state = "scraping"
        
        rankings = get_ranking()[1]
        rankparsed = parse_to_db_formatted(rankings, current_update)
        db.update(rankparsed, current_update)
        db.save()
        
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
