"""
FastAPI application for game ranking tracker.
Converted from Flask application.
"""
import os
import gc
import asyncio
import queue
import shutil
from datetime import datetime, timedelta
from typing import List, Optional
import pandas as pd
import plotly.graph_objects as go

from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates

# Import local modules
from fastapi_models import (
    GraphRequest, StatsRequest, RankingsTableRequest, VIPAddRequest,
    VIPRemoveRequest, VIPGraphRequest, ScrapingConfigRequest,
    ErrorResponse, SuccessResponse
)
from fastapi_database import Database
from fastapi_utils import log_console, console_queue, delta_queue, parse_online_time_to_minutes
from fastapi_scraper import (
    scrape_player_data, get_ranking, return_last_update, parse_to_db_formatted
)
from fastapi_vip import scrape_single_vip
from fastapi_analytics import (
    get_delta_between, create_interactive_graph, get_player_stats
)
from fastapi_background import start_scraper_thread, get_scraper_status, scraper_lock, scraper_state


# Configuration from environment variables
UPLOAD_PASSWORD = os.environ.get('UPLOAD_PASSWORD', 'Rollabostx1234')
DEFAULT_WORLD = os.environ.get('DEFAULT_WORLD', 'Auroria')
DEFAULT_GUILD = os.environ.get('DEFAULT_GUILD', 'Ascended Auroria')
DATA_FOLDER = os.environ.get('DATA_FOLDER', '/var/data')
TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))


# Configure aggressive garbage collection for memory efficiency
gc.set_threshold(700, 10, 5)
gc.enable()


# Create FastAPI app
app = FastAPI(
    title="Game Ranking Tracker API",
    description="API for tracking game player rankings and EXP gains",
    version="2.0.0"
)


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Add url_for function to Jinja2 environment for Flask compatibility
def url_for(name: str, **path_params):
    """Custom url_for function for Jinja2 templates (Flask compatibility)"""
    if name == "static":
        # Handle static files: url_for('static', filename='style.css')
        filename = path_params.get('filename', '')
        return f"/static/{filename}"
    else:
        # For other routes, return the path as-is
        return f"/{name}"

templates.env.globals['url_for'] = url_for


# Initialize database
db = Database(log_func=log_console)
db.load()


# Start background scraper
start_scraper_thread(db)


# Error Handlers
@app.exception_handler(400)
async def handle_bad_request(request: Request, exc: HTTPException):
    """Handle 400 Bad Request errors"""
    return JSONResponse(
        status_code=400,
        content={
            'error': 'Bad Request',
            'message': str(exc.detail),
            'status': 400
        }
    )


@app.exception_handler(404)
async def handle_not_found(request: Request, exc: HTTPException):
    """Handle 404 Not Found errors"""
    return JSONResponse(
        status_code=404,
        content={
            'error': 'Not Found',
            'message': str(exc.detail),
            'status': 404
        }
    )


@app.exception_handler(500)
async def handle_internal_error(request: Request, exc: Exception):
    """Handle 500 Internal Server errors"""
    error_msg = str(exc)
    log_console(f"Internal server error: {error_msg}", "ERROR")
    return JSONResponse(
        status_code=500,
        content={
            'error': 'Internal Server Error',
            'message': error_msg,
            'status': 500
        }
    )


@app.exception_handler(Exception)
async def handle_exception(request: Request, exc: Exception):
    """Handle all unhandled exceptions"""
    error_msg = str(exc)
    error_type = type(exc).__name__
    
    # Log the full traceback for debugging
    log_console(f"Unhandled exception ({error_type}): {error_msg}", "ERROR")
    
    # Return a clean JSON error response
    return JSONResponse(
        status_code=500,
        content={
            'error': error_type,
            'message': error_msg,
            'status': 500
        }
    )


# Routes - Main Page
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main page"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/vip", response_class=HTMLResponse)
async def vip_page(request: Request):
    """VIP tracking page"""
    return templates.TemplateResponse("vip.html", {"request": request})


# API Routes - Player Data
@app.get("/api/players")
async def get_players(world: Optional[str] = None, guild: Optional[str] = None):
    """Get list of all players"""
    deltas = db.get_deltas()
    
    # Filter by world and guild if specified
    if world:
        deltas = deltas[deltas['world'] == world]
    if guild:
        deltas = deltas[deltas['guild'] == guild]
    
    players = sorted(deltas['name'].unique().tolist())
    return players


@app.get("/api/date-range")
async def get_date_range(world: Optional[str] = None, guild: Optional[str] = None):
    """Get available date range"""
    deltas = db.get_deltas()
    
    # Filter by world and guild if specified
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
async def get_graph(request: GraphRequest):
    """Generate interactive graph with stats and comparison data"""
    names = request.names
    datetime1 = request.datetime1
    datetime2 = request.datetime2

    if not names:
        raise HTTPException(status_code=400, detail='No players selected')

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

        return {
            'graph': graph_json,
            'stats': stats,
            'comparison': comparison
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/stats")
async def get_stats(request: StatsRequest):
    """Get player statistics (deprecated - use /api/graph instead)"""
    names = request.names
    datetime1 = request.datetime1
    datetime2 = request.datetime2

    try:
        stats = get_player_stats(names, db, datetime1, datetime2)
        return {'stats': stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-players")
async def get_top_players(
    limit: int = Query(10),
    datetime1: Optional[str] = None,
    datetime2: Optional[str] = None
):
    """Get top players by total EXP"""
    table = db.get_deltas()

    if datetime1 and datetime2:
        table = get_delta_between(datetime1, datetime2, db)

    # Calculate top players
    top = table.groupby('name')['deltaexp'].sum().sort_values(ascending=False).head(limit)

    result = [{'name': name, 'total_exp': int(exp)} for name, exp in top.items()]
    return result


@app.get("/api/recent-updates")
async def get_recent_updates(limit: int = Query(20)):
    """Get recent EXP updates"""
    deltas = db.get_deltas()
    recent = deltas.sort_values('update time', ascending=False).head(limit)
    result = recent.to_dict('records')

    # Convert datetime to string
    for item in result:
        item['update time'] = item['update time'].isoformat()

    return result


@app.post("/api/rankings-table")
async def get_rankings_table(request: RankingsTableRequest):
    """Get grouped rankings table with filters and sorting"""
    datetime1 = request.datetime1
    datetime2 = request.datetime2

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

        # Use efficient aggregation
        grouped['sum'] = grouped['deltaexp'].apply(sum)
        grouped['number of updates'] = grouped['deltaexp'].str.len()
        grouped['avg'] = grouped['sum'] / grouped['number of updates']
        grouped['max'] = grouped['deltaexp'].apply(max)
        grouped['min'] = grouped['deltaexp'].apply(min)

        # Convert to records
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

        response = {'rankings': result}
        del table, grouped, result
        gc.collect()
        return response
    except Exception as e:
        log_console(f"Error in rankings table: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scraper-status")
async def api_scraper_status():
    """Get scraper status"""
    status = get_scraper_status()
    
    deltas = db.get_deltas()
    if not deltas.empty:
        status['last_update'] = deltas['update time'].max().isoformat()
    else:
        status['last_update'] = None
    
    return status


# File Download/Upload Routes
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
async def upload_deltas(password: str = Form(...), file: UploadFile = File(...)):
    """Upload deltas.csv file"""
    try:
        # Check password
        if password != UPLOAD_PASSWORD:
            raise HTTPException(status_code=401, detail='Invalid password')
        
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail='Only CSV files are allowed')
        
        # Read and validate the CSV
        contents = await file.read()
        from io import StringIO
        df = pd.read_csv(
            StringIO(contents.decode('utf-8')),
            dtype={'name': str, 'deltaexp': 'int64', 'world': str, 'guild': str},
            parse_dates=['update time']
        )
        
        required_columns = ['name', 'deltaexp', 'update time']
        if not all(col in df.columns for col in required_columns):
            raise HTTPException(status_code=400, detail=f'CSV must have columns: {required_columns}')
        
        # Backup existing file
        if os.path.exists(db.deltas_file):
            backup_file = db.deltas_file.replace('.csv', '_backup.csv')
            shutil.copy(db.deltas_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")
        
        # Save the uploaded file
        records_count = len(df)
        df.to_csv(db.deltas_file, index=False)
        log_console(f"Uploaded deltas.csv with {records_count} records", "SUCCESS")
        
        del df
        gc.collect()
        return {'success': True, 'records': records_count}
    except Exception as e:
        log_console(f"Error uploading deltas.csv: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/upload/exps")
async def upload_exps(password: str = Form(...), file: UploadFile = File(...)):
    """Upload exps.csv file"""
    try:
        # Check password
        if password != UPLOAD_PASSWORD:
            raise HTTPException(status_code=401, detail='Invalid password')
        
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail='Only CSV files are allowed')
        
        # Read and validate the CSV
        contents = await file.read()
        from io import StringIO
        df = pd.read_csv(
            StringIO(contents.decode('utf-8')),
            dtype={'name': str, 'exp': 'int64', 'world': str, 'guild': str},
            parse_dates=['last update']
        )
        
        required_columns = ['name', 'exp', 'last update']
        if not all(col in df.columns for col in required_columns):
            raise HTTPException(status_code=400, detail=f'CSV must have columns: {required_columns}')
        
        # Backup existing file
        if os.path.exists(db.exps_file):
            backup_file = db.exps_file.replace('.csv', '_backup.csv')
            shutil.copy(db.exps_file, backup_file)
            log_console(f"Created backup: {backup_file}", "INFO")
        
        # Save the uploaded file
        records_count = len(df)
        df.to_csv(db.exps_file, index=False)
        log_console(f"Uploaded exps.csv with {records_count} records", "SUCCESS")
        
        del df
        gc.collect()
        return {'success': True, 'records': records_count}
    except Exception as e:
        log_console(f"Error uploading exps.csv: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# Player-specific Routes
@app.get("/api/player-graph/{player_name}")
async def get_player_graph(
    player_name: str,
    datetime1: Optional[str] = None,
    datetime2: Optional[str] = None
):
    """Get individual player graph data"""
    try:
        graph_json = create_interactive_graph(player_name, db, datetime1, datetime2)
        return {'graph': graph_json, 'player': player_name}
    except Exception as e:
        log_console(f"Error generating player graph for {player_name}: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/player-details/{player_name}")
async def get_player_details(player_name: str):
    """Get detailed player data from rubinothings.com.br and process VIP deltas if applicable"""
    try:
        player_data = scrape_player_data(player_name)
        
        # If scraping was successful and player is a VIP, update VIP data and calculate deltas
        if player_data.get('success') and player_data.get('tables'):
            vips = db.get_vips()
            matching_vip = next((v for v in vips if v['name'] == player_name), None)
            
            if matching_vip:
                # This is a VIP - process their data
                world = matching_vip['world']
                today_exp = 0
                today_online = 0
                
                for table in player_data['tables']:
                    columns = table['columns']
                    data = table['data']
                    
                    # Check for Raw XP table
                    if 'Raw XP no dia' in columns and data:
                        idx = columns.index('Raw XP no dia')
                        raw_value = data[0][idx] if len(data[0]) > idx else "0"
                        today_exp = int(raw_value.replace(',', '').replace('.', ''))
                    
                    # Check for Online time table
                    if 'Online time' in columns and data:
                        idx = columns.index('Online time')
                        online_time_str = data[0][idx] if len(data[0]) > idx else "0:00"
                        today_online = parse_online_time_to_minutes(online_time_str)
                
                # Get OLD values from vipsdata BEFORE updating
                vipsdata = db.get_vipsdata()
                existing_vip = vipsdata[(vipsdata['name'] == player_name) & (vipsdata['world'] == world)]
                
                if not existing_vip.empty:
                    # VIP exists - calculate delta from previous cumulative
                    old_exp = existing_vip['today_exp'].values[0]
                    old_online = existing_vip['today_online'].values[0]
                    delta_exp = today_exp - old_exp
                    delta_online = today_online - old_online
                    
                    # Only process if exp has changed
                    if delta_exp != 0:
                        now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                        today_date = now.strftime("%Y-%m-%d")
                        db.add_vip_delta(player_name, world, today_date, delta_exp, delta_online, now)
                        log_console(f"VIP delta processed via player-details: {player_name} +{delta_exp} exp, +{delta_online} online", "INFO")
                        
                        # Update VIP data with NEW values
                        db.update_vipdata(player_name, world, today_exp, today_online)
        
        return player_data
    except Exception as e:
        log_console(f"Error getting player details for {player_name}: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/delta")
async def get_deltas(
    limit: int = Query(100),
    world: Optional[str] = None,
    guild: Optional[str] = None
):
    """Get recent delta updates for polling"""
    try:
        all_deltas = db.get_deltas()
        
        if all_deltas.empty:
            return {'deltas': []}
        
        # Filter by world and guild if specified
        if world:
            all_deltas = all_deltas[all_deltas['world'] == world]
        if guild:
            all_deltas = all_deltas[all_deltas['guild'] == guild]
        
        if all_deltas.empty:
            return {'deltas': []}
        
        recent_deltas = all_deltas.sort_values(['update time', 'name'], ascending=[False, True]).head(limit)

        # Get distinct update times for efficient lookup
        distinct_times_list = sorted(all_deltas['update time'].unique())

        # Create a mapping of update_time -> prev_update_time
        prev_time_map = {}
        for i, current_time in enumerate(distinct_times_list):
            if i > 0:
                prev_time_map[current_time] = distinct_times_list[i - 1]
            else:
                prev_time_map[current_time] = current_time

        # Build delta list with calculated previous update times
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
        
        response = {'deltas': deltas}
        del all_deltas, recent_deltas, distinct_times_list, prev_time_map, deltas
        gc.collect()
        return response
    except Exception as e:
        log_console(f"Error getting deltas: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# Status and Configuration Routes
@app.get("/api/status-data")
async def get_status_data():
    """Get the stored status data from all worlds"""
    try:
        status_data = db.get_status_data()
        if status_data:
            return status_data
        else:
            raise HTTPException(
                status_code=404,
                detail='No status data available yet. Status data will be available after the first update'
            )
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error getting status data: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scraping-config")
async def get_scraping_config():
    """Get the scraping configuration"""
    try:
        config = db.get_scraping_config()
        return config
    except Exception as e:
        log_console(f"Error getting scraping config: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraping-config")
async def update_scraping_config(request: ScrapingConfigRequest):
    """Update the scraping configuration"""
    try:
        # Check password
        if request.password != UPLOAD_PASSWORD:
            raise HTTPException(status_code=401, detail='Invalid password')
        
        config = [item.dict() for item in request.config]
        
        # Save the configuration
        db.save_scraping_config(config)
        log_console(f"Scraping configuration updated via API", "SUCCESS")
        
        return {'success': True, 'config': config}
    except HTTPException:
        raise
    except Exception as e:
        log_console(f"Error updating scraping config: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/manual-update")
async def manual_update():
    """Manually trigger a ranking update"""
    global scraper_state
    
    # Check if scraper is already active
    with scraper_lock:
        current_state = scraper_state
    
    if current_state in ['checking', 'scraping']:
        raise HTTPException(
            status_code=409,
            detail=f'Scraper is already running (state: {current_state})'
        )
    
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
            db.update(combined_df, current_update, delta_queue=delta_queue)
            db.save()
            log_console(f"Manual update: {len(combined_df)} total players", "SUCCESS")
        else:
            raise Exception("No player data collected from any world/guild")
        
        with scraper_lock:
            scraper_state = "idle"
        
        log_console(f"Manual update completed successfully at {current_update}", "SUCCESS")
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


# VIP Routes
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
async def add_vip(request: VIPAddRequest):
    """Add a VIP player and immediately scrape their data"""
    try:
        success = db.add_vip(request.name, request.world)
        if success:
            # Immediately scrape the new VIP's data
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
async def remove_vip(request: VIPRemoveRequest):
    """Remove a VIP player"""
    try:
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
async def get_vip_deltas(
    limit: int = Query(100),
    name: Optional[str] = None,
    world: Optional[str] = None
):
    """Get VIP delta history for live feed"""
    try:
        deltavip = db.get_deltavip()
        
        if deltavip.empty:
            return {'deltas': []}
        
        # Filter by name and world if provided
        if name:
            deltavip = deltavip[deltavip['name'] == name]
        if world:
            deltavip = deltavip[deltavip['world'] == world]
        
        if deltavip.empty:
            return {'deltas': []}
        
        # Sort by update time descending and limit
        recent_deltas = deltavip.sort_values('update_time', ascending=False).head(limit)
        
        # Get distinct update times for efficient lookup
        distinct_times_list = sorted(deltavip['update_time'].unique())
        
        # Create a mapping of update_time -> prev_update_time
        prev_time_map = {}
        for i, current_time in enumerate(distinct_times_list):
            if i > 0:
                prev_time_map[current_time] = distinct_times_list[i - 1]
            else:
                prev_time_map[current_time] = current_time
        
        # Build delta list with calculated previous update times using itertuples
        deltas = []
        for row in recent_deltas.itertuples(index=False):
            # Access columns by position
            current_time = row[5]  # update_time is 6th column
            prev_update_time = prev_time_map.get(current_time, current_time)
            
            deltas.append({
                'name': row.name,
                'world': row.world,
                'delta_exp': int(row.delta_exp),
                'delta_online': int(row.delta_online),
                'update_time': current_time.isoformat(),
                'prev_update_time': prev_update_time.isoformat(),
                'date': row.date
            })
        
        response = {'deltas': deltas}
        del deltavip, recent_deltas, distinct_times_list, prev_time_map, deltas
        gc.collect()
        return response
    except Exception as e:
        log_console(f"Error getting VIP deltas: {str(e)}", "ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/vip/graph")
async def get_vip_graph(request: VIPGraphRequest):
    """Generate combined VIP graph with exp (bars) and online time (line)"""
    try:
        deltavip = db.get_deltavip()
        vip_data = deltavip[(deltavip['name'] == request.name) & (deltavip['world'] == request.world)]
        
        if vip_data.empty:
            raise HTTPException(status_code=404, detail='No data available for this VIP')
        
        # Sort by update time
        vip_data = vip_data.sort_values('update_time')
        
        # Apply zero grouping logic (compress consecutive zeros)
        all_update_times = vip_data['update_time'].tolist()
        exp_values = vip_data['delta_exp'].tolist()
        online_values = vip_data['delta_online'].tolist()
        
        # Identify positions where BOTH exp AND online are zero
        all_zero_positions = []
        for i in range(len(exp_values)):
            if exp_values[i] == 0 and online_values[i] == 0:
                all_zero_positions.append(i)
        
        # Group consecutive zero positions
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
        
        # Build compressed data
        time_labels = []
        compressed_exp = []
        compressed_online = []
        compressed_online_display = []
        time_diffs = []
        
        prev_date = None
        prev_time = None
        prev_timestamp = None
        i = 0
        while i < len(all_update_times):
            # Check if this position starts a zero group
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
                        if start_date != prev_date:
                            label = f"{start_time.strftime('%d/%m/%Y %H:%M')}->{end_time.strftime('%H:%M')}"
                        else:
                            label = f"{start_time.strftime('%H:%M')}->{end_time.strftime('%H:%M')}"
                    else:
                        if start_date != prev_date:
                            label = f"{start_time.strftime('%d/%m/%Y %H:%M')}->{end_time.strftime('%d/%m/%Y %H:%M')}"
                        else:
                            label = f"{start_time.strftime('%H:%M')}->{end_time.strftime('%d/%m/%Y %H:%M')}"
                    
                    time_labels.append(label)
                    compressed_exp.append(0)
                    compressed_online.append(0)
                    compressed_online_display.append("0 / 0 min")
                    time_diffs.append(0)
                    prev_date = end_date
                    prev_time = end_time
                    prev_timestamp = end_time
                    
                    i = end + 1
                    in_zero_group = True
                    break
            
            if not in_zero_group:
                time_obj = pd.to_datetime(all_update_times[i])
                current_date = time_obj.date()
                
                # Calculate time difference
                if prev_timestamp is not None:
                    time_diff_minutes = int((time_obj - prev_timestamp).total_seconds() / 60)
                else:
                    time_diff_minutes = 0
                
                if prev_time is None:
                    if current_date != prev_date:
                        time_str = time_obj.strftime('%d/%m/%Y %H:%M')
                    else:
                        time_str = time_obj.strftime('%H:%M')
                else:
                    start_date = prev_time.date()
                    if start_date == current_date:
                        if current_date != prev_date:
                            time_str = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%H:%M')}"
                        else:
                            time_str = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%H:%M')}"
                    else:
                        if start_date != prev_date:
                            time_str = f"{prev_time.strftime('%d/%m/%Y %H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
                        else:
                            time_str = f"{prev_time.strftime('%H:%M')}-{time_obj.strftime('%d/%m/%Y %H:%M')}"
                
                time_labels.append(time_str)
                compressed_exp.append(exp_values[i])
                
                # If online time is 0 but exp is not 0, use None for interpolation
                online_val = online_values[i]
                if online_val == 0 and exp_values[i] > 0:
                    compressed_online.append(None)
                else:
                    compressed_online.append(online_val)
                
                display_label = f"{online_val} / {time_diff_minutes} min"
                compressed_online_display.append(display_label)
                time_diffs.append(time_diff_minutes)
                
                prev_date = current_date
                prev_time = time_obj
                prev_timestamp = time_obj
                i += 1
        
        # Create combined graph
        fig = go.Figure()
        
        # Add EXP bars
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
        
        # Add smooth EXP trend line
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
        
        # Add Online Time line (right y-axis)
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
        
        # Update layout with dual y-axes
        fig.update_layout(
            title=f'🌟 {request.name} - VIP Stats ({request.world})',
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


# SSE Stream for console logs
@app.get("/api/console-stream")
async def console_stream():
    """Server-Sent Events stream for console logs"""
    async def generate():
        # Send initial connection message
        yield f"data: [CONNECTED] Console stream started\n\n"

        while True:
            try:
                # Try to get log from queue
                try:
                    log = console_queue.get(timeout=1)
                    yield f"data: {log}\n\n"
                except queue.Empty:
                    # Send keepalive
                    yield f": keepalive\n\n"
                
                # Small delay to prevent overwhelming the client
                await asyncio.sleep(0.1)
            except Exception as e:
                log_console(f"Error in console stream: {str(e)}", "ERROR")
                break

    return StreamingResponse(generate(), media_type="text/event-stream")


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# Main entry point
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
