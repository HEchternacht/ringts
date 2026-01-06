"""
Background task functions for scraping rankings.
"""
import time
import multiprocessing
import traceback
import gc
from datetime import datetime
import pandas as pd
from fastapi_utils import log_console
from fastapi_scraper import get_last_status_updates, get_ranking, parse_to_db_formatted
from fastapi_vip import scrape_vip_data


# Scraper state - using Manager for cross-process shared state
_manager = None
_scraper_process = None
scraper_state_dict = None  # Will be initialized with Manager.dict()


def _get_manager():
    """Get or create the multiprocessing manager"""
    global _manager, scraper_state_dict
    if _manager is None:
        _manager = multiprocessing.Manager()
        scraper_state_dict = _manager.dict()
        scraper_state_dict['running'] = False
        scraper_state_dict['state'] = 'idle'
        scraper_state_dict['last_status_check'] = None
    return _manager


def loop_get_rankings(database, debug=False):
    """Background loop to continuously fetch rankings from all configured worlds and guilds"""
    database.load()
    
    _get_manager()  # Initialize manager in this process
    scraper_state_dict['running'] = True
    
    # Track last update per world (worlds update independently)
    last_updates = {}
    
    # Get scraping configuration
    scraping_config = database.get_scraping_config()
    log_console(f"Starting ranking scraper for {len(scraping_config)} world(s)")
    
    ignore_updates = []
    while scraper_state_dict['running']:
        try:
            scraper_state_dict['state'] = "checking"
            
            # Get status data for all worlds to check what's available
            all_status = get_last_status_updates()
            
            if not all_status:
                log_console("Failed to get status data, retrying...", "WARNING")
                scraper_state_dict['state'] = "sleeping"
                time.sleep(60)
                continue
            
            # Save all status data to JSON
            from datetime import timedelta
            import os
            TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
            
            json_data = {
                "fetch_time": datetime.now().isoformat(),
                "worlds": {}
            }
            
            for world_name, df in all_status.items():
                if not df.empty and 'last update' in df.columns:
                    world_data = df.to_dict('records')
                    for record in world_data:
                        if 'last update' in record and pd.notna(record['last update']):
                            # Apply timezone offset before converting to ISO format
                            dt = pd.to_datetime(record['last update']) - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                            record['last update'] = dt.isoformat()
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
                scraper_state_dict['state'] = "sleeping"
                time.sleep(60)
            else:
                # Process EACH WORLD separately with its own timestamp
                scraper_state_dict['state'] = "scraping"
                
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
                        from fastapi_utils import delta_queue
                        database.update(combined_df, update_time, delta_queue=delta_queue)
                        database.save()
                        log_console(f"Updated {len(combined_df)} players for {world} at {update_time}", "SUCCESS")
                        
                        # Free memory after database update
                        del combined_df
                        gc.collect()
                        
                        # Scrape VIP data for THIS specific world
                        scrape_vip_data(database, world)
                        
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
                
                scraper_state_dict['state'] = "idle"
        except Exception as e:
            log_console(f"Error in scraper: {str(e)}", "ERROR")
            traceback.print_exc()
            scraper_state_dict['state'] = "sleeping"
            time.sleep(10)  # Wait before retry


def _scraper_worker(database_config):
    """Worker function to run in separate process"""
    # Recreate database instance in this process
    from fastapi_database import Database
    database = Database(**database_config)
    
    def run_with_restart():
        while True:
            try:
                log_console("Starting scraper process...")
                loop_get_rankings(database, debug=True)
            except Exception as e:
                log_console(f"Scraper crashed: {str(e)}. Restarting in 1s...", "ERROR")
                time.sleep(1)
    
    run_with_restart()


def start_scraper_process(database):
    """Start the scraper in a background process with auto-restart"""
    global _scraper_process
    
    _get_manager()  # Initialize manager
    
    # Stop existing process if running
    if _scraper_process is not None and _scraper_process.is_alive():
        log_console("Stopping existing scraper process...")
        scraper_state_dict['running'] = False
        _scraper_process.terminate()
        _scraper_process.join(timeout=5)
        if _scraper_process.is_alive():
            _scraper_process.kill()
            _scraper_process.join()
    
    # Extract database configuration to pass to subprocess
    database_config = {
        'data_folder': database.data_folder,
        'timezone_offset_hours': database.timezone_offset_hours,
        'daily_reset_hour': database.daily_reset_hour,
        'daily_reset_minute': database.daily_reset_minute
    }
    
    # Start new process
    _scraper_process = multiprocessing.Process(
        target=_scraper_worker,
        args=(database_config,),
        daemon=True
    )
    _scraper_process.start()
    log_console(f"Scraper process started with PID {_scraper_process.pid}")


def stop_scraper_process():
    """Stop the scraper process gracefully"""
    global _scraper_process
    
    if _scraper_process is None:
        return
    
    _get_manager()
    scraper_state_dict['running'] = False
    
    if _scraper_process.is_alive():
        log_console("Stopping scraper process...")
        _scraper_process.terminate()
        _scraper_process.join(timeout=5)
        
        if _scraper_process.is_alive():
            log_console("Force killing scraper process...")
            _scraper_process.kill()
            _scraper_process.join()
        
        log_console("Scraper process stopped")
    
    _scraper_process = None


def get_scraper_status():
    """Get current scraper status"""
    from datetime import timedelta
    import os
    TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
    
    _get_manager()
    
    last_check = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
    scraper_state_dict['last_status_check'] = last_check.isoformat()
    
    # Check if process is actually alive
    global _scraper_process
    is_alive = _scraper_process is not None and _scraper_process.is_alive()
    
    return {
        'running': scraper_state_dict.get('running', False) and is_alive,
        'state': scraper_state_dict.get('state', 'idle'),
        'last_check': scraper_state_dict.get('last_status_check', last_check.isoformat()),
        'process_alive': is_alive,
        'pid': _scraper_process.pid if is_alive else None
    }
