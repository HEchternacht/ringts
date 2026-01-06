"""
Background task functions for scraping rankings.
"""
import time
import threading
import traceback
import gc
from datetime import datetime
import pandas as pd
from fastapi_utils import log_console
from fastapi_scraper import get_last_status_updates, get_ranking, parse_to_db_formatted
from fastapi_vip import scrape_vip_data


# Scraper state
scraper_running = False
scraper_state = "idle"  # idle, checking, scraping, sleeping
scraper_lock = threading.Lock()
last_status_check = None


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
    
    ignore_updates = []
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


def get_scraper_status():
    """Get current scraper status"""
    global last_status_check, scraper_running, scraper_state
    
    from datetime import timedelta
    import os
    TIMEZONE_OFFSET_HOURS = int(os.environ.get('TIMEZONE_OFFSET_HOURS', '3'))
    
    last_status_check = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
    
    with scraper_lock:
        state = scraper_state
    
    return {
        'running': scraper_running,
        'state': state,
        'last_check': last_status_check.isoformat()
    }
