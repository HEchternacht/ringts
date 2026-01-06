"""
VIP tracking functions for monitoring specific players.
"""
from datetime import datetime, timedelta
from fastapi_utils import log_console, parse_online_time_to_minutes, TIMEZONE_OFFSET_HOURS
from fastapi_scraper import scrape_player_data


def scrape_single_vip(database, name, world):
    """Scrape a single VIP player and update their data"""
    try:
        result = scrape_player_data(name)
        if result['success'] and result['tables']:
            # Find the Raw XP table (table with "Raw XP no dia" column)
            today_exp = 0
            today_online = 0  # minutes
            
            for table in result['tables']:
                columns = table['columns']
                data = table['data']
                
                # Check for Raw XP table
                if 'Raw XP no dia' in columns and data:
                    idx = columns.index('Raw XP no dia')
                    # Get the first row's value
                    raw_value = data[0][idx] if len(data[0]) > idx else "0"
                    # Remove formatting and convert
                    today_exp = int(raw_value.replace(',', '').replace('.', ''))
                
                # Check for Online time table
                if 'Online time' in columns and data:
                    idx = columns.index('Online time')
                    online_time_str = data[0][idx] if len(data[0]) > idx else "0:00"
                    # Parse to minutes
                    today_online = parse_online_time_to_minutes(online_time_str)
            
            # Get OLD values from vipsdata BEFORE updating
            vipsdata = database.get_vipsdata()
            existing_vip = vipsdata[(vipsdata['name'] == name) & (vipsdata['world'] == world)]
            
            if not existing_vip.empty:
                # VIP exists - calculate delta from previous cumulative
                old_exp = existing_vip['today_exp'].values[0]
                old_online = existing_vip['today_online'].values[0]
                delta_exp = today_exp - old_exp
                delta_online = today_online - old_online
                
                # Only process if exp has changed
                if delta_exp != 0:
                    # Save delta (including online time change)
                    now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                    today_date = now.strftime("%Y-%m-%d")
                    database.add_vip_delta(name, world, today_date, delta_exp, delta_online, now)
                    
                    # Update VIP data with NEW values
                    database.update_vipdata(name, world, today_exp, today_online)
                    log_console(f"VIP {name} ({world}): {today_exp} exp, {today_online} min online", "INFO")
                else:
                    # Exp hasn't changed, skip update
                    log_console(f"VIP {name} ({world}): No exp change, skipping update", "INFO")
            else:
                # First time tracking - create initial baseline with 0 delta
                now = datetime.now() - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                today_date = now.strftime("%Y-%m-%d")
                database.add_vip_delta(name, world, today_date, 0, 0, now)
                database.update_vipdata(name, world, today_exp, today_online)
                log_console(f"VIP delta: {name} ({world}) +0 exp, +0 online (initial baseline)", "INFO")
                log_console(f"VIP {name} ({world}): {today_exp} exp, {today_online} min online", "INFO")
            
            return True
        else:
            log_console(f"Failed to scrape VIP {name} ({world})", "WARNING")
            return False
    except Exception as e:
        log_console(f"Error scraping VIP {name} ({world}): {str(e)}", "ERROR")
        return False


def scrape_vip_data(database, world):
    """Scrape VIP player data for a specific world and update today's stats"""
    vips = database.get_vips()
    if not vips:
        return
    
    # Filter VIPs for this world
    world_vips = [v for v in vips if v['world'] == world]
    if not world_vips:
        return
    
    log_console(f"Scraping {len(world_vips)} VIP players for {world}...", "INFO")
    for vip in world_vips:
        scrape_single_vip(database, vip['name'], vip['world'])


def process_vip_deltas(database, world, update_time):
    """Process VIP data after world update - scraping handles delta calculation"""
    # Simply trigger a scrape for this world's VIPs
    scrape_vip_data(database, world)
