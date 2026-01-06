"""
Web scraping functions for fetching player data from rubinothings.com.br
"""
import os
import gc
import requests
import pandas as pd
from bs4 import BeautifulSoup
import bs4
from datetime import datetime, timedelta
from fastapi_utils import log_console, parse_datetime, get_multiple, FORCE_PROXY, PROXY_LIST, TIMEZONE_OFFSET_HOURS


# Configuration
DEFAULT_WORLD = os.environ.get('DEFAULT_WORLD', 'Auroria')
DEFAULT_GUILD = os.environ.get('DEFAULT_GUILD', 'Ascended Auroria')


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
    gc.collect()
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
    if not FORCE_PROXY:
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed for '{name}' with status {response.status_code}, trying proxies...", "WARNING")
                # Build URL with params for proxy attempt
                url_with_params = f"{url}?name={name.replace(' ', '+')}"
                response = get_multiple(url_with_params, PROXY_LIST)
                log_console(f"Proxy fetch response for '{name}': {response}", "INFO")
        except Exception as e:
            log_console(f"Direct fetch failed for '{name}': {str(e)}, trying proxies...", "WARNING")
            url_with_params = f"{url}?name={name.replace(' ', '+')}"
            response = get_multiple(url_with_params, PROXY_LIST)
            log_console(f"Proxy fetch response for '{name}': {response}", "INFO")
    else:
        url_with_params = f"{url}?name={name.replace(' ', '+')}"
        response = get_multiple(url_with_params, PROXY_LIST)
        log_console(f"Proxy fetch response for '{name}': {response}", "INFO")

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
            
            # Free memory
            del soup, tables, tables_dict
        else:
            log_console(f"Request failed for '{name}' with status code: {result['response_status']}", "ERROR")
            
    except Exception as e:
        log_console(f"Error parsing player data for '{name}': {str(e)}", "ERROR")
    
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

    # Try direct fetch first
    if not FORCE_PROXY:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
                response = get_multiple(url, PROXY_LIST)
        except Exception as e:
            log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
            response = get_multiple(url, PROXY_LIST)
    else:
        response = get_multiple(url, PROXY_LIST)
    
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
    if not FORCE_PROXY:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                log_console(f"Direct fetch failed with status {response.status_code}, trying proxies...", "WARNING")
                response = get_multiple(url, PROXY_LIST)
        except Exception as e:
            log_console(f"Direct fetch failed: {str(e)}, trying proxies...", "WARNING")
            response = get_multiple(url, PROXY_LIST)
    else:
        response = get_multiple(url, PROXY_LIST)
    
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

    # Clean up
    del soup, r, split_tables, response
    gc.collect()
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
                            # Apply timezone offset before converting to ISO format
                            dt = pd.to_datetime(record['last update']) - timedelta(hours=TIMEZONE_OFFSET_HOURS)
                            record['last update'] = dt.isoformat()
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
