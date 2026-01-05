"""
Web scraping functions for Tibia character data.
"""
import requests
import pandas as pd
from bs4 import BeautifulSoup


def extract_tables(soup):
    """Extract all tables from BeautifulSoup object"""
    dataframes = []
    
    for table in soup.find_all('table'):
        headers = []
        rows = []
        
        # Extract headers
        header_elements = table.find_all('th')
        if header_elements:
            headers = [h.get_text(strip=True) for h in header_elements]
        
        # Extract rows
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if cells:
                row_data = [cell.get_text(strip=True) for cell in cells]
                if row_data != headers:
                    rows.append(row_data)
        
        # Create DataFrame
        if rows:
            if headers and len(headers) == len(rows[0]):
                df = pd.DataFrame(rows, columns=headers)
            else:
                df = pd.DataFrame(rows)
            dataframes.append(df)
    
    return dataframes


def scrape_character(character_name):
    """Scrape character data from rubinothings.com.br"""
    try:
        response = requests.get("https://rubinothings.com.br/player", 
                              params={"name": character_name}, 
                              timeout=30)
        
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = extract_tables(soup)
        
        return tables if tables else None
        
    except Exception as e:
        print(f"❌ Error scraping {character_name}: {e}")
        return None


def scrape_status():
    """Scrape status updates from rubinothings.com.br/status"""
    try:
        response = requests.get("https://rubinothings.com.br/status", timeout=30)
        
        if response.status_code != 200:
            return {}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = extract_tables(soup)
        
        # Split tables by empty rows
        split_tables = []
        for df in tables:
            mask = df.apply(lambda row: all(str(x).strip() == '' or pd.isna(x) for x in row), axis=1)
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
        
        # Organize by world name
        status_dict = {}
        for table in split_tables:
            if not table.empty:
                world_name = str(table.iloc[0, 0]).strip()
                if "Status" in world_name:
                    world_name = world_name.split("Status")[0].strip()
                
                # Clean data
                new_table = table.iloc[1:].reset_index(drop=True)
                if not new_table.empty and new_table.shape[1] >= 4:
                    new_table.columns = ["rotina", "last_update", "time_outdated", "status"]
                    status_dict[world_name] = new_table
        
        return status_dict
        
    except Exception as e:
        print(f"❌ Error scraping status: {e}")
        return {}
