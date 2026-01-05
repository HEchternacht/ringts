"""
Data parsing utilities - simple and reliable.
"""
import re
import pandas as pd
from datetime import datetime, timedelta


def parse_online_time(time_str):
    """Parse online time string like '3h 10m' to minutes"""
    if pd.isna(time_str) or not time_str:
        return 0
    
    time_str = str(time_str).strip().lower()
    hours = minutes = 0
    
    # Extract hours
    hour_match = re.search(r'(\d+)\s*h', time_str)
    if hour_match:
        hours = int(hour_match.group(1))
    
    # Extract minutes
    min_match = re.search(r'(\d+)\s*m', time_str)
    if min_match:
        minutes = int(min_match.group(1))
    
    return hours * 60 + minutes


def parse_experience_number(xp_str):
    """Parse experience string like '176.495.455' to integer"""
    if pd.isna(xp_str) or not xp_str:
        return 0
    
    # Remove dots and convert
    xp_str = str(xp_str).strip().replace('.', '').replace(',', '')
    try:
        return int(xp_str)
    except ValueError:
        return 0


def parse_level_delta(delta_str):
    """Parse level delta like '+2' or '-1' to integer"""
    if pd.isna(delta_str) or not delta_str:
        return 0
    
    delta_str = str(delta_str).strip().replace('+', '')
    try:
        return int(delta_str)
    except ValueError:
        return 0


def parse_datetime(date_str):
    """Parse date string in DD/MM/YYYY or DD/MM/YYYY HH:MM format"""
    if pd.isna(date_str) or not date_str:
        return datetime.now()
    
    date_str = str(date_str).strip()
    
    # Try formats
    for fmt in ['%d/%m/%Y %H:%M', '%d/%m/%Y']:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return datetime.now()


def parse_portuguese_time(time_str):
    """Parse Portuguese time strings like 'Hoje às 09:17' or 'Ontem às 14:23'"""
    if pd.isna(time_str) or not time_str:
        return datetime.now()
    
    time_str = str(time_str).strip().lower()
    now = datetime.now()
    
    # Extract time part (HH:MM)
    time_match = re.search(r'(\d{1,2}):(\d{2})', time_str)
    if not time_match:
        return now
    
    hour = int(time_match.group(1))
    minute = int(time_match.group(2))
    
    # Determine date based on keywords
    if 'hoje' in time_str or 'today' in time_str:
        base_date = now.date()
    elif 'ontem' in time_str or 'yesterday' in time_str:
        base_date = (now - timedelta(days=1)).date()
    elif 'anteontem' in time_str:
        base_date = (now - timedelta(days=2)).date()
    else:
        base_date = now.date()
    
    return datetime.combine(base_date, datetime.min.time().replace(hour=hour, minute=minute))


def normalize_column_names(df):
    """Normalize dataframe column names for consistent parsing"""
    if df.empty:
        return df
    
    # Common column mappings
    mappings = {
        'Data': 'Date',
        'Online time': 'Online Time',
        'Raw XP no dia': 'Raw XP Day',
        'Δ Level': 'Level Delta',
        'Time': 'Kill Time',
        'Victim': 'Victim Name',
        'Victim Level': 'Victim Level',
        'Killed by': 'Killed By',
        'World': 'World',
        'Level': 'Level'
    }
    
    # Rename columns
    df = df.rename(columns=mappings)
    return df
