import pandas as pd
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from alchemy import (
    Character, ScrapingSession, CharacterDeath, CharacterKill,
    CharacterOnlineTime, CharacterExperience, CharacterDeltaExperience, 
    CharacterDeltaOnline, DatabaseManager
)

def parse_online_time(time_str: str) -> int:
    """
    Parse online time string like "3h 10m" to total minutes
    
    Args:
        time_str (str): Time string in format like "3h 10m", "2h", "45m"
    
    Returns:
        int: Total minutes
    """
    if not time_str or pd.isna(time_str):
        return 0
    
    total_minutes = 0
    
    # Find hours
    hours_match = re.search(r'(\d+)h', str(time_str))
    if hours_match:
        total_minutes += int(hours_match.group(1)) * 60
    
    # Find minutes
    minutes_match = re.search(r'(\d+)m', str(time_str))
    if minutes_match:
        total_minutes += int(minutes_match.group(1))
    
    return total_minutes

def parse_experience_number(xp_str: str) -> int:
    """
    Parse experience string like "176.495.455" to integer
    
    Args:
        xp_str (str): Experience string with dots as thousands separator
    
    Returns:
        int: Experience as integer
    """
    if not xp_str or pd.isna(xp_str):
        return 0
    
    # Remove dots and convert to integer
    clean_str = str(xp_str).replace('.', '').replace(',', '')
    
    try:
        return int(clean_str)
    except ValueError:
        return 0

def parse_level_delta(delta_str: str) -> int:
    """
    Parse level delta string like "+2", "-1" to integer
    
    Args:
        delta_str (str): Level delta string
    
    Returns:
        int: Level change as integer
    """
    if not delta_str or pd.isna(delta_str):
        return 0
    
    clean_str = str(delta_str).replace('+', '')
    
    try:
        return int(clean_str)
    except ValueError:
        return 0

def parse_portuguese_time(time_str: str) -> datetime:
    """
    Parse Portuguese time strings like "hoje às 13:23", "ontem às 09:17"
    to datetime objects
    
    Args:
        time_str (str): Portuguese time string
    
    Returns:
        datetime: Parsed datetime object
    """
    if not time_str or pd.isna(time_str):
        return datetime.now()
    
    import re
    from datetime import date, timedelta
    
    time_str = str(time_str).lower().strip()
    
    # Extract time part (HH:MM)
    time_match = re.search(r'(\d{1,2}):(\d{2})', time_str)
    if not time_match:
        return datetime.now()
    
    hour = int(time_match.group(1))
    minute = int(time_match.group(2))
    
    # Determine the date
    today = date.today()
    
    if 'hoje' in time_str or 'today' in time_str:
        target_date = today
    elif 'ontem' in time_str or 'yesterday' in time_str:
        target_date = today - timedelta(days=1)
    elif 'anteontem' in time_str:
        target_date = today - timedelta(days=2)
    else:
        # Try to extract specific date patterns
        date_match = re.search(r'(\d{1,2})/(\d{1,2})', time_str)
        if date_match:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            year = today.year
            try:
                target_date = date(year, month, day)
            except ValueError:
                target_date = today
        else:
            target_date = today
    
    try:
        return datetime.combine(target_date, datetime.min.time().replace(hour=hour, minute=minute))
    except ValueError:
        return datetime.now()

def parse_datetime(date_str: str) -> datetime:
    """
    Parse date string in DD/MM/YYYY format or DD/MM/YYYY HH:MM format
    
    Args:
        date_str (str): Date string
    
    Returns:
        datetime: Parsed datetime object
    """
    if not date_str or pd.isna(date_str):
        return datetime.now()
    
    # Try different date formats
    formats = [
        "%d/%m/%Y %H:%M",  # 23/12/2025 19:24
        "%d/%m/%Y",        # 29/12/2025
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except ValueError:
            continue
    
    # If no format matches, return current time
    print(f"Warning: Could not parse date '{date_str}', using current time")
    return datetime.now()

def create_scraping_session(db_manager: DatabaseManager, world: str, status_data: Dict) -> int:
    """
    Create a new scraping session record
    
    Args:
        db_manager (DatabaseManager): Database manager
        world (str): World name
        status_data (Dict): Status data from the scraping endpoint
    
    Returns:
        int: Created scraping session ID
    """
    session = db_manager.get_session()
    
    try:
        # Extract status information for the world
        world_status = status_data.get(world, pd.DataFrame())
        
        # Initialize default values
        death_kills_data = {"last update": "", "time_outdated": "", "status": ""}
        online_data = {"last update": "", "time_outdated": "", "status": ""}
        ranking_data = {"last update": "", "time_outdated": "", "status": ""}
        
        if not world_status.empty:
            # Find relevant rows
            for _, row in world_status.iterrows():
                rotina = str(row.get('rotina', '')).lower()
                if 'deathskills' in rotina:
                    death_kills_data = {
                        "last update": row.get('last update', ''),
                        "time_outdated": row.get('time_outdated', ''),
                        "status": row.get('status', '')
                    }
                elif 'online' in rotina:
                    online_data = {
                        "last update": row.get('last update', ''),
                        "time_outdated": row.get('time_outdated', ''),
                        "status": row.get('status', '')
                    }
                elif 'ranking' in rotina:
                    ranking_data = {
                        "last update": row.get('last update', ''),
                        "time_outdated": row.get('time_outdated', ''),
                        "status": row.get('status', '')
                    }
        
        scraping_session = ScrapingSession(
            world=world,
            last_update_deaths_kills=death_kills_data["last update"],
            time_outdated_deaths_kills=death_kills_data["time_outdated"],
            status_deaths_kills=death_kills_data["status"],
            last_update_online=online_data["last update"],
            time_outdated_online=online_data["time_outdated"],
            status_online=online_data["status"],
            last_update_ranking=ranking_data["last update"],
            time_outdated_ranking=ranking_data["time_outdated"],
            status_ranking=ranking_data["status"]
        )
        
        session.add(scraping_session)
        session.commit()
        session_id = scraping_session.id
        
        return session_id
        
    finally:
        db_manager.close_session(session)

def get_or_create_character(db_manager: DatabaseManager, name: str, world: str) -> int:
    """
    Get existing character or create new one
    
    Args:
        db_manager (DatabaseManager): Database manager
        name (str): Character name
        world (str): World name
    
    Returns:
        int: Character ID
    """
    session = db_manager.get_session()
    
    try:
        # Try to find existing character
        character = session.query(Character).filter_by(name=name).first()
        
        if not character:
            # Create new character
            character = Character(name=name, world=world)
            session.add(character)
            session.commit()
            print(f"✅ Created new character: {name} ({world})")
        else:
            # Update world if different
            if character.world != world:
                character.world = world
                session.commit()
                print(f"🔄 Updated world for character {name}: {world}")
        
        return character.id
        
    finally:
        db_manager.close_session(session)

def insert_new_deaths(db_manager: DatabaseManager, character_id: int, scraping_session_id: int, 
                     deaths_df: pd.DataFrame) -> int:
    """
    Insert only new death records (differences from previous scraping)
    Uses unique constraints to prevent duplicates
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        scraping_session_id (int): Scraping session ID
        deaths_df (pd.DataFrame): Deaths dataframe from scraping
    
    Returns:
        int: Number of new deaths inserted
    """
    if deaths_df.empty:
        return 0
    
    session = db_manager.get_session()
    new_deaths = 0
    
    try:
        # Insert new deaths, skip duplicates using unique constraint
        for _, row in deaths_df.iterrows():
            death_time = parse_datetime(row['Time'])
            killed_by = str(row['Killed by'])
            
            # Check if this exact death already exists
            existing_death = session.query(CharacterDeath)\
                .filter_by(
                    character_id=character_id,
                    death_time=death_time,
                    killed_by=killed_by
                ).first()
            
            if not existing_death:
                death = CharacterDeath(
                    character_id=character_id,
                    scraping_session_id=scraping_session_id,
                    death_time=death_time,
                    level_at_death=int(row['Level']),
                    killed_by=killed_by
                )
                session.add(death)
                new_deaths += 1
        
        session.commit()
        if new_deaths > 0:
            print(f"➕ Inserted {new_deaths} new deaths for character {character_id}")
        
        return new_deaths
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error inserting deaths: {e}")
        return 0
        
    finally:
        db_manager.close_session(session)

def insert_new_kills(db_manager: DatabaseManager, character_id: int, scraping_session_id: int,
                    kills_df: pd.DataFrame) -> int:
    """
    Insert only new kill records (differences from previous scraping)
    Uses unique constraints to prevent duplicates
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        scraping_session_id (int): Scraping session ID
        kills_df (pd.DataFrame): Kills dataframe from scraping
    
    Returns:
        int: Number of new kills inserted
    """
    if kills_df.empty:
        return 0
    
    session = db_manager.get_session()
    new_kills = 0
    
    try:
        # Insert new kills, skip duplicates using unique constraint
        for _, row in kills_df.iterrows():
            kill_time = parse_datetime(row['Time'])
            victim_name = str(row['Victim'])
            
            # Check if this exact kill already exists
            existing_kill = session.query(CharacterKill)\
                .filter_by(
                    character_id=character_id,
                    kill_time=kill_time,
                    victim_name=victim_name
                ).first()
            
            if not existing_kill:
                kill = CharacterKill(
                    character_id=character_id,
                    scraping_session_id=scraping_session_id,
                    kill_time=kill_time,
                    victim_name=victim_name,
                    victim_level=int(row['Victim Level'])
                )
                session.add(kill)
                new_kills += 1
        
        session.commit()
        if new_kills > 0:
            print(f"➕ Inserted {new_kills} new kills for character {character_id}")
        
        return new_kills
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error inserting kills: {e}")
        return 0
        
    finally:
        db_manager.close_session(session)

def insert_new_online_times(db_manager: DatabaseManager, character_id: int, scraping_session_id: int,
                           online_df: pd.DataFrame) -> int:
    """
    Insert only new online time records (differences from previous scraping)
    Uses unique constraints to prevent duplicates
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        scraping_session_id (int): Scraping session ID
        online_df (pd.DataFrame): Online times dataframe from scraping
    
    Returns:
        int: Number of new online time records inserted
    """
    if online_df.empty:
        return 0
    
    session = db_manager.get_session()
    new_records = 0
    
    try:
        # Insert new online time records, skip duplicates using unique constraint
        for _, row in online_df.iterrows():
            date = parse_datetime(row['Date']).date()
            
            # Check if this date already exists
            existing_online_time = session.query(CharacterOnlineTime)\
                .filter_by(
                    character_id=character_id,
                    date=datetime.combine(date, datetime.min.time())
                ).first()
            
            if not existing_online_time:
                online_time = CharacterOnlineTime(
                    character_id=character_id,
                    scraping_session_id=scraping_session_id,
                    date=datetime.combine(date, datetime.min.time()),
                    online_time_minutes=parse_online_time(row['Online time'])
                )
                session.add(online_time)
                new_records += 1
            else:
                # Update existing record if new value is different
                new_minutes = parse_online_time(row['Online time'])
                if existing_online_time.online_time_minutes != new_minutes:
                    existing_online_time.online_time_minutes = new_minutes
                    existing_online_time.scraping_session_id = scraping_session_id
                    print(f"🔄 Updated online time for {character_id} on {date}: {new_minutes} minutes")
        
        session.commit()
        if new_records > 0:
            print(f"➕ Inserted {new_records} new online time records for character {character_id}")
        
        return new_records
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error inserting online times: {e}")
        return 0
        
    finally:
        db_manager.close_session(session)

def insert_new_experiences(db_manager: DatabaseManager, character_id: int, scraping_session_id: int,
                          experience_df: pd.DataFrame) -> int:
    """
    Insert only new experience records (differences from previous scraping)
    Uses unique constraints to prevent duplicates
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        scraping_session_id (int): Scraping session ID
        experience_df (pd.DataFrame): Experience dataframe from scraping
    
    Returns:
        int: Number of new experience records inserted
    """
    if experience_df.empty:
        return 0
    
    session = db_manager.get_session()
    new_records = 0
    
    try:
        # Insert new experience records, skip duplicates using unique constraint
        for _, row in experience_df.iterrows():
            date = parse_datetime(row['Data']).date()
            
            # Check if this date already exists
            existing_experience = session.query(CharacterExperience)\
                .filter_by(
                    character_id=character_id,
                    date=datetime.combine(date, datetime.min.time())
                ).first()
            
            if not existing_experience:
                experience = CharacterExperience(
                    character_id=character_id,
                    scraping_session_id=scraping_session_id,
                    date=datetime.combine(date, datetime.min.time()),
                    level=int(row['Level']),
                    level_delta=parse_level_delta(row['Δ Level']),
                    raw_xp_day=parse_experience_number(row['Raw XP no dia'])
                )
                session.add(experience)
                new_records += 1
            else:
                # Update existing record if new values are different
                new_level = int(row['Level'])
                new_level_delta = parse_level_delta(row['Δ Level'])
                new_xp = parse_experience_number(row['Raw XP no dia'])
                
                if (existing_experience.level != new_level or 
                    existing_experience.level_delta != new_level_delta or 
                    existing_experience.raw_xp_day != new_xp):
                    
                    existing_experience.level = new_level
                    existing_experience.level_delta = new_level_delta
                    existing_experience.raw_xp_day = new_xp
                    existing_experience.scraping_session_id = scraping_session_id
                    print(f"🔄 Updated experience for {character_id} on {date}: Level {new_level}, XP {new_xp:,}")
        
        session.commit()
        if new_records > 0:
            print(f"➕ Inserted {new_records} new experience records for character {character_id}")
        
        return new_records
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error inserting experiences: {e}")
        return 0
        
    finally:
        db_manager.close_session(session)

def process_character_data(db_manager: DatabaseManager, character_name: str, world: str, 
                          scraped_data: List[pd.DataFrame], status_data: Dict) -> Dict:
    """
    Process complete character data and insert only new records with native delta tracking
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_name (str): Character name
        world (str): World name
        scraped_data (List[pd.DataFrame]): List of scraped dataframes [deaths, kills, online, experience]
        status_data (Dict): Status data from scraping endpoint
    
    Returns:
        Dict: Summary of processing results
    """
    print(f"🔄 Processing data for character: {character_name} ({world})")
    
    # Parse status times natively
    world_status = status_data.get(world, pd.DataFrame())
    status_times = extract_status_times_native(world_status)
    
    # Create scraping session with parsed times
    scraping_session_id = create_scraping_session_with_parsed_times(db_manager, world, status_data, status_times)
    
    # Get or create character
    character_id = get_or_create_character(db_manager, character_name, world)
    
    results = {
        'character_id': character_id,
        'scraping_session_id': scraping_session_id,
        'new_deaths': 0,
        'new_kills': 0,
        'new_online_times': 0,
        'new_experiences': 0,
        'new_experience_deltas': 0,
        'new_online_deltas': 0
    }
    
    # Process each table if available
    if len(scraped_data) >= 4:
        deaths_df = scraped_data[0] if not scraped_data[0].empty else pd.DataFrame()
        kills_df = scraped_data[1] if not scraped_data[1].empty else pd.DataFrame()
        online_df = scraped_data[2] if not scraped_data[2].empty else pd.DataFrame()
        experience_df = scraped_data[3] if not scraped_data[3].empty else pd.DataFrame()
        
        results['new_deaths'] = insert_new_deaths(db_manager, character_id, scraping_session_id, deaths_df)
        results['new_kills'] = insert_new_kills(db_manager, character_id, scraping_session_id, kills_df)
        results['new_online_times'] = insert_new_online_times(db_manager, character_id, scraping_session_id, online_df)
        results['new_experiences'] = insert_new_experiences(db_manager, character_id, scraping_session_id, experience_df)
        
        # Native delta processing - always insert new delta rows
        if results['new_experiences'] > 0 and not experience_df.empty:
            results['new_experience_deltas'] = process_experience_deltas_native(
                db_manager, character_id, scraping_session_id, experience_df, status_times
            )
        
        if results['new_online_times'] > 0 and not online_df.empty:
            results['new_online_deltas'] = process_online_deltas_native(
                db_manager, character_id, scraping_session_id, online_df, status_times
            )
    
    total_new = sum([results['new_deaths'], results['new_kills'], 
                    results['new_online_times'], results['new_experiences']])
    total_deltas = results['new_experience_deltas'] + results['new_online_deltas']
    
    print(f"✅ Processing complete for {character_name}. New records: {total_new}, New deltas: {total_deltas}")
    
    return results

def extract_status_times_native(world_status: pd.DataFrame) -> Dict:
    """
    Extract and parse status times natively from world status data
    
    Args:
        world_status (pd.DataFrame): World status dataframe
    
    Returns:
        Dict: Parsed status times
    """
    status_times = {
        'deaths_kills_time': datetime.now(),
        'online_time': datetime.now(),
        'ranking_time': datetime.now()
    }
    
    if not world_status.empty:
        for _, row in world_status.iterrows():
            rotina = str(row.get('rotina', '')).lower()
            time_str = row.get('last update', '')
            
            if time_str:
                parsed_time = parse_portuguese_time(time_str)
                
                if 'deathskills' in rotina or 'death' in rotina:
                    status_times['deaths_kills_time'] = parsed_time
                elif 'online' in rotina:
                    status_times['online_time'] = parsed_time
                elif 'ranking' in rotina:
                    status_times['ranking_time'] = parsed_time
    
    return status_times

def create_scraping_session_with_parsed_times(db_manager: DatabaseManager, world: str, 
                                             status_data: Dict, status_times: Dict) -> int:
    """
    Create scraping session with natively parsed Portuguese times
    
    Args:
        db_manager (DatabaseManager): Database manager
        world (str): World name
        status_data (Dict): Raw status data
        status_times (Dict): Parsed status times
    
    Returns:
        int: Scraping session ID
    """
    session = db_manager.get_session()
    
    try:
        # Extract status information for the world
        world_status = status_data.get(world, pd.DataFrame())
        
        # Initialize default values
        death_kills_data = {"last update": "", "time_outdated": "", "status": ""}
        online_data = {"last update": "", "time_outdated": "", "status": ""}
        ranking_data = {"last update": "", "time_outdated": "", "status": ""}
        
        if not world_status.empty:
            for _, row in world_status.iterrows():
                rotina = str(row.get('rotina', '')).lower()
                if 'deathskills' in rotina or 'death' in rotina:
                    death_kills_data = {
                        "last update": parse_portuguese_time(row.get('last update', '')),
                        "time_outdated": row.get('time_outdated', ''),
                        # Also store parsed datetime for native use
                        "status": row.get('status', '')
                    }
                elif 'online' in rotina:
                    online_data = {
                        "last update": parse_portuguese_time(row.get('last update', '')),
                        "time_outdated": row.get('time_outdated', ''),
                        "status": row.get('status', '')
                    }
                elif 'ranking' in rotina:
                    ranking_data = {
                        "last update": parse_portuguese_time(row.get('last update', '')),
                        "time_outdated": row.get('time_outdated', ''),
                        "status": row.get('status', '')
                    }
        
        scraping_session = ScrapingSession(
            world=world,
            last_update_deaths_kills=death_kills_data["last update"],
            time_outdated_deaths_kills=death_kills_data["time_outdated"],
            status_deaths_kills=death_kills_data["status"],
            last_update_online=online_data["last update"],
            time_outdated_online=online_data["time_outdated"],
            status_online=online_data["status"],
            last_update_ranking=ranking_data["last update"],
            time_outdated_ranking=ranking_data["time_outdated"],
            status_ranking=ranking_data["status"]
        )
        
        session.add(scraping_session)
        session.commit()
        session_id = scraping_session.id
        
        return session_id
        
    finally:
        db_manager.close_session(session)

def process_experience_deltas_native(db_manager: DatabaseManager, character_id: int,
                                   scraping_session_id: int, experience_df: pd.DataFrame,
                                   status_times: Dict) -> int:
    """
    Process experience deltas natively - always insert new delta rows
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        scraping_session_id (int): Scraping session ID
        experience_df (pd.DataFrame): Experience dataframe
        status_times (Dict): Parsed status times
    
    Returns:
        int: Number of delta rows inserted
    """
    session = db_manager.get_session()
    deltas_inserted = 0
    
    try:
        # Get the most recent experience data from the dataframe
        if experience_df.empty:
            return 0
            
        first_row = experience_df.iloc[0]
        if 'Level' not in first_row or 'Raw XP/day' not in first_row:
            return 0
            
        new_level = int(first_row['Level'])
        new_xp = parse_experience_number(str(first_row['Raw XP/day']))
        current_time = datetime.now()
        status_time = status_times.get('ranking_time', current_time)
        
        # Get the last experience record for this character
        last_experience = session.query(CharacterExperience)\
            .filter_by(character_id=character_id)\
            .order_by(CharacterExperience.created_at.desc())\
            .first()
        
        if last_experience:
            # Calculate deltas
            xp_delta = new_xp - last_experience.raw_xp_day
            level_delta = new_level - last_experience.level
            
            # Calculate time difference
            time_diff = status_time - last_experience.created_at
            time_delta_minutes = max(1, int(time_diff.total_seconds() / 60))
            
            # Always insert new delta row - never update
            delta_experience = CharacterDeltaExperience(
                character_id=character_id,
                scraping_session_id=scraping_session_id,
                scraping_time_before=last_experience.created_at,
                status_time_before=last_experience.created_at,
                experience_before=last_experience.raw_xp_day,
                level_before=last_experience.level,
                scraping_time_after=current_time,
                status_time_after=status_time,
                experience_after=new_xp,
                level_after=new_level,
                experience_delta=xp_delta,
                level_delta=level_delta,
                time_delta_minutes=time_delta_minutes
            )
            
            session.add(delta_experience)
            session.commit()
            deltas_inserted = 1
            
            print(f"📈 XP Delta inserted: {xp_delta:,} XP, {level_delta} levels over {time_delta_minutes} min")
        
        return deltas_inserted
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error processing XP delta: {e}")
        return 0
        
    finally:
        db_manager.close_session(session)

def process_online_deltas_native(db_manager: DatabaseManager, character_id: int,
                                scraping_session_id: int, online_df: pd.DataFrame,
                                status_times: Dict) -> int:
    """
    Process online deltas natively - always insert new delta rows
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        scraping_session_id (int): Scraping session ID
        online_df (pd.DataFrame): Online time dataframe
        status_times (Dict): Parsed status times
    
    Returns:
        int: Number of delta rows inserted
    """
    session = db_manager.get_session()
    deltas_inserted = 0
    
    try:
        # Get the most recent online data from the dataframe
        if online_df.empty:
            return 0
            
        first_row = online_df.iloc[0]
        if 'Online Time' not in first_row:
            return 0
            
        new_online_minutes = parse_online_time(str(first_row['Online Time']))
        current_time = datetime.now()
        status_time = status_times.get('online_time', current_time)
        
        # Get the last online record for this character
        last_online = session.query(CharacterOnlineTime)\
            .filter_by(character_id=character_id)\
            .order_by(CharacterOnlineTime.created_at.desc())\
            .first()
        
        if last_online:
            # Calculate deltas
            online_delta = new_online_minutes - last_online.online_time_minutes
            
            # Calculate time difference
            time_diff = status_time - last_online.created_at
            time_delta_minutes = max(1, int(time_diff.total_seconds() / 60))
            
            # Always insert new delta row - never update
            delta_online = CharacterDeltaOnline(
                character_id=character_id,
                scraping_session_id=scraping_session_id,
                scraping_time_before=last_online.created_at,
                status_time_before=last_online.created_at,
                online_minutes_before=last_online.online_time_minutes,
                scraping_time_after=current_time,
                status_time_after=status_time,
                online_minutes_after=new_online_minutes,
                online_minutes_delta=online_delta,
                time_delta_minutes=time_delta_minutes
            )
            
            session.add(delta_online)
            session.commit()
            deltas_inserted = 1
            
            print(f"⏰ Online Delta inserted: {online_delta} min over {time_delta_minutes} min")
        
        return deltas_inserted
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error processing online delta: {e}")
        return 0
        
    finally:
        db_manager.close_session(session)

def get_all_online_deltas_with_constraint(db_manager: DatabaseManager, 
                                        character_ids: List[int] = None,
                                        hours_back: int = 24,
                                        min_delta: int = 0) -> pd.DataFrame:
    """
    Get all online time deltas with time constraints
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_ids (List[int]): List of character IDs to filter (None for all)
        hours_back (int): Hours to look back from now
        min_delta (int): Minimum delta value to include
    
    Returns:
        pd.DataFrame: DataFrame with online delta data
    """
    session = db_manager.get_session()
    
    try:
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        # Build query
        query = session.query(
            CharacterDeltaOnline.character_id,
            Character.name.label('character_name'),
            CharacterDeltaOnline.status_time_after,
            CharacterDeltaOnline.online_minutes_delta,
            CharacterDeltaOnline.time_delta_minutes,
            CharacterDeltaOnline.online_minutes_before,
            CharacterDeltaOnline.online_minutes_after
        ).join(Character).filter(
            CharacterDeltaOnline.status_time_after >= cutoff_time,
            CharacterDeltaOnline.online_minutes_delta >= min_delta
        )
        
        # Filter by character IDs if provided
        if character_ids:
            query = query.filter(CharacterDeltaOnline.character_id.in_(character_ids))
        
        # Order by most recent first
        query = query.order_by(CharacterDeltaOnline.status_time_after.desc())
        
        # Execute and convert to DataFrame
        results = query.all()
        
        if results:
            df = pd.DataFrame(results)
            df['efficiency_rate'] = df['online_minutes_delta'] / df['time_delta_minutes']  # Minutes online per minute passed
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error getting online deltas: {e}")
        return pd.DataFrame()
        
    finally:
        db_manager.close_session(session)

def calculate_and_store_experience_delta(db_manager: DatabaseManager, character_id: int, 
                                       scraping_session_id: int, new_xp: int, 
                                       new_level: int, status_data: Dict) -> bool:
    """
    Calculate experience delta from previous measurement and store it
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        scraping_session_id (int): Current scraping session ID
        new_xp (int): New experience value
        new_level (int): New level value
        status_data (Dict): Status data with timestamp info
    
    Returns:
        bool: True if delta was calculated and stored
    """
    session = db_manager.get_session()
    
    try:
        # Get the last experience record for this character
        last_experience = session.query(CharacterExperience)\
            .filter_by(character_id=character_id)\
            .order_by(CharacterExperience.created_at.desc())\
            .first()
        
        # Parse current status time
        world_status = list(status_data.values())[0] if status_data else pd.DataFrame()
        current_status_time = datetime.now()
        
        if not world_status.empty:
            for _, row in world_status.iterrows():
                if 'ranking' in str(row.get('rotina', '')).lower():
                    time_str = row.get('last update', '')
                    if time_str:
                        current_status_time = parse_portuguese_time(time_str)
                    break
        
        if last_experience:
            # Calculate deltas
            xp_delta = new_xp - last_experience.raw_xp_day
            level_delta = new_level - last_experience.level
            
            # Calculate time difference
            time_diff = current_status_time - last_experience.created_at
            time_delta_minutes = int(time_diff.total_seconds() / 60)
            
            # Create delta record - ALWAYS INSERT NEW ROW
            delta_experience = CharacterDeltaExperience(
                character_id=character_id,
                scraping_session_id=scraping_session_id,
                scraping_time_before=last_experience.created_at,
                status_time_before=last_experience.created_at,  # Approximate
                experience_before=last_experience.raw_xp_day,
                level_before=last_experience.level,
                scraping_time_after=datetime.now(),
                status_time_after=current_status_time,
                experience_after=new_xp,
                level_after=new_level,
                experience_delta=xp_delta,
                level_delta=level_delta,
                time_delta_minutes=max(1, time_delta_minutes)  # Minimum 1 minute
            )
            
            session.add(delta_experience)
            session.commit()
            
            print(f"📈 XP Delta stored: {xp_delta:,} XP, {level_delta} levels over {time_delta_minutes} minutes")
            return True
        else:
            print("📈 First XP record - no delta to calculate")
            return False
            
    except Exception as e:
        session.rollback()
        print(f"❌ Error calculating XP delta: {e}")
        return False
        
    finally:
        db_manager.close_session(session)

def calculate_and_store_online_delta(db_manager: DatabaseManager, character_id: int,
                                   scraping_session_id: int, new_online_minutes: int,
                                   status_data: Dict) -> bool:
    """
    Calculate online time delta from previous measurement and store it
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID  
        scraping_session_id (int): Current scraping session ID
        new_online_minutes (int): New online time in minutes
        status_data (Dict): Status data with timestamp info
    
    Returns:
        bool: True if delta was calculated and stored
    """
    session = db_manager.get_session()
    
    try:
        # Get the last online time record for this character
        last_online = session.query(CharacterOnlineTime)\
            .filter_by(character_id=character_id)\
            .order_by(CharacterOnlineTime.created_at.desc())\
            .first()
        
        # Parse current status time
        world_status = list(status_data.values())[0] if status_data else pd.DataFrame()
        current_status_time = datetime.now()
        
        if not world_status.empty:
            for _, row in world_status.iterrows():
                if 'online' in str(row.get('rotina', '')).lower():
                    time_str = row.get('last update', '')
                    if time_str:
                        current_status_time = parse_portuguese_time(time_str)
                    break
        
        if last_online:
            # Calculate deltas
            online_delta = new_online_minutes - last_online.online_time_minutes
            
            # Calculate time difference  
            time_diff = current_status_time - last_online.created_at
            time_delta_minutes = int(time_diff.total_seconds() / 60)
            
            # Create delta record - ALWAYS INSERT NEW ROW
            delta_online = CharacterDeltaOnline(
                character_id=character_id,
                scraping_session_id=scraping_session_id,
                scraping_time_before=last_online.created_at,
                status_time_before=last_online.created_at,  # Approximate
                online_minutes_before=last_online.online_time_minutes,
                scraping_time_after=datetime.now(),
                status_time_after=current_status_time,
                online_minutes_after=new_online_minutes,
                online_minutes_delta=online_delta,
                time_delta_minutes=max(1, time_delta_minutes)  # Minimum 1 minute
            )
            
            session.add(delta_online)
            session.commit()
            
            print(f"⏰ Online Delta stored: {online_delta} minutes over {time_delta_minutes} minutes")
            return True
        else:
            print("⏰ First online record - no delta to calculate")
            return False
            
    except Exception as e:
        session.rollback()
        print(f"❌ Error calculating online delta: {e}")
        return False
        
    finally:
        db_manager.close_session(session)