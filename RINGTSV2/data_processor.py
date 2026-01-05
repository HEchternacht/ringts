"""
Data processing - insert character data into database with delta tracking.
Clean, simple, and reliable.
"""
import pandas as pd
from datetime import datetime
from database import (Character, ScrapingSession, CharacterDeath, CharacterKill,
                     CharacterOnlineTime, CharacterExperience, 
                     CharacterDeltaExperience, CharacterDeltaOnline)
from parsers import (parse_datetime, parse_online_time, parse_experience_number,
                    parse_level_delta, parse_portuguese_time, normalize_column_names)


def get_or_create_character(db, name, world):
    """Get existing character or create new one"""
    with db.session() as session:
        char = session.query(Character).filter_by(name=name).first()
        
        if not char:
            char = Character(name=name, world=world)
            session.add(char)
            session.flush()
        
        return char.id


def create_scraping_session(db, world, status_data=None):
    """Create a new scraping session"""
    with db.session() as session:
        scraping_session = ScrapingSession(
            world=world,
            session_timestamp=datetime.now()
        )
        
        # Add status metadata if available
        if status_data and world in status_data:
            world_status = status_data[world]
            for _, row in world_status.iterrows():
                routine = str(row['rotina']).lower()
                if 'death' in routine or 'kill' in routine:
                    scraping_session.deaths_kills_update = parse_portuguese_time(row['last_update'])
                elif 'online' in routine:
                    scraping_session.online_update = parse_portuguese_time(row['last_update'])
                elif 'ranking' in routine:
                    scraping_session.ranking_update = parse_portuguese_time(row['last_update'])
        
        session.add(scraping_session)
        session.flush()
        
        return scraping_session.id


def insert_deaths(db, character_id, session_id, deaths_df):
    """Insert new death records"""
    if deaths_df.empty:
        return 0
    
    deaths_df = normalize_column_names(deaths_df)
    count = 0
    
    with db.session() as session:
        for _, row in deaths_df.iterrows():
            # Check if already exists
            death_time = parse_datetime(row.get('Kill Time', row.get('Time', '')))
            killed_by = row.get('Killed By', '')
            
            exists = session.query(CharacterDeath).filter_by(
                character_id=character_id,
                death_time=death_time,
                killed_by=killed_by
            ).first()
            
            if not exists:
                death = CharacterDeath(
                    character_id=character_id,
                    scraping_session_id=session_id,
                    death_time=death_time,
                    level_at_death=int(row.get('Level', 0)),
                    killed_by=killed_by
                )
                session.add(death)
                count += 1
    
    if count > 0:
        print(f"  ➕ Inserted {count} new deaths")
    
    return count


def insert_kills(db, character_id, session_id, kills_df):
    """Insert new kill records"""
    if kills_df.empty:
        return 0
    
    kills_df = normalize_column_names(kills_df)
    count = 0
    
    with db.session() as session:
        for _, row in kills_df.iterrows():
            kill_time = parse_datetime(row.get('Kill Time', row.get('Time', '')))
            victim_name = row.get('Victim Name', '')
            
            exists = session.query(CharacterKill).filter_by(
                character_id=character_id,
                kill_time=kill_time,
                victim_name=victim_name
            ).first()
            
            if not exists:
                kill = CharacterKill(
                    character_id=character_id,
                    scraping_session_id=session_id,
                    kill_time=kill_time,
                    victim_name=victim_name,
                    victim_level=int(row.get('Victim Level', 0))
                )
                session.add(kill)
                count += 1
    
    if count > 0:
        print(f"  ➕ Inserted {count} new kills")
    
    return count


def insert_online_times(db, character_id, session_id, online_df):
    """Insert or update online time records"""
    if online_df.empty:
        return 0
    
    online_df = normalize_column_names(online_df)
    count = 0
    
    with db.session() as session:
        for _, row in online_df.iterrows():
            date = parse_datetime(row.get('Date', '')).replace(hour=0, minute=0, second=0, microsecond=0)
            online_minutes = parse_online_time(row.get('Online Time', ''))
            
            existing = session.query(CharacterOnlineTime).filter_by(
                character_id=character_id,
                date=date
            ).first()
            
            if not existing:
                online_time = CharacterOnlineTime(
                    character_id=character_id,
                    scraping_session_id=session_id,
                    date=date,
                    online_time_minutes=online_minutes
                )
                session.add(online_time)
                count += 1
            elif existing.online_time_minutes != online_minutes:
                # Update if value changed
                existing.online_time_minutes = online_minutes
                existing.scraping_session_id = session_id
    
    if count > 0:
        print(f"  ➕ Inserted {count} new online times")
    
    return count


def insert_experiences(db, character_id, session_id, experience_df):
    """Insert or update experience records"""
    if experience_df.empty:
        return 0
    
    experience_df = normalize_column_names(experience_df)
    count = 0
    
    with db.session() as session:
        for _, row in experience_df.iterrows():
            date = parse_datetime(row.get('Date', '')).replace(hour=0, minute=0, second=0, microsecond=0)
            level = int(row.get('Level', 0))
            level_delta = parse_level_delta(row.get('Level Delta', ''))
            raw_xp = parse_experience_number(row.get('Raw XP Day', ''))
            
            existing = session.query(CharacterExperience).filter_by(
                character_id=character_id,
                date=date
            ).first()
            
            if not existing:
                exp = CharacterExperience(
                    character_id=character_id,
                    scraping_session_id=session_id,
                    date=date,
                    level=level,
                    level_delta=level_delta,
                    raw_xp_day=raw_xp
                )
                session.add(exp)
                count += 1
            elif existing.raw_xp_day != raw_xp:
                # Update if value changed
                existing.raw_xp_day = raw_xp
                existing.level = level
                existing.level_delta = level_delta
                existing.scraping_session_id = session_id
    
    if count > 0:
        print(f"  ➕ Inserted {count} new experiences")
    
    return count


def calculate_experience_delta(db, character_id, session_id, experience_df, status_time=None):
    """Calculate and insert experience delta"""
    if experience_df.empty:
        return 0
    
    experience_df = normalize_column_names(experience_df)
    
    # Get latest experience from dataframe
    first_row = experience_df.iloc[0]
    new_level = int(first_row.get('Level', 0))
    new_xp = parse_experience_number(first_row.get('Raw XP Day', ''))
    
    if new_xp == 0:
        print("  📈 New experience is zero, skipping delta calculation.")
        return 0
    
    current_time = datetime.now()
    if status_time is None:
        status_time = current_time
    
    with db.session() as session:
        # Get last experience record
        last_exp = session.query(CharacterExperience).filter_by(
            character_id=character_id
        ).order_by(CharacterExperience.created_at.desc()).first()
        
        if not last_exp:
            #create a dummy object with zero experience
            print("  📈 No previous experience record found, assuming zero previous experience.")
            last_exp = CharacterExperience(
                character_id=character_id,
                scraping_session_id=session_id,
                date=current_time.replace(hour=0, minute=0, second=0, microsecond=0),
                level=0,
                level_delta=0,
                raw_xp_day=0,
                created_at=current_time
            )
            return 0
        
        # Calculate deltas
        print(f"  📈 Calculating Experience Delta: New {new_xp:,} XP - Last {last_exp.raw_xp_day:,} XP")
        xp_delta = new_xp - last_exp.raw_xp_day
        print(f"  📈 Calculated XP Delta: {xp_delta:,} XP")
        level_delta = new_level - last_exp.level
        time_diff = (status_time - last_exp.created_at).total_seconds() / 60
        time_delta_minutes = max(1, int(time_diff))
        
        # Only insert if there's actual change
        if xp_delta == 0 and level_delta == 0:
            print("  📈 No change in experience or level, skipping delta insertion.")
            return 0
        
        # Insert delta record
        print(f"inserting data, {character_id}, {session_id}, {last_exp.created_at}, {last_exp.raw_xp_day}, {last_exp.level}, {status_time}, {new_xp}, {new_level}, {xp_delta}, {level_delta}, {time_delta_minutes}")
        delta = CharacterDeltaExperience(
            character_id=character_id,
            scraping_session_id=session_id,
            time_before=last_exp.created_at,
            experience_before=last_exp.raw_xp_day,
            level_before=last_exp.level,
            time_after=status_time,
            experience_after=new_xp,
            level_after=new_level,
            experience_delta=xp_delta,
            level_delta=level_delta,
            time_delta_minutes=time_delta_minutes
        )
        session.add(delta)
        
        xp_per_hour = (xp_delta / time_delta_minutes * 60) if time_delta_minutes > 0 else 0
        print(f"  📈 XP Delta: {xp_delta:,} XP, {level_delta} levels ({xp_per_hour:,.0f} XP/h)")
        
        return xp_delta


def calculate_online_delta(db, character_id, session_id, online_df, status_time=None):
    """Calculate and insert online time delta"""
    if online_df.empty:
        return 0
    
    online_df = normalize_column_names(online_df)
    
    # Get latest online time from dataframe
    first_row = online_df.iloc[0]
    new_online_minutes = parse_online_time(first_row["Online Time"])
    print(f"  ⏰ New Online Minutes: {new_online_minutes} min")
    if new_online_minutes == 0:
        print("  ⏰ New online minutes is zero, skipping delta calculation.")
        return 0
    
    current_time = datetime.now()
    if status_time is None:
        status_time = current_time
    
    with db.session() as session:
        # Get last online record
        last_online = session.query(CharacterOnlineTime).filter_by(
            character_id=character_id
        ).order_by(CharacterOnlineTime.created_at.desc()).first()
        
        print(last_online)
        if not last_online:
            #create a dummy object with zero online time
            print("  ⏰ No previous online record found, assuming zero previous online time.")
            last_online = CharacterOnlineTime(
                character_id=character_id,
                scraping_session_id=session_id,
                date=current_time.replace(hour=0, minute=0, second=0, microsecond=0),
                online_time_minutes=0,
                created_at=current_time
            )
        
        print(f"Last online record: {last_online.created_at}, {last_online.online_time_minutes}")
        
        # Calculate deltas
        print(f"  ⏰ Calculating Online Delta: New {new_online_minutes} - Last {last_online.online_time_minutes}")
        online_delta = new_online_minutes - last_online.online_time_minutes
        print(f"  ⏰ Calculated Online Delta: {online_delta} min")
        time_diff = (status_time - last_online.created_at).total_seconds() / 60
        time_delta_minutes = max(1, int(time_diff))
        
        # Only insert if there's actual change
        if online_delta == 0:
            print("  ⏰ No change in online time, skipping delta insertion.")
            return 0
        
        # Insert delta record

        print(f"inserting data, {character_id}, {session_id}, {last_online.created_at}, {last_online.online_time_minutes}, {status_time}, {new_online_minutes}, {online_delta}, {time_delta_minutes}")
        delta = CharacterDeltaOnline(
            character_id=character_id,
            scraping_session_id=session_id,
            time_before=last_online.created_at,
            online_minutes_before=last_online.online_time_minutes,
            time_after=status_time,
            online_minutes_after=new_online_minutes,
            online_minutes_delta=online_delta,
            time_delta_minutes=time_delta_minutes
        )
        session.add(delta)
        
        efficiency = (online_delta / time_delta_minutes * 100) if time_delta_minutes > 0 else 0
        print(f"  ⏰ Online Delta: {online_delta} min ({efficiency:.1f}% efficiency)")
        
        return online_delta


def process_character(db, character_name, world, scraped_tables, status_data=None):
    """
    Main processing function - insert all character data and calculate deltas.
    
    Args:
        db: Database instance
        character_name: Character name
        world: World name
        scraped_tables: List of dataframes [deaths, kills, online, experience]
        status_data: Optional status data dict
    
    Returns:
        Dict with processing results
    """
    print(f"\n🔄 Processing {character_name} ({world})")
    
    # Get or create character
    character_id = get_or_create_character(db, character_name, world)
    
    # Create scraping session
    session_id = create_scraping_session(db, world, status_data)
    
    # Parse status time if available
    status_time = None
    if status_data and world in status_data:
        world_status = status_data[world]
        for _, row in world_status.iterrows():
            if 'ranking' in str(row['rotina']).lower():
                status_time = parse_portuguese_time(row['last_update'])
                break
    
    # Initialize results
    results = {
        'character_id': character_id,
        'session_id': session_id,
        'deaths': 0,
        'kills': 0,
        'online_times': 0,
        'experiences': 0,
        'xp_deltas': 0,
        'online_deltas': 0
    }
    
    # Process tables
    if len(scraped_tables) >= 4:
        deaths_df = scraped_tables[0] if not scraped_tables[0].empty else pd.DataFrame()
        kills_df = scraped_tables[1] if not scraped_tables[1].empty else pd.DataFrame()
        online_df = scraped_tables[2] if not scraped_tables[2].empty else pd.DataFrame()
        experience_df = scraped_tables[3] if not scraped_tables[3].empty else pd.DataFrame()
        
        if not experience_df.empty:
            print(f"  🔢 Calculating experience delta...")
            results['xp_deltas'] = calculate_experience_delta(db, character_id, session_id, 
                                                             experience_df, status_time)
        
        if not online_df.empty:
            print(f"  🔢 Calculating online time delta...")
            results['online_deltas'] = calculate_online_delta(db, character_id, session_id, 
                                                              online_df, status_time)
    

        results['deaths'] = insert_deaths(db, character_id, session_id, deaths_df)
        results['kills'] = insert_kills(db, character_id, session_id, kills_df)
        results['online_times'] = insert_online_times(db, character_id, session_id, online_df)
        results['experiences'] = insert_experiences(db, character_id, session_id, experience_df)
        
        print(f"  ➕ Total new records: {results['deaths'] + results['kills'] + results['online_times'] + results['experiences']}")

     
    
    total_new = results['deaths'] + results['kills'] + results['online_times'] + results['experiences']
    total_deltas = results['xp_deltas'] + results['online_deltas']
    
    print(f"✅ Complete: {total_new} new records, {total_deltas} deltas calculated")
    
    return results
