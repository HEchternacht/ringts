"""
Example usage and testing script for the Tibia scraper database system
"""

import pandas as pd
from datetime import datetime, timedelta
from alchemy import get_database_manager
from utils import process_character_data

def test_database_setup():
    """Test the database setup and table creation"""
    print("🔧 Testing database setup...")
    
    # Initialize SQLite database (no server required!)
    db_manager = get_database_manager('tibia_scraper_test.db')
    
    # Create tables
    db_manager.create_tables()
    print("✅ SQLite database tables created successfully!")
    
    return db_manager

def create_sample_data():
    """Create sample data similar to the scraped format"""
    
    # Sample deaths data (Table 1)
    deaths_data = {
        'Time': ['23/12/2025 19:24', '22/12/2025 01:55', '21/12/2025 15:18'],
        'World': ['Auroria', 'Auroria', 'Auroria'],
        'Level': [1521, 1519, 1519],
        'Killed by': ['frostreaper', 'sulphider', 'frostreaper']
    }
    deaths_df = pd.DataFrame(deaths_data)
    
    # Sample kills data (Table 2)
    kills_data = {
        'Time': ['23/12/2025 16:16', '23/12/2025 00:27', '19/12/2025 02:36'],
        'World': ['Auroria', 'Auroria', 'Auroria'],
        'Victim': ['Fator Suurpresa', 'Gauchao Bomba Um', 'Magnuszord Bomba Defender'],
        'Victim Level': [658, 783, 894]
    }
    kills_df = pd.DataFrame(kills_data)
    
    # Sample online time data (Table 3)
    online_data = {
        'Date': ['29/12/2025', '28/12/2025', '27/12/2025', '26/12/2025'],
        'Online time': ['3h 10m', '2h 20m', '12h 10m', '8h 40m']
    }
    online_df = pd.DataFrame(online_data)
    
    # Sample experience data (Table 4)
    experience_data = {
        'Data': ['28/12/2025', '27/12/2025', '26/12/2025', '25/12/2025'],
        'Level': [1531, 1529, 1526, 1523],
        'Δ Level': ['+2', '+3', '+3', '+1'],
        'Raw XP no dia': ['176.495.455', '183.245.167', '188.546.855', '146.823.790']
    }
    experience_df = pd.DataFrame(experience_data)
    
    return [deaths_df, kills_df, online_df, experience_df]

def create_sample_status_data():
    """Create sample status data"""
    auroria_status = pd.DataFrame({
        'rotina': ['Daily Raw Ranking', 'Online', 'DeathsKills'],
        'last update': ['Hoje às 09:23', 'Hoje às 09:19', 'Hoje às 09:17'],
        'time_outdated': ['0 min', '4 min', '6 min'],
        'status': ['Atualizado', 'Atualizado', 'Atualizado']
    })
    
    return {'Auroria': auroria_status}

def test_data_insertion():
    """Test the complete data insertion process"""
    print("\n🧪 Testing data insertion...")
    
    # Get database manager
    db_manager = test_database_setup()
    
    # Create sample data
    scraped_data = create_sample_data()
    status_data = create_sample_status_data()
    
    # Process character data
    character_name = "rollabostx"
    world = "Auroria"
    
    results = process_character_data(
        db_manager=db_manager,
        character_name=character_name,
        world=world,
        scraped_data=scraped_data,
        status_data=status_data
    )
    
    print(f"\n📊 Processing Results:")
    print(f"   Character ID: {results['character_id']}")
    print(f"   Scraping Session ID: {results['scraping_session_id']}")
    print(f"   New Deaths: {results['new_deaths']}")
    print(f"   New Kills: {results['new_kills']}")
    print(f"   New Online Times: {results['new_online_times']}")
    print(f"   New Experiences: {results['new_experiences']}")
    
    # Test second insertion (should insert 0 new records)
    print("\n🔄 Testing second insertion (should be 0 new records)...")
    results2 = process_character_data(
        db_manager=db_manager,
        character_name=character_name,
        world=world,
        scraped_data=scraped_data,
        status_data=status_data
    )
    
    print(f"   Second insertion - New Deaths: {results2['new_deaths']}")
    print(f"   Second insertion - New Kills: {results2['new_kills']}")
    print(f"   Second insertion - New Online Times: {results2['new_online_times']}")
    print(f"   Second insertion - New Experiences: {results2['new_experiences']}")
    
    return db_manager

def query_character_data(db_manager, character_name: str):
    """Query and display character data"""
    print(f"\n📋 Querying data for character: {character_name}")
    
    from alchemy import Character, CharacterDeath, CharacterKill, CharacterOnlineTime, CharacterExperience
    
    session = db_manager.get_session()
    
    try:
        # Get character
        character = session.query(Character).filter_by(name=character_name).first()
        if not character:
            print(f"❌ Character '{character_name}' not found")
            return
        
        print(f"✅ Character: {character.name} (ID: {character.id}, World: {character.world})")
        
        # Query deaths
        deaths = session.query(CharacterDeath).filter_by(character_id=character.id).order_by(CharacterDeath.death_time.desc()).limit(5).all()
        print(f"\n💀 Recent Deaths ({len(deaths)}):")
        for death in deaths:
            print(f"   {death.death_time} - Level {death.level_at_death} - Killed by {death.killed_by}")
        
        # Query kills
        kills = session.query(CharacterKill).filter_by(character_id=character.id).order_by(CharacterKill.kill_time.desc()).limit(5).all()
        print(f"\n⚔️ Recent Kills ({len(kills)}):")
        for kill in kills:
            print(f"   {kill.kill_time} - {kill.victim_name} (Level {kill.victim_level})")
        
        # Query online times
        online_times = session.query(CharacterOnlineTime).filter_by(character_id=character.id).order_by(CharacterOnlineTime.date.desc()).limit(5).all()
        print(f"\n🕐 Recent Online Times ({len(online_times)}):")
        for ot in online_times:
            hours = ot.online_time_minutes // 60
            minutes = ot.online_time_minutes % 60
            print(f"   {ot.date.date()} - {hours}h {minutes}m")
        
        # Query experiences
        experiences = session.query(CharacterExperience).filter_by(character_id=character.id).order_by(CharacterExperience.date.desc()).limit(5).all()
        print(f"\n📈 Recent Experiences ({len(experiences)}):")
        for exp in experiences:
            print(f"   {exp.date.date()} - Level {exp.level} ({exp.level_delta:+d}) - XP: {exp.raw_xp_day:,}")
        
    finally:
        db_manager.close_session(session)

def integration_with_scraper_example():
    """Example of how to integrate with the existing scraper code"""
    print("\n🔗 Integration Example with Existing Scraper:")
    print("""
# In your main scraping script, you would use it like this:

from alchemy import get_database_manager
from utils import process_character_data

# Initialize SQLite database (no server setup required!)
db_manager = get_database_manager('tibia_scraper.db')  # Creates tibia_scraper.db file

# Create tables (run once)
db_manager.create_tables()

# For each character you scrape:
character_name = "rollabostx"
world = "Auroria"

# Use your existing scraper functions
data = scrape_player_data(character_name)  # Your existing function
status_data = get_last_status_updates()    # Your existing function

if data['success'] and data['tables']:
    # Process and insert only new data
    results = process_character_data(
        db_manager=db_manager,
        character_name=character_name,
        world=world,
        scraped_data=data['tables'],  # List of DataFrames
        status_data=status_data       # Status dict
    )
    
    print(f"Processed {character_name}: {results}")

# The system will automatically:
# 1. Create scraping session with status timestamps
# 2. Create character record if doesn't exist
# 3. Insert only NEW deaths, kills, online times, and experiences
# 4. Skip duplicates based on timestamps and dates
""")

if __name__ == "__main__":
    # Run tests
    db_manager = test_data_insertion()
    query_character_data(db_manager, "rollabostx")
    integration_with_scraper_example()
    
    print("\n✅ All tests completed!")