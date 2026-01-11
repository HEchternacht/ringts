"""
Test script for daily reset functionality.
This verifies that the reset happens correctly and only once per day.
"""
from database_sqlalchemy import SQLAlchemyDatabase
from database_models import Player
from datetime import datetime
import os

def test_daily_reset():
    print("=" * 60)
    print("Testing Daily Reset Functionality")
    print("=" * 60)
    
    # Use test database
    test_folder = "var/data_test"
    os.makedirs(test_folder, exist_ok=True)
    
    # Create database instance
    db = SQLAlchemyDatabase(folder=test_folder)
    
    # Add some test players
    session = db._get_session()
    try:
        print("\n1. Adding test players...")
        for i in range(5):
            player = Player(
                name=f"TestPlayer{i}",
                exp=1000 * (i + 1),
                last_update=datetime.utcnow(),
                world="TestWorld",
                guild="TestGuild"
            )
            session.add(player)
        session.commit()
        
        player_count = session.query(Player).count()
        print(f"   ✓ Added {player_count} players")
        
    finally:
        session.close()
    
    # Check current time and reset status
    print(f"\n2. Current local time: {db._get_local_datetime()}")
    print(f"   Reset time configured: {db.daily_reset_hour:02d}:{db.daily_reset_minute:02d}")
    print(f"   Timezone offset: +{db.timezone_offset_hours} hours")
    
    last_reset = db._get_last_reset_date()
    print(f"\n3. Last reset date: {last_reset or 'Never'}")
    
    should_reset = db._should_reset_today()
    print(f"\n4. Should reset today: {should_reset}")
    
    if should_reset:
        print("\n5. Testing reset...")
        db._perform_daily_reset()
        
        session = db._get_session()
        try:
            player_count = session.query(Player).count()
            print(f"   ✓ Players after reset: {player_count}")
            
            if player_count == 0:
                print("   ✓ PASS: Reset successful!")
            else:
                print("   ✗ FAIL: Players still exist after reset")
        finally:
            session.close()
        
        # Try resetting again - should not happen
        print("\n6. Testing reset prevention (should not reset twice)...")
        initial_reset_date = db._get_last_reset_date()
        db.check_daily_reset()
        final_reset_date = db._get_last_reset_date()
        
        if initial_reset_date == final_reset_date:
            print("   ✓ PASS: Reset prevented (same date)")
        else:
            print("   ✗ FAIL: Reset happened twice on same day")
    else:
        print("\n5. Not time to reset yet or already reset today")
        print(f"   Next reset will happen at {db.daily_reset_hour:02d}:{db.daily_reset_minute:02d}")
    
    # Test persistence across restarts
    print("\n7. Testing persistence across 'restarts'...")
    last_reset_before = db._get_last_reset_date()
    
    # Create new instance (simulates restart)
    db2 = SQLAlchemyDatabase(folder=test_folder)
    last_reset_after = db2._get_last_reset_date()
    
    if last_reset_before == last_reset_after:
        print(f"   ✓ PASS: Reset date persisted ({last_reset_after})")
    else:
        print(f"   ✗ FAIL: Reset date not persisted")
    
    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60)
    
    # Cleanup
    import shutil
    try:
        shutil.rmtree(test_folder)
        print("\n✓ Test database cleaned up")
    except:
        pass


if __name__ == '__main__':
    test_daily_reset()
