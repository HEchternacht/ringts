"""
Example usage and test cases for RINGTS V2
"""
from database import Database
from scraper import scrape_character, scrape_status
from data_processor import process_character
from analytics import (
    get_top_xp_players, get_top_online_players, get_top_killers,
    get_character_summary, get_character_xp_history,
    get_top_xp_delta_players, get_character_delta_summary,
    export_to_csv
)


def example_1_basic_scraping():
    """Example 1: Basic character scraping and storage"""
    print("\n" + "="*50)
    print("EXAMPLE 1: Basic Character Scraping")
    print("="*50)
    
    db = Database('tibia_v2.db')
    db.create_tables()
    
    # Scrape a character
    character_name = "Rollabostx"
    print(f"\n📡 Scraping {character_name}...")
    
    tables = scrape_character(character_name)
    status_data = scrape_status()
    
    if tables:
        results = process_character(db, character_name, "Auroria", tables, status_data)
        print(f"\n✅ Processed successfully!")
        print(f"   New records: {sum([results['deaths'], results['kills'], results['online_times'], results['experiences']])}")
        print(f"   Deltas calculated: {results['xp_deltas'] + results['online_deltas']}")


def example_2_multiple_characters():
    """Example 2: Process multiple characters"""
    print("\n" + "="*50)
    print("EXAMPLE 2: Multiple Characters")
    print("="*50)
    
    db = Database('tibia_v2.db')
    db.create_tables()
    
    characters = ["Rollabostx", "King Bueno"]
    status_data = scrape_status()
    
    for char_name in characters:
        print(f"\n📡 Processing {char_name}...")
        tables = scrape_character(char_name)
        
        if tables:
            process_character(db, char_name, "Auroria", tables, status_data)
        else:
            print(f"❌ Failed to scrape {char_name}")


def example_3_analytics():
    """Example 3: Run analytics queries"""
    print("\n" + "="*50)
    print("EXAMPLE 3: Analytics")
    print("="*50)
    
    db = Database('tibia_v2.db')
    
    # Top players
    print("\n🏆 Top XP Players (Last 7 days):")
    top_xp = get_top_xp_players(db, n=10, days=7)
    print(top_xp)
    
    print("\n⏰ Most Active Players (Last 7 days):")
    top_online = get_top_online_players(db, n=10, days=7)
    print(top_online)
    
    print("\n⚔️ Top Killers (Last 7 days):")
    top_killers = get_top_killers(db, n=10, days=7)
    print(top_killers)
    
    # Character-specific analytics
    character_name = "Rollabostx"
    
    print(f"\n📊 Character Summary: {character_name}")
    summary = get_character_summary(db, character_name, days=7)
    if summary:
        for key, value in summary.items():
            print(f"   {key}: {value}")
    
    print(f"\n📈 XP History:")
    history = get_character_xp_history(db, character_name, days=30)
    print(history.head())


def example_4_delta_tracking():
    """Example 4: Delta tracking and efficiency"""
    print("\n" + "="*50)
    print("EXAMPLE 4: Delta Tracking")
    print("="*50)
    
    db = Database('tibia_v2.db')
    
    # Top XP rates
    print("\n🚀 Highest XP/Hour (Last 24h):")
    top_xp_rate = get_top_xp_delta_players(db, n=10, hours=24)
    print(top_xp_rate)
    
    # Character delta summary
    character_name = "Rollabostx"
    print(f"\n🔥 Delta Summary: {character_name}")
    delta = get_character_delta_summary(db, character_name, hours=24)
    if delta:
        for key, value in delta.items():
            print(f"   {key}: {value}")


def example_5_export_data():
    """Example 5: Export analytics to CSV"""
    print("\n" + "="*50)
    print("EXAMPLE 5: Export Data")
    print("="*50)
    
    db = Database('tibia_v2.db')
    
    # Export top players
    print("\n💾 Exporting analytics...")
    
    top_xp = get_top_xp_players(db, n=20, days=7)
    export_to_csv(top_xp, 'top_xp_players.csv')
    
    top_online = get_top_online_players(db, n=20, days=7)
    export_to_csv(top_online, 'top_online_players.csv')
    
    top_xp_rate = get_top_xp_delta_players(db, n=20, hours=24)
    export_to_csv(top_xp_rate, 'top_xp_rates.csv')
    
    print("\n✅ Exports complete!")


def run_all_examples():
    """Run all examples"""
    print("🚀 RINGTS V2 - Example Usage")
    print("Simplified, Reliable Tibia Character Tracking\n")
    
    # Run examples
    example_1_basic_scraping()
    example_2_multiple_characters()
    example_3_analytics()
    example_4_delta_tracking()
    example_5_export_data()
    
    print("\n" + "="*50)
    print("✅ All examples completed!")
    print("="*50)


if __name__ == "__main__":
    # Run individual examples or all
    run_all_examples()
    
    # Or run specific example:
    # example_1_basic_scraping()
    # example_3_analytics()
