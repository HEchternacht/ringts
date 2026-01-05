"""
RINGTS V2 - Tibia Character Tracking System
Simplified, reliable, and straightforward.
"""
from database import Database
from scraper import scrape_character, scrape_status
from data_processor import process_character
from analytics import (get_top_xp_players, get_top_online_players, 
                      get_character_summary, get_character_delta_summary,
                      get_top_xp_delta_players)


def main():
    """Example usage of RINGTS V2"""
    
    # Initialize database
    print("🚀 RINGTS V2 - Tibia Character Tracking System")
    print("=" * 50)
    
    db = Database('tibia_v2.db')
    db.create_tables()
    print("✅ Database initialized")
    
    # Example: Scrape and process a character
    character_name = "Rollabostx"
    
    print(f"\n📡 Scraping {character_name}...")
    tables = scrape_character(character_name)
    
    if tables:
        print(f"✅ Scraped {len(tables)} tables")
        
        # Get status data
        status_data = scrape_status()
        
        # Process the character data
        results = process_character(db, character_name, "Auroria", tables, status_data)
        
        print(f"\n📊 Results:")
        print(f"  • Deaths: {results['deaths']}")
        print(f"  • Kills: {results['kills']}")
        print(f"  • Online times: {results['online_times']}")
        print(f"  • Experiences: {results['experiences']}")
        print(f"  • XP deltas: {results['xp_deltas']}")
        print(f"  • Online deltas: {results['online_deltas']}")
    else:
        print(f"❌ Failed to scrape {character_name}")
    
    # Show some analytics
    print("\n📈 Top Players (Last 7 days):")
    print("-" * 50)
    
    top_xp = get_top_xp_players(db, n=5, days=7)
    if not top_xp.empty:
        print("\n🏆 Top XP Gainers:")
        for _, row in top_xp.iterrows():
            print(f"  {row['name']}: {row['total_xp']:,.0f} XP")
    
    top_online = get_top_online_players(db, n=5, days=7)
    if not top_online.empty:
        print("\n⏰ Most Active:")
        for _, row in top_online.iterrows():
            print(f"  {row['name']}: {row['total_hours']:.1f} hours")
    
    # Character summary
    summary = get_character_summary(db, character_name, days=7)
    if summary:
        print(f"\n📋 Summary for {character_name}:")
        print(f"  Total XP: {summary['total_xp']:,.0f}")
        print(f"  Avg Daily XP: {summary['avg_daily_xp']:,.0f}")
        print(f"  Online Hours: {summary['total_online_hours']:.1f}")
        print(f"  Kills: {summary['total_kills']}")
        print(f"  Deaths: {summary['total_deaths']}")
        print(f"  K/D Ratio: {summary['kd_ratio']:.2f}")
    
    # Delta summary
    delta_summary = get_character_delta_summary(db, character_name, hours=24)
    if delta_summary:
        print(f"\n🔥 Recent Activity (Last 24h):")
        print(f"  XP Gained: {delta_summary['total_xp_delta']:,.0f}")
        print(f"  XP/Hour: {delta_summary['xp_per_hour']:,.0f}")
        print(f"  Online Minutes: {delta_summary['online_delta_minutes']}")
        print(f"  Efficiency: {delta_summary['online_efficiency_pct']:.1f}%")
    
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
