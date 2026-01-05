"""
Advanced Analytics Module for Tibia Character Analysis System
Contains modular, one-line functions for specific character analysis tasks
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc
from alchemy import (Character, CharacterExperience, CharacterOnlineTime, CharacterKill, 
                    CharacterDeath, CharacterDeltaExperience, CharacterDeltaOnline)

# ====== RANKING FUNCTIONS ======

def get_top_xp_players(db_manager, n: int = 10, days_back: int = 7) -> pd.DataFrame:
    """Get top N players by XP gain in last N days"""
    cutoff = datetime.now() - timedelta(days=days_back)
    with db_manager.get_session() as session:
        query = session.query(
            Character.name, 
            func.sum(CharacterExperience.experience_gained).label('total_xp')
        ).join(CharacterExperience).filter(
            CharacterExperience.recorded_at >= cutoff
        ).group_by(Character.id).order_by(desc('total_xp')).limit(n)
        return pd.DataFrame(query.all())

def get_bottom_xp_players(db_manager, n: int = 10, days_back: int = 7) -> pd.DataFrame:
    """Get bottom N players by XP gain in last N days"""
    cutoff = datetime.now() - timedelta(days=days_back)
    with db_manager.get_session() as session:
        query = session.query(
            Character.name, 
            func.sum(CharacterExperience.experience_gained).label('total_xp')
        ).join(CharacterExperience).filter(
            CharacterExperience.recorded_at >= cutoff
        ).group_by(Character.id).order_by(asc('total_xp')).limit(n)
        return pd.DataFrame(query.all())

def get_top_online_players(db_manager, n: int = 10, days_back: int = 7) -> pd.DataFrame:
    """Get top N players by online time in last N days"""
    cutoff = datetime.now() - timedelta(days=days_back)
    with db_manager.get_session() as session:
        query = session.query(
            Character.name, 
            func.sum(CharacterOnlineTime.minutes_online).label('total_online')
        ).join(CharacterOnlineTime).filter(
            CharacterOnlineTime.recorded_at >= cutoff
        ).group_by(Character.id).order_by(desc('total_online')).limit(n)
        return pd.DataFrame(query.all())

def get_top_kills_players(db_manager, n: int = 10, days_back: int = 7) -> pd.DataFrame:
    """Get top N players by kills in last N days"""
    cutoff = datetime.now() - timedelta(days=days_back)
    with db_manager.get_session() as session:
        query = session.query(
            Character.name, 
            func.count(CharacterKill.id).label('total_kills')
        ).join(CharacterKill).filter(
            CharacterKill.recorded_at >= cutoff
        ).group_by(Character.id).order_by(desc('total_kills')).limit(n)
        return pd.DataFrame(query.all())

def get_most_deaths_players(db_manager, n: int = 10, days_back: int = 7) -> pd.DataFrame:
    """Get top N players by deaths in last N days"""
    cutoff = datetime.now() - timedelta(days=days_back)
    with db_manager.get_session() as session:
        query = session.query(
            Character.name, 
            func.count(CharacterDeath.id).label('total_deaths')
        ).join(CharacterDeath).filter(
            CharacterDeath.recorded_at >= cutoff
        ).group_by(Character.id).order_by(desc('total_deaths')).limit(n)
        return pd.DataFrame(query.all())

# ====== CHARACTER COMPARISON FUNCTIONS ======

def get_character_percentile(db_manager, character_name: str, metric: str, days_back: int = 7) -> float:
    """Get character's percentile rank for a specific metric"""
    cutoff = datetime.now() - timedelta(days=days_back)
    
    with db_manager.get_session() as session:
        if metric == 'xp':
            all_values = session.query(
                func.sum(CharacterExperience.experience_gained).label('total')
            ).join(Character).join(CharacterExperience).filter(
                CharacterExperience.recorded_at >= cutoff
            ).group_by(Character.id).subquery()
            
            char_value = session.query(
                func.sum(CharacterExperience.experience_gained)
            ).join(Character).filter(
                Character.name == character_name,
                CharacterExperience.recorded_at >= cutoff
            ).scalar() or 0
            
        elif metric == 'online':
            all_values = session.query(
                func.sum(CharacterOnlineTime.minutes_online).label('total')
            ).join(Character).join(CharacterOnlineTime).filter(
                CharacterOnlineTime.recorded_at >= cutoff
            ).group_by(Character.id).subquery()
            
            char_value = session.query(
                func.sum(CharacterOnlineTime.minutes_online)
            ).join(Character).filter(
                Character.name == character_name,
                CharacterOnlineTime.recorded_at >= cutoff
            ).scalar() or 0
            
        # Calculate percentile
        total_count = session.query(func.count(all_values.c.total)).scalar()
        lower_count = session.query(func.count(all_values.c.total)).filter(
            all_values.c.total < char_value
        ).scalar()
        
        return (lower_count / total_count * 100) if total_count > 0 else 0

def is_character_top_performer(db_manager, character_name: str, metric: str, threshold: float = 90) -> bool:
    """Check if character is in top percentage for a metric"""
    return get_character_percentile(db_manager, character_name, metric) >= threshold

def get_character_rank(db_manager, character_name: str, metric: str, days_back: int = 7) -> int:
    """Get character's absolute rank for a specific metric"""
    cutoff = datetime.now() - timedelta(days=days_back)
    
    with db_manager.get_session() as session:
        if metric == 'xp':
            char_value = session.query(
                func.sum(CharacterExperience.experience_gained)
            ).join(Character).filter(
                Character.name == character_name,
                CharacterExperience.recorded_at >= cutoff
            ).scalar() or 0
            
            higher_count = session.query(func.count(Character.id)).join(
                CharacterExperience
            ).filter(
                CharacterExperience.recorded_at >= cutoff
            ).group_by(Character.id).having(
                func.sum(CharacterExperience.experience_gained) > char_value
            ).count()
            
        return higher_count + 1

# ====== VISUALIZATION FUNCTIONS ======

def plot_character_comparison(db_manager, character_name: str, metric: str, days_back: int = 7):
    """Generate comparison plot for character vs all players"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Get all player data
    if metric == 'xp':
        top_players = get_top_xp_players(db_manager, n=20, days_back=days_back)
        ylabel = 'Experience Gained'
        title = f'Experience Ranking - Last {days_back} Days'
    elif metric == 'online':
        top_players = get_top_online_players(db_manager, n=20, days_back=days_back)
        ylabel = 'Minutes Online'
        title = f'Online Time Ranking - Last {days_back} Days'
    
    # Highlight target character
    colors = ['red' if name == character_name else 'skyblue' for name in top_players['name']]
    
    bars = ax.bar(range(len(top_players)), top_players.iloc[:, 1], color=colors)
    ax.set_xlabel('Players')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(range(len(top_players)))
    ax.set_xticklabels(top_players['name'], rotation=45, ha='right')
    
    plt.tight_layout()
    return fig

def plot_character_percentile_radar(db_manager, character_name: str, days_back: int = 7):
    """Create radar chart showing character performance across all metrics"""
    metrics = ['xp', 'online']  # Add more metrics as available
    percentiles = [get_character_percentile(db_manager, character_name, m, days_back) for m in metrics]
    
    # Radar chart setup
    angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
    angles += angles[:1]  # Complete the circle
    percentiles += percentiles[:1]
    
    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(projection='polar'))
    ax.plot(angles, percentiles, 'o-', linewidth=2, color='red')
    ax.fill(angles, percentiles, alpha=0.25, color='red')
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels([m.upper() for m in metrics])
    ax.set_ylim(0, 100)
    ax.set_title(f'{character_name} Performance Percentiles', pad=20)
    
    return fig

# ====== HISTORICAL ANALYSIS FUNCTIONS ======

def get_character_trend(db_manager, character_name: str, metric: str, days_back: int = 30) -> pd.DataFrame:
    """Get daily trend data for character"""
    cutoff = datetime.now() - timedelta(days=days_back)
    
    with db_manager.get_session() as session:
        if metric == 'xp':
            query = session.query(
                func.date(CharacterExperience.recorded_at).label('date'),
                func.sum(CharacterExperience.experience_gained).label('value')
            ).join(Character).filter(
                Character.name == character_name,
                CharacterExperience.recorded_at >= cutoff
            ).group_by(func.date(CharacterExperience.recorded_at))
            
        return pd.DataFrame(query.all())

def calculate_character_consistency(db_manager, character_name: str, metric: str, days_back: int = 30) -> float:
    """Calculate coefficient of variation for character performance"""
    trend_data = get_character_trend(db_manager, character_name, metric, days_back)
    if len(trend_data) < 2:
        return 0
    cv = trend_data['value'].std() / trend_data['value'].mean()
    return 1 / (1 + cv) * 100  # Convert to consistency score (higher = more consistent)

# ====== BULK PROCESSING FUNCTIONS ======

import joblib

def bulk_load_characters(db_manager, character_list: List[str], world: str = "Auroria", 
                        scrape_function=None, load_to_db: bool = True) -> Dict:
    """Bulk process multiple characters with real data scraping"""
    results = {}
    
    for character_name in character_list:
        print(f"\n🔄 Processing {character_name}...")
        
        if scrape_function:
            result = scrape_function(character_name, world, load_to_db)
            results[character_name] = result
            
            # Small delay to be respectful to the server
            import time
            time.sleep(2)
        
    return results

def generate_multi_character_report(db_manager, character_list: List[str], days_back: int = 7) -> pd.DataFrame:
    """Generate comprehensive comparison report for multiple characters"""
    report_data = []
    
    for character_name in character_list:
        char_data = {
            'Character': character_name,
            'XP Percentile': get_character_percentile(db_manager, character_name, 'xp', days_back),
            'Online Percentile': get_character_percentile(db_manager, character_name, 'online', days_back),
            'XP Rank': get_character_rank(db_manager, character_name, 'xp', days_back),
            'Consistency Score': calculate_character_consistency(db_manager, character_name, 'xp', days_back),
            'Top Performer (XP)': is_character_top_performer(db_manager, character_name, 'xp'),
            'Top Performer (Online)': is_character_top_performer(db_manager, character_name, 'online')
        }
        report_data.append(char_data)
    
    return pd.DataFrame(report_data)

# ====== EXPORT FUNCTIONS ======

def export_character_analysis(db_manager, character_name: str, days_back: int = 7, filename: Optional[str] = None):
    """Export comprehensive character analysis to CSV"""
    if not filename:
        filename = f"{character_name}_analysis_{datetime.now().strftime('%Y%m%d')}.csv"
    
    analysis_data = {
        'Character': character_name,
        'Analysis Date': datetime.now().strftime('%Y-%m-%d'),
        'Period (Days)': days_back,
        'XP Percentile': get_character_percentile(db_manager, character_name, 'xp', days_back),
        'Online Percentile': get_character_percentile(db_manager, character_name, 'online', days_back),
        'XP Rank': get_character_rank(db_manager, character_name, 'xp', days_back),
        'Consistency Score': calculate_character_consistency(db_manager, character_name, 'xp', days_back),
        'Is Top XP Performer': is_character_top_performer(db_manager, character_name, 'xp'),
        'Is Top Online Performer': is_character_top_performer(db_manager, character_name, 'online')
    }
    
    df = pd.DataFrame([analysis_data])
    df.to_csv(filename, index=False)
    print(f"📊 Analysis exported to {filename}")
    return filename

# ====== DELTA ANALYSIS FUNCTIONS ======

def get_top_xp_delta_players(db_manager, n: int = 10, hours_back: int = 24) -> pd.DataFrame:
    """Get top N players by XP delta (rate of gain) in last N hours"""
    cutoff = datetime.now() - timedelta(hours=hours_back)
    with db_manager.get_session() as session:
        query = session.query(
            Character.name,
            func.sum(CharacterDeltaExperience.experience_delta).label('total_xp_delta'),
            func.sum(CharacterDeltaExperience.time_delta_minutes).label('total_time_minutes'),
            (func.sum(CharacterDeltaExperience.experience_delta) / 
             func.sum(CharacterDeltaExperience.time_delta_minutes) * 60).label('xp_per_hour')
        ).join(Character).filter(
            CharacterDeltaExperience.status_time_after >= cutoff
        ).group_by(Character.id).order_by(desc('total_xp_delta')).limit(n)
        return pd.DataFrame(query.all())

def get_top_online_delta_players(db_manager, n: int = 10, hours_back: int = 24) -> pd.DataFrame:
    """Get top N players by online time delta in last N hours"""
    cutoff = datetime.now() - timedelta(hours=hours_back)
    with db_manager.get_session() as session:
        query = session.query(
            Character.name,
            func.sum(CharacterDeltaOnline.online_minutes_delta).label('total_online_delta'),
            func.avg(CharacterDeltaOnline.online_minutes_delta / CharacterDeltaOnline.time_delta_minutes).label('efficiency_rate')
        ).join(Character).filter(
            CharacterDeltaOnline.status_time_after >= cutoff
        ).group_by(Character.id).order_by(desc('total_online_delta')).limit(n)
        return pd.DataFrame(query.all())

def get_character_xp_rate(db_manager, character_name: str, hours_back: int = 24) -> float:
    """Get character's XP gain rate (XP per hour) over the specified period"""
    cutoff = datetime.now() - timedelta(hours=hours_back)
    with db_manager.get_session() as session:
        query = session.query(
            func.sum(CharacterDeltaExperience.experience_delta).label('total_xp'),
            func.sum(CharacterDeltaExperience.time_delta_minutes).label('total_minutes')
        ).join(Character).filter(
            Character.name == character_name,
            CharacterDeltaExperience.status_time_after >= cutoff
        ).first()
        
        if query and query.total_xp and query.total_minutes:
            return (query.total_xp / query.total_minutes) * 60  # Convert to per hour
        return 0.0

def get_character_online_efficiency(db_manager, character_name: str, hours_back: int = 24) -> float:
    """Get character's online efficiency (online minutes per real minute) over the specified period"""
    cutoff = datetime.now() - timedelta(hours=hours_back)
    with db_manager.get_session() as session:
        query = session.query(
            func.avg(CharacterDeltaOnline.online_minutes_delta / CharacterDeltaOnline.time_delta_minutes).label('avg_efficiency')
        ).join(Character).filter(
            Character.name == character_name,
            CharacterDeltaOnline.status_time_after >= cutoff
        ).first()
        
        return query.avg_efficiency if query and query.avg_efficiency else 0.0

def get_all_deltas_summary(db_manager, hours_back: int = 24) -> pd.DataFrame:
    """Get comprehensive delta summary for all characters"""
    cutoff = datetime.now() - timedelta(hours=hours_back)
    
    with db_manager.get_session() as session:
        # XP deltas
        xp_deltas = session.query(
            Character.name,
            func.sum(CharacterDeltaExperience.experience_delta).label('total_xp_delta'),
            func.sum(CharacterDeltaExperience.level_delta).label('total_level_delta'),
            func.sum(CharacterDeltaExperience.time_delta_minutes).label('total_time_minutes_xp')
        ).join(Character).filter(
            CharacterDeltaExperience.status_time_after >= cutoff
        ).group_by(Character.id).subquery()
        
        # Online deltas
        online_deltas = session.query(
            Character.name,
            func.sum(CharacterDeltaOnline.online_minutes_delta).label('total_online_delta'),
            func.avg(CharacterDeltaOnline.online_minutes_delta / CharacterDeltaOnline.time_delta_minutes).label('avg_efficiency')
        ).join(Character).filter(
            CharacterDeltaOnline.status_time_after >= cutoff
        ).group_by(Character.id).subquery()
        
        # Combine results
        combined_query = session.query(
            Character.name,
            xp_deltas.c.total_xp_delta,
            xp_deltas.c.total_level_delta,
            xp_deltas.c.total_time_minutes_xp,
            online_deltas.c.total_online_delta,
            online_deltas.c.avg_efficiency
        ).outerjoin(xp_deltas, Character.name == xp_deltas.c.name)\
         .outerjoin(online_deltas, Character.name == online_deltas.c.name)\
         .filter((xp_deltas.c.name.isnot(None)) | (online_deltas.c.name.isnot(None)))
        
        df = pd.DataFrame(combined_query.all())
        
        if not df.empty:
            # Calculate rates
            df['xp_per_hour'] = df.apply(lambda row: 
                (row['total_xp_delta'] / row['total_time_minutes_xp'] * 60) 
                if row['total_xp_delta'] and row['total_time_minutes_xp'] else 0, axis=1)
            
            # Fill NaN values
            df = df.fillna(0)
            
        return df

def plot_character_delta_trends(db_manager, character_name: str, hours_back: int = 48):
    """Plot character's XP and online delta trends over time"""
    cutoff = datetime.now() - timedelta(hours=hours_back)
    
    with db_manager.get_session() as session:
        # Get XP deltas
        xp_query = session.query(
            CharacterDeltaExperience.status_time_after,
            CharacterDeltaExperience.experience_delta,
            (CharacterDeltaExperience.experience_delta / CharacterDeltaExperience.time_delta_minutes * 60).label('xp_per_hour')
        ).join(Character).filter(
            Character.name == character_name,
            CharacterDeltaExperience.status_time_after >= cutoff
        ).order_by(CharacterDeltaExperience.status_time_after)
        
        xp_data = pd.DataFrame(xp_query.all())
        
        # Get online deltas
        online_query = session.query(
            CharacterDeltaOnline.status_time_after,
            CharacterDeltaOnline.online_minutes_delta,
            (CharacterDeltaOnline.online_minutes_delta / CharacterDeltaOnline.time_delta_minutes).label('efficiency_rate')
        ).join(Character).filter(
            Character.name == character_name,
            CharacterDeltaOnline.status_time_after >= cutoff
        ).order_by(CharacterDeltaOnline.status_time_after)
        
        online_data = pd.DataFrame(online_query.all())
        
        # Create plots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        if not xp_data.empty:
            ax1.plot(pd.to_datetime(xp_data['status_time_after']), xp_data['experience_delta'], 'o-', color='blue')
            ax1.set_title(f'{character_name} - XP Deltas')
            ax1.set_ylabel('XP Gained')
            ax1.tick_params(axis='x', rotation=45)
            
            ax2.plot(pd.to_datetime(xp_data['status_time_after']), xp_data['xp_per_hour'], 'o-', color='green')
            ax2.set_title(f'{character_name} - XP Rate')
            ax2.set_ylabel('XP per Hour')
            ax2.tick_params(axis='x', rotation=45)
        else:
            ax1.text(0.5, 0.5, 'No XP Delta Data', ha='center', va='center', transform=ax1.transAxes)
            ax2.text(0.5, 0.5, 'No XP Rate Data', ha='center', va='center', transform=ax2.transAxes)
        
        if not online_data.empty:
            ax3.plot(pd.to_datetime(online_data['status_time_after']), online_data['online_minutes_delta'], 'o-', color='orange')
            ax3.set_title(f'{character_name} - Online Deltas')
            ax3.set_ylabel('Online Minutes Gained')
            ax3.tick_params(axis='x', rotation=45)
            
            ax4.plot(pd.to_datetime(online_data['status_time_after']), online_data['efficiency_rate'], 'o-', color='red')
            ax4.set_title(f'{character_name} - Online Efficiency')
            ax4.set_ylabel('Efficiency Rate')
            ax4.tick_params(axis='x', rotation=45)
        else:
            ax3.text(0.5, 0.5, 'No Online Delta Data', ha='center', va='center', transform=ax3.transAxes)
            ax4.text(0.5, 0.5, 'No Efficiency Data', ha='center', va='center', transform=ax4.transAxes)
        
        plt.tight_layout()
        return fig

print("🔧 Advanced Analytics Module loaded with modular functions!")

# ====== ONE-LINE SHOWCASE FUNCTIONS ======

def show_delta_tables_status(db_manager):
    """One-line: Show status of delta tables"""
    try:
        delta_exp_df = pd.read_sql_table('character_delta_experiences', db_manager.engine)
        delta_online_df = pd.read_sql_table('character_delta_onlines', db_manager.engine)
        print(f"📈 Experience Deltas: {len(delta_exp_df)} records")
        print(f"⏰ Online Deltas: {len(delta_online_df)} records")
        return {'exp_deltas': len(delta_exp_df), 'online_deltas': len(delta_online_df)}
    except Exception as e:
        print(f"⚠️ Delta tables not populated: {e}")
        return {'exp_deltas': 0, 'online_deltas': 0}

def show_portuguese_time_examples():
    """One-line: Show Portuguese time parsing examples"""
    from utils import parse_portuguese_time
    examples = ["hoje às 13:23", "ontem às 09:17", "hoje às 08:45", "anteontem às 15:30"]
    print("⏰ PORTUGUESE TIME PARSING:")
    for time_str in examples:
        parsed = parse_portuguese_time(time_str)
        print(f"   '{time_str}' → {parsed.strftime('%Y-%m-%d %H:%M')}")

def show_delta_leaderboards(db_manager, hours=24):
    """One-line: Show delta-based leaderboards"""
    print(f"🏆 DELTA LEADERBOARDS (Last {hours}h):")
    
    # XP rate leaders
    xp_leaders = get_top_xp_delta_players(db_manager, n=5, hours_back=hours)
    if not xp_leaders.empty:
        print("🥇 XP Rate Leaders:")
        for idx, row in xp_leaders.iterrows():
            print(f"   {idx+1}. {row['name']} - {row.get('xp_per_hour', 0):,.0f} XP/h")
    
    # Online efficiency leaders
    online_leaders = get_top_online_delta_players(db_manager, n=5, hours_back=hours)
    if not online_leaders.empty:
        print("⏰ Online Efficiency Leaders:")
        for idx, row in online_leaders.iterrows():
            eff = row.get('efficiency_rate', 0)
            print(f"   {idx+1}. {row['name']} - {eff:.3f} efficiency")

def show_character_delta_summary(db_manager, character_name, hours=24):
    """One-line: Show character delta summary"""
    xp_rate = get_character_xp_rate(db_manager, character_name, hours)
    online_eff = get_character_online_efficiency(db_manager, character_name, hours)
    print(f"🎯 {character_name} DELTA SUMMARY ({hours}h):")
    print(f"   📈 XP Rate: {xp_rate:,.0f} XP/hour")
    print(f"   ⏰ Online Efficiency: {online_eff:.3f}")
    return {'xp_rate': xp_rate, 'online_efficiency': online_eff}

def show_all_delta_trends(db_manager, character_name, hours=48):
    """One-line: Show character delta trends"""
    try:
        fig = plot_character_delta_trends(db_manager, character_name, hours)
        if fig:
            plt.show()
            print(f"✅ Delta trends displayed for {character_name}")
            return True
        else:
            print(f"⚠️ No trend data for {character_name}")
            return False
    except Exception as e:
        print(f"❌ Error showing trends: {e}")
        return False

def show_delta_analytics_summary(db_manager, hours=48):
    """One-line: Complete delta analytics summary"""
    print(f"📊 COMPLETE DELTA ANALYTICS SUMMARY ({hours}h)")
    print("=" * 60)
    
    # Status
    status = show_delta_tables_status(db_manager)
    
    # Leaderboards
    show_delta_leaderboards(db_manager, hours)
    
    # Comprehensive summary
    try:
        delta_summary = get_all_deltas_summary(db_manager, hours)
        if not delta_summary.empty:
            print(f"\n📋 COMPREHENSIVE DATA:")
            print(f"   Characters with deltas: {len(delta_summary)}")
            print(f"   Average XP/hour: {delta_summary['xp_per_hour'].mean():,.0f}")
            print(f"   Average efficiency: {delta_summary['avg_efficiency'].mean():.3f}")
        else:
            print("\n📋 No comprehensive delta data yet")
    except Exception as e:
        print(f"\n❌ Error in comprehensive summary: {e}")
    
    return status

# ====== CORE DELTA FUNCTIONS ======

def get_top_xp_delta_players(db_manager, n=10, hours_back=24):
    """Get top players by XP rate in last N hours"""
    try:
        query = """
        SELECT 
            c.name,
            AVG(cd.experience_gained) as avg_xp_gain,
            AVG(cd.hours_elapsed) as avg_hours,
            AVG(cd.experience_gained / NULLIF(cd.hours_elapsed, 0)) as xp_per_hour,
            COUNT(cd.id) as delta_count
        FROM character_delta_experiences cd
        JOIN characters c ON cd.character_id = c.id
        WHERE cd.timestamp >= datetime('now', '-{} hours')
            AND cd.hours_elapsed > 0
            AND cd.experience_gained > 0
        GROUP BY c.id, c.name
        HAVING COUNT(cd.id) >= 2
        ORDER BY xp_per_hour DESC
        LIMIT ?
        """.format(hours_back)
        
        result = pd.read_sql(query, db_manager.engine, params=[n])
        return result
    except Exception as e:
        print(f"⚠️ Error getting XP leaders: {e}")
        return pd.DataFrame()

def get_top_online_delta_players(db_manager, n=10, hours_back=24):
    """Get top players by online efficiency"""
    try:
        query = """
        SELECT 
            c.name,
            AVG(cd.efficiency_rate) as efficiency_rate,
            AVG(cd.online_minutes_gained) as avg_online_gain,
            COUNT(cd.id) as delta_count
        FROM character_delta_onlines cd
        JOIN characters c ON cd.character_id = c.id
        WHERE cd.timestamp >= datetime('now', '-{} hours')
            AND cd.efficiency_rate > 0
        GROUP BY c.id, c.name
        HAVING COUNT(cd.id) >= 2
        ORDER BY efficiency_rate DESC
        LIMIT ?
        """.format(hours_back)
        
        result = pd.read_sql(query, db_manager.engine, params=[n])
        return result
    except Exception as e:
        print(f"⚠️ Error getting online leaders: {e}")
        return pd.DataFrame()

def get_character_xp_rate(db_manager, character_name, hours=24):
    """Get character's XP rate in last N hours"""
    try:
        query = """
        SELECT AVG(cd.experience_gained / NULLIF(cd.hours_elapsed, 0)) as xp_rate
        FROM character_delta_experiences cd
        JOIN characters c ON cd.character_id = c.id
        WHERE c.name = ?
            AND cd.timestamp >= datetime('now', '-{} hours')
            AND cd.hours_elapsed > 0
            AND cd.experience_gained > 0
        """.format(hours)
        
        result = pd.read_sql(query, db_manager.engine, params=[character_name])
        return result.iloc[0]['xp_rate'] if not result.empty else 0
    except Exception as e:
        print(f"⚠️ Error getting XP rate for {character_name}: {e}")
        return 0

def get_character_online_efficiency(db_manager, character_name, hours=24):
    """Get character's online efficiency in last N hours"""
    try:
        query = """
        SELECT AVG(cd.efficiency_rate) as efficiency
        FROM character_delta_onlines cd
        JOIN characters c ON cd.character_id = c.id
        WHERE c.name = ?
            AND cd.timestamp >= datetime('now', '-{} hours')
            AND cd.efficiency_rate > 0
        """.format(hours)
        
        result = pd.read_sql(query, db_manager.engine, params=[character_name])
        return result.iloc[0]['efficiency'] if not result.empty else 0
    except Exception as e:
        print(f"⚠️ Error getting efficiency for {character_name}: {e}")
        return 0

def get_all_deltas_summary(db_manager, hours=24):
    """Get comprehensive delta summary for all characters"""
    try:
        query = """
        SELECT 
            c.name,
            AVG(xp.experience_gained / NULLIF(xp.hours_elapsed, 0)) as xp_per_hour,
            AVG(ol.efficiency_rate) as avg_efficiency,
            COUNT(DISTINCT xp.id) as xp_deltas,
            COUNT(DISTINCT ol.id) as online_deltas
        FROM characters c
        LEFT JOIN character_delta_experiences xp ON c.id = xp.character_id 
            AND xp.timestamp >= datetime('now', '-{} hours')
            AND xp.hours_elapsed > 0
            AND xp.experience_gained > 0
        LEFT JOIN character_delta_onlines ol ON c.id = ol.character_id 
            AND ol.timestamp >= datetime('now', '-{} hours')
            AND ol.efficiency_rate > 0
        WHERE (xp.id IS NOT NULL OR ol.id IS NOT NULL)
        GROUP BY c.id, c.name
        HAVING (COUNT(DISTINCT xp.id) > 0 OR COUNT(DISTINCT ol.id) > 0)
        ORDER BY xp_per_hour DESC
        """.format(hours)
        
        result = pd.read_sql(query, db_manager.engine)
        return result
    except Exception as e:
        print(f"⚠️ Error getting comprehensive summary: {e}")
        return pd.DataFrame()

def plot_character_delta_trends(db_manager, character_name, hours=48):
    """Plot character's delta trends over time"""
    try:
        # Get XP deltas
        xp_query = """
        SELECT 
            cd.timestamp,
            cd.experience_gained / NULLIF(cd.hours_elapsed, 0) as xp_rate,
            cd.experience_gained,
            cd.hours_elapsed
        FROM character_delta_experiences cd
        JOIN characters c ON cd.character_id = c.id
        WHERE c.name = ?
            AND cd.timestamp >= datetime('now', '-{} hours')
            AND cd.hours_elapsed > 0
        ORDER BY cd.timestamp
        """.format(hours)
        
        xp_data = pd.read_sql(xp_query, db_manager.engine, params=[character_name])
        
        # Get online deltas
        online_query = """
        SELECT 
            cd.timestamp,
            cd.efficiency_rate,
            cd.online_minutes_gained
        FROM character_delta_onlines cd
        JOIN characters c ON cd.character_id = c.id
        WHERE c.name = ?
            AND cd.timestamp >= datetime('now', '-{} hours')
        ORDER BY cd.timestamp
        """.format(hours)
        
        online_data = pd.read_sql(online_query, db_manager.engine, params=[character_name])
        
        if xp_data.empty and online_data.empty:
            print(f"⚠️ No delta data found for {character_name}")
            return None
        
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))
        fig.suptitle(f'{character_name} - Delta Trends ({hours}h)', fontsize=14, fontweight='bold')
        
        # XP Rate trend
        if not xp_data.empty:
            xp_data['timestamp'] = pd.to_datetime(xp_data['timestamp'])
            axes[0].plot(xp_data['timestamp'], xp_data['xp_rate'], marker='o', linewidth=2, markersize=6)
            axes[0].set_title('XP Rate Over Time', fontweight='bold')
            axes[0].set_ylabel('XP/Hour')
            axes[0].grid(True, alpha=0.3)
        else:
            axes[0].text(0.5, 0.5, 'No XP delta data', ha='center', va='center', transform=axes[0].transAxes)
        
        # Online Efficiency trend
        if not online_data.empty:
            online_data['timestamp'] = pd.to_datetime(online_data['timestamp'])
            axes[1].plot(online_data['timestamp'], online_data['efficiency_rate'], 
                        marker='s', color='orange', linewidth=2, markersize=6)
            axes[1].set_title('Online Efficiency Over Time', fontweight='bold')
            axes[1].set_ylabel('Efficiency Rate')
            axes[1].grid(True, alpha=0.3)
        else:
            axes[1].text(0.5, 0.5, 'No online delta data', ha='center', va='center', transform=axes[1].transAxes)
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        print(f"❌ Error creating trends plot: {e}")
        return None