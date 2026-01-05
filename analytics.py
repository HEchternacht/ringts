"""
Analytics and reporting functions for Tibia character data
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, and_, desc

from alchemy import (
    Character, ScrapingSession, CharacterDeath, CharacterKill,
    CharacterOnlineTime, CharacterExperience, DatabaseManager
)

def calculate_xp_growth(db_manager: DatabaseManager, character_id: int, 
                       start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Dict:
    """
    Calculate XP growth for a character in a given period
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        start_date (datetime): Start date (default: 30 days ago)
        end_date (datetime): End date (default: today)
    
    Returns:
        Dict: XP growth statistics
    """
    session = db_manager.get_session()
    
    try:
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
        
        # Get experience records in the date range
        experiences = session.query(CharacterExperience)\
            .filter(
                CharacterExperience.character_id == character_id,
                CharacterExperience.date >= start_date,
                CharacterExperience.date <= end_date
            )\
            .order_by(CharacterExperience.date)\
            .all()
        
        if not experiences:
            return {
                'total_xp': 0,
                'avg_daily_xp': 0,
                'level_gained': 0,
                'days_tracked': 0,
                'start_level': 0,
                'end_level': 0
            }
        
        total_xp = sum(exp.raw_xp_day for exp in experiences)
        level_gained = sum(exp.level_delta for exp in experiences)
        days_tracked = len(experiences)
        avg_daily_xp = total_xp / days_tracked if days_tracked > 0 else 0
        
        start_level = experiences[0].level - experiences[0].level_delta
        end_level = experiences[-1].level
        
        return {
            'total_xp': total_xp,
            'avg_daily_xp': avg_daily_xp,
            'level_gained': level_gained,
            'days_tracked': days_tracked,
            'start_level': start_level,
            'end_level': end_level,
            'period': f"{start_date.date()} to {end_date.date()}"
        }
        
    finally:
        db_manager.close_session(session)

def calculate_death_rate(db_manager: DatabaseManager, character_id: int,
                        days: int = 30) -> Dict:
    """
    Calculate death rate and statistics for a character
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        days (int): Number of days to analyze
    
    Returns:
        Dict: Death statistics
    """
    session = db_manager.get_session()
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        # Get deaths in the period
        deaths = session.query(CharacterDeath)\
            .filter(
                CharacterDeath.character_id == character_id,
                CharacterDeath.death_time >= start_date
            )\
            .order_by(CharacterDeath.death_time.desc())\
            .all()
        
        if not deaths:
            return {
                'total_deaths': 0,
                'deaths_per_day': 0,
                'most_common_killer': None,
                'avg_death_level': 0,
                'period_days': days
            }
        
        # Calculate statistics
        total_deaths = len(deaths)
        deaths_per_day = total_deaths / days
        
        # Most common killer
        killers = [death.killed_by for death in deaths]
        most_common_killer = max(set(killers), key=killers.count) if killers else None
        
        # Average level at death
        avg_death_level = sum(death.level_at_death for death in deaths) / total_deaths
        
        return {
            'total_deaths': total_deaths,
            'deaths_per_day': deaths_per_day,
            'most_common_killer': most_common_killer,
            'avg_death_level': avg_death_level,
            'period_days': days,
            'recent_deaths': [(d.death_time, d.level_at_death, d.killed_by) for d in deaths[:5]]
        }
        
    finally:
        db_manager.close_session(session)

def calculate_kill_rate(db_manager: DatabaseManager, character_id: int,
                       days: int = 30) -> Dict:
    """
    Calculate kill rate and statistics for a character
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        days (int): Number of days to analyze
    
    Returns:
        Dict: Kill statistics
    """
    session = db_manager.get_session()
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        # Get kills in the period
        kills = session.query(CharacterKill)\
            .filter(
                CharacterKill.character_id == character_id,
                CharacterKill.kill_time >= start_date
            )\
            .order_by(CharacterKill.kill_time.desc())\
            .all()
        
        if not kills:
            return {
                'total_kills': 0,
                'kills_per_day': 0,
                'avg_victim_level': 0,
                'highest_level_kill': 0,
                'period_days': days
            }
        
        # Calculate statistics
        total_kills = len(kills)
        kills_per_day = total_kills / days
        
        # Victim level statistics
        victim_levels = [kill.victim_level for kill in kills]
        avg_victim_level = sum(victim_levels) / len(victim_levels)
        highest_level_kill = max(victim_levels)
        
        return {
            'total_kills': total_kills,
            'kills_per_day': kills_per_day,
            'avg_victim_level': avg_victim_level,
            'highest_level_kill': highest_level_kill,
            'period_days': days,
            'recent_kills': [(k.kill_time, k.victim_name, k.victim_level) for k in kills[:5]]
        }
        
    finally:
        db_manager.close_session(session)

def calculate_online_time_stats(db_manager: DatabaseManager, character_id: int,
                               days: int = 30) -> Dict:
    """
    Calculate online time statistics for a character
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_id (int): Character ID
        days (int): Number of days to analyze
    
    Returns:
        Dict: Online time statistics
    """
    session = db_manager.get_session()
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        # Get online time records in the period
        online_times = session.query(CharacterOnlineTime)\
            .filter(
                CharacterOnlineTime.character_id == character_id,
                CharacterOnlineTime.date >= start_date
            )\
            .order_by(CharacterOnlineTime.date.desc())\
            .all()
        
        if not online_times:
            return {
                'total_online_minutes': 0,
                'total_online_hours': 0,
                'avg_daily_minutes': 0,
                'avg_daily_hours': 0,
                'max_daily_minutes': 0,
                'days_played': 0,
                'period_days': days
            }
        
        # Calculate statistics
        total_minutes = sum(ot.online_time_minutes for ot in online_times)
        total_hours = total_minutes / 60
        days_played = len(online_times)
        avg_daily_minutes = total_minutes / days if days > 0 else 0
        avg_daily_hours = avg_daily_minutes / 60
        max_daily_minutes = max(ot.online_time_minutes for ot in online_times)
        
        return {
            'total_online_minutes': total_minutes,
            'total_online_hours': round(total_hours, 2),
            'avg_daily_minutes': round(avg_daily_minutes, 2),
            'avg_daily_hours': round(avg_daily_hours, 2),
            'max_daily_minutes': max_daily_minutes,
            'max_daily_hours': round(max_daily_minutes / 60, 2),
            'days_played': days_played,
            'period_days': days
        }
        
    finally:
        db_manager.close_session(session)

def get_character_summary(db_manager: DatabaseManager, character_name: str) -> Dict:
    """
    Get complete character summary with all statistics
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_name (str): Character name
    
    Returns:
        Dict: Complete character summary
    """
    session = db_manager.get_session()
    
    try:
        # Get character
        character = session.query(Character).filter_by(name=character_name).first()
        if not character:
            return {'error': f"Character '{character_name}' not found"}
        
        # Get basic info
        summary = {
            'character_id': character.id,
            'name': character.name,
            'world': character.world,
            'created_at': character.created_at,
            'last_updated': character.updated_at
        }
        
        # Get statistics for different periods
        periods = [7, 30, 90]
        
        for period in periods:
            period_key = f'{period}d'
            
            summary[f'xp_growth_{period_key}'] = calculate_xp_growth(
                db_manager, character.id, 
                datetime.now() - timedelta(days=period)
            )
            
            summary[f'death_stats_{period_key}'] = calculate_death_rate(
                db_manager, character.id, period
            )
            
            summary[f'kill_stats_{period_key}'] = calculate_kill_rate(
                db_manager, character.id, period
            )
            
            summary[f'online_stats_{period_key}'] = calculate_online_time_stats(
                db_manager, character.id, period
            )
        
        return summary
        
    finally:
        db_manager.close_session(session)

def export_character_data_to_csv(db_manager: DatabaseManager, character_name: str, 
                                output_dir: str = ".") -> Dict[str, str]:
    """
    Export all character data to CSV files
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_name (str): Character name
        output_dir (str): Output directory for CSV files
    
    Returns:
        Dict[str, str]: Dictionary with table names and their CSV file paths
    """
    session = db_manager.get_session()
    
    try:
        # Get character
        character = session.query(Character).filter_by(name=character_name).first()
        if not character:
            return {'error': f"Character '{character_name}' not found"}
        
        file_paths = {}
        
        # Export deaths
        deaths = session.query(CharacterDeath)\
            .filter_by(character_id=character.id)\
            .order_by(CharacterDeath.death_time.desc())\
            .all()
        
        if deaths:
            deaths_data = [
                {
                    'death_time': death.death_time,
                    'level_at_death': death.level_at_death,
                    'killed_by': death.killed_by,
                    'scraping_session_id': death.scraping_session_id
                }
                for death in deaths
            ]
            deaths_df = pd.DataFrame(deaths_data)
            deaths_file = f"{output_dir}/{character_name}_deaths.csv"
            deaths_df.to_csv(deaths_file, index=False)
            file_paths['deaths'] = deaths_file
        
        # Export kills
        kills = session.query(CharacterKill)\
            .filter_by(character_id=character.id)\
            .order_by(CharacterKill.kill_time.desc())\
            .all()
        
        if kills:
            kills_data = [
                {
                    'kill_time': kill.kill_time,
                    'victim_name': kill.victim_name,
                    'victim_level': kill.victim_level,
                    'scraping_session_id': kill.scraping_session_id
                }
                for kill in kills
            ]
            kills_df = pd.DataFrame(kills_data)
            kills_file = f"{output_dir}/{character_name}_kills.csv"
            kills_df.to_csv(kills_file, index=False)
            file_paths['kills'] = kills_file
        
        # Export online times
        online_times = session.query(CharacterOnlineTime)\
            .filter_by(character_id=character.id)\
            .order_by(CharacterOnlineTime.date.desc())\
            .all()
        
        if online_times:
            online_data = [
                {
                    'date': ot.date.date(),
                    'online_time_minutes': ot.online_time_minutes,
                    'online_time_hours': round(ot.online_time_minutes / 60, 2),
                    'scraping_session_id': ot.scraping_session_id
                }
                for ot in online_times
            ]
            online_df = pd.DataFrame(online_data)
            online_file = f"{output_dir}/{character_name}_online_times.csv"
            online_df.to_csv(online_file, index=False)
            file_paths['online_times'] = online_file
        
        # Export experiences
        experiences = session.query(CharacterExperience)\
            .filter_by(character_id=character.id)\
            .order_by(CharacterExperience.date.desc())\
            .all()
        
        if experiences:
            exp_data = [
                {
                    'date': exp.date.date(),
                    'level': exp.level,
                    'level_delta': exp.level_delta,
                    'raw_xp_day': exp.raw_xp_day,
                    'scraping_session_id': exp.scraping_session_id
                }
                for exp in experiences
            ]
            exp_df = pd.DataFrame(exp_data)
            exp_file = f"{output_dir}/{character_name}_experiences.csv"
            exp_df.to_csv(exp_file, index=False)
            file_paths['experiences'] = exp_file
        
        return file_paths
        
    finally:
        db_manager.close_session(session)

def analyze_character_group(db_manager: DatabaseManager, character_names: List[str], 
                           days_back: int = 7) -> Dict:
    """
    Analyze a group of characters over a specific time period with comprehensive metrics
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_names (List[str]): List of character names to analyze
        days_back (int): Number of days to look back (default: 7)
    
    Returns:
        Dict: Comprehensive group analysis with rankings and statistics
    """
    session = db_manager.get_session()
    
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Initialize results structure
        results = {
            'analysis_period': f"{start_date.date()} to {end_date.date()}",
            'days_analyzed': days_back,
            'characters_analyzed': len(character_names),
            'characters': {},
            'daily_metrics': {},
            'rankings': {},
            'group_stats': {}
        }
        
        # Create date range for analysis
        date_range = [start_date.date() + timedelta(days=x) for x in range(days_back)]
        
        # Collect data for each character
        character_data = {}
        valid_characters = []
        
        for char_name in character_names:
            character = session.query(Character).filter_by(name=char_name).first()
            if not character:
                print(f"⚠️ Character '{char_name}' not found in database")
                continue
                
            valid_characters.append(char_name)
            
            # Initialize character data structure
            char_data = {
                'character_id': character.id,
                'world': character.world,
                'daily_data': {}
            }
            
            # Initialize all dates with zeros
            for date in date_range:
                char_data['daily_data'][date] = {
                    'xp': 0,
                    'level': 0,
                    'level_delta': 0,
                    'online_minutes': 0,
                    'deaths': 0,
                    'kills': 0
                }
            
            # Get experience data
            experiences = session.query(CharacterExperience)\
                .filter(
                    CharacterExperience.character_id == character.id,
                    CharacterExperience.date >= start_date,
                    CharacterExperience.date <= end_date
                ).all()
            
            for exp in experiences:
                date_key = exp.date.date()
                if date_key in char_data['daily_data']:
                    char_data['daily_data'][date_key]['xp'] = exp.raw_xp_day
                    char_data['daily_data'][date_key]['level'] = exp.level
                    char_data['daily_data'][date_key]['level_delta'] = exp.level_delta
            
            # Get online time data
            online_times = session.query(CharacterOnlineTime)\
                .filter(
                    CharacterOnlineTime.character_id == character.id,
                    CharacterOnlineTime.date >= start_date,
                    CharacterOnlineTime.date <= end_date
                ).all()
            
            for ot in online_times:
                date_key = ot.date.date()
                if date_key in char_data['daily_data']:
                    char_data['daily_data'][date_key]['online_minutes'] = ot.online_time_minutes
            
            # Get deaths data (count per day)
            deaths = session.query(CharacterDeath)\
                .filter(
                    CharacterDeath.character_id == character.id,
                    CharacterDeath.death_time >= start_date,
                    CharacterDeath.death_time <= end_date
                ).all()
            
            death_counts = {}
            for death in deaths:
                date_key = death.death_time.date()
                death_counts[date_key] = death_counts.get(date_key, 0) + 1
            
            for date_key, count in death_counts.items():
                if date_key in char_data['daily_data']:
                    char_data['daily_data'][date_key]['deaths'] = count
            
            # Get kills data (count per day)
            kills = session.query(CharacterKill)\
                .filter(
                    CharacterKill.character_id == character.id,
                    CharacterKill.kill_time >= start_date,
                    CharacterKill.kill_time <= end_date
                ).all()
            
            kill_counts = {}
            for kill in kills:
                date_key = kill.kill_time.date()
                kill_counts[date_key] = kill_counts.get(date_key, 0) + 1
            
            for date_key, count in kill_counts.items():
                if date_key in char_data['daily_data']:
                    char_data['daily_data'][date_key]['kills'] = count
            
            character_data[char_name] = char_data
        
        # Calculate metrics for each character
        for char_name in valid_characters:
            char_data = character_data[char_name]
            daily_values = list(char_data['daily_data'].values())
            
            # Extract daily metrics
            xp_values = [day['xp'] for day in daily_values]
            online_values = [day['online_minutes'] for day in daily_values]
            death_values = [day['deaths'] for day in daily_values]
            kill_values = [day['kills'] for day in daily_values]
            level_deltas = [day['level_delta'] for day in daily_values]
            
            # Calculate comprehensive statistics
            char_stats = {
                'total_xp': sum(xp_values),
                'avg_xp': sum(xp_values) / len(xp_values),
                'max_xp': max(xp_values),
                'min_xp': min(xp_values),
                'xp_75_percentile': pd.Series(xp_values).quantile(0.75),
                'xp_25_percentile': pd.Series(xp_values).quantile(0.25),
                'xp_std': pd.Series(xp_values).std(),
                
                'total_online_hours': sum(online_values) / 60,
                'avg_online_hours': sum(online_values) / len(online_values) / 60,
                'max_online_hours': max(online_values) / 60,
                'online_75_percentile': pd.Series(online_values).quantile(0.75) / 60,
                'online_consistency': 1 - (pd.Series(online_values).std() / (pd.Series(online_values).mean() + 0.001)),
                
                'total_deaths': sum(death_values),
                'avg_deaths_per_day': sum(death_values) / len(death_values),
                'max_deaths_per_day': max(death_values),
                'death_rate': sum(death_values) / days_back,
                
                'total_kills': sum(kill_values),
                'avg_kills_per_day': sum(kill_values) / len(kill_values),
                'max_kills_per_day': max(kill_values),
                'kill_rate': sum(kill_values) / days_back,
                'kd_ratio': sum(kill_values) / (sum(death_values) + 1),  # +1 to avoid division by zero
                
                'total_levels_gained': sum(level_deltas),
                'avg_levels_per_day': sum(level_deltas) / len(level_deltas),
                'max_levels_per_day': max(level_deltas),
                
                'activity_score': (sum(xp_values) / 1000000) + (sum(online_values) / 60) + sum(kill_values) - sum(death_values),
                'efficiency_score': (sum(xp_values) / (sum(online_values) + 1)) * 60,  # XP per hour
            }
            
            results['characters'][char_name] = {
                'world': char_data['world'],
                'character_id': char_data['character_id'],
                'stats': char_stats,
                'daily_data': char_data['daily_data']
            }
        
        # Calculate daily group metrics across all characters
        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            daily_totals = {
                'total_xp': 0,
                'total_online_hours': 0,
                'total_deaths': 0,
                'total_kills': 0,
                'active_characters': 0
            }
            
            for char_name in valid_characters:
                day_data = character_data[char_name]['daily_data'][date]
                daily_totals['total_xp'] += day_data['xp']
                daily_totals['total_online_hours'] += day_data['online_minutes'] / 60
                daily_totals['total_deaths'] += day_data['deaths']
                daily_totals['total_kills'] += day_data['kills']
                
                # Count as active if had any XP or online time
                if day_data['xp'] > 0 or day_data['online_minutes'] > 0:
                    daily_totals['active_characters'] += 1
            
            results['daily_metrics'][date_str] = daily_totals
        
        # Create rankings for different metrics
        ranking_metrics = [
            ('total_xp', 'Total XP', 'desc'),
            ('avg_xp', 'Average Daily XP', 'desc'),
            ('max_xp', 'Best Single Day XP', 'desc'),
            ('total_online_hours', 'Total Online Time', 'desc'),
            ('avg_online_hours', 'Average Online Time', 'desc'),
            ('online_consistency', 'Online Consistency', 'desc'),
            ('total_levels_gained', 'Levels Gained', 'desc'),
            ('total_kills', 'Total Kills', 'desc'),
            ('kd_ratio', 'K/D Ratio', 'desc'),
            ('total_deaths', 'Total Deaths', 'asc'),  # Lower is better
            ('activity_score', 'Activity Score', 'desc'),
            ('efficiency_score', 'Efficiency (XP/hour)', 'desc'),
        ]
        
        for metric_key, metric_name, order in ranking_metrics:
            rankings = []
            for char_name in valid_characters:
                value = results['characters'][char_name]['stats'][metric_key]
                rankings.append((char_name, value))
            
            # Sort rankings
            reverse_order = (order == 'desc')
            rankings.sort(key=lambda x: x[1], reverse=reverse_order)
            
            results['rankings'][metric_name] = {
                'rankings': rankings,
                'best': rankings[0] if rankings else None,
                'worst': rankings[-1] if rankings else None
            }
        
        # Calculate group statistics
        if valid_characters:
            all_stats = [results['characters'][char]['stats'] for char in valid_characters]
            
            results['group_stats'] = {
                'total_group_xp': sum(stats['total_xp'] for stats in all_stats),
                'avg_character_xp': sum(stats['total_xp'] for stats in all_stats) / len(all_stats),
                'total_group_online_hours': sum(stats['total_online_hours'] for stats in all_stats),
                'total_group_kills': sum(stats['total_kills'] for stats in all_stats),
                'total_group_deaths': sum(stats['total_deaths'] for stats in all_stats),
                'group_kd_ratio': sum(stats['total_kills'] for stats in all_stats) / (sum(stats['total_deaths'] for stats in all_stats) + 1),
                'most_active_character': max(valid_characters, key=lambda x: results['characters'][x]['stats']['activity_score']),
                'most_efficient_character': max(valid_characters, key=lambda x: results['characters'][x]['stats']['efficiency_score']),
            }
        
        return results
        
    finally:
        db_manager.close_session(session)

def print_group_analysis_report(group_analysis: Dict):
    """
    Print a formatted group analysis report
    
    Args:
        group_analysis (Dict): Result from analyze_character_group function
    """
    print(f"\n📊 GROUP ANALYSIS REPORT")
    print("=" * 60)
    print(f"📅 Period: {group_analysis['analysis_period']}")
    print(f"⏰ Days Analyzed: {group_analysis['days_analyzed']}")
    print(f"👥 Characters: {group_analysis['characters_analyzed']}")
    
    if 'group_stats' in group_analysis and group_analysis['group_stats']:
        stats = group_analysis['group_stats']
        print(f"\n🎯 GROUP TOTALS")
        print("-" * 30)
        print(f"💫 Total Group XP: {stats['total_group_xp']:,.0f}")
        print(f"📊 Average per Character: {stats['avg_character_xp']:,.0f} XP")
        print(f"🕐 Total Online Time: {stats['total_group_online_hours']:.1f}h")
        print(f"⚔️ Total Kills: {stats['total_group_kills']}")
        print(f"💀 Total Deaths: {stats['total_group_deaths']}")
        print(f"📈 Group K/D Ratio: {stats['group_kd_ratio']:.2f}")
        print(f"🏆 Most Active: {stats['most_active_character']}")
        print(f"⚡ Most Efficient: {stats['most_efficient_character']}")
    
    # Show top rankings
    print(f"\n🏅 RANKINGS")
    print("-" * 30)
    
    important_rankings = ['Total XP', 'Average Daily XP', 'Total Online Time', 'K/D Ratio', 'Activity Score']
    
    for ranking_name in important_rankings:
        if ranking_name in group_analysis['rankings']:
            ranking_data = group_analysis['rankings'][ranking_name]
            if ranking_data['rankings']:
                best = ranking_data['best']
                worst = ranking_data['worst']
                print(f"\n{ranking_name}:")
                print(f"  🥇 Best: {best[0]} ({best[1]:,.2f})")
                print(f"  🥉 Worst: {worst[0]} ({worst[1]:,.2f})")
                
                # Show top 3 if more than 3 characters
                if len(ranking_data['rankings']) > 3:
                    print("  📊 Top 3:")
                    for i, (name, value) in enumerate(ranking_data['rankings'][:3]):
                        emoji = ["🥇", "🥈", "🥉"][i]
                        print(f"     {emoji} {name}: {value:,.2f}")

def export_group_analysis_to_csv(group_analysis: Dict, output_dir: str = ".") -> str:
    """
    Export group analysis results to CSV files
    
    Args:
        group_analysis (Dict): Group analysis results
        output_dir (str): Output directory for CSV files
    
    Returns:
        str: Path to the main summary CSV file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"group_analysis_{timestamp}"
    
    # Character summary CSV
    char_summary_data = []
    for char_name, char_data in group_analysis['characters'].items():
        stats = char_data['stats']
        char_summary_data.append({
            'character_name': char_name,
            'world': char_data['world'],
            'total_xp': stats['total_xp'],
            'avg_daily_xp': stats['avg_xp'],
            'max_daily_xp': stats['max_xp'],
            'total_online_hours': stats['total_online_hours'],
            'avg_online_hours': stats['avg_online_hours'],
            'total_deaths': stats['total_deaths'],
            'total_kills': stats['total_kills'],
            'kd_ratio': stats['kd_ratio'],
            'levels_gained': stats['total_levels_gained'],
            'activity_score': stats['activity_score'],
            'efficiency_score': stats['efficiency_score']
        })
    
    char_df = pd.DataFrame(char_summary_data)
    char_file = f"{output_dir}/{base_filename}_character_summary.csv"
    char_df.to_csv(char_file, index=False)
    
    # Daily metrics CSV
    daily_data = []
    for date, metrics in group_analysis['daily_metrics'].items():
        daily_data.append({
            'date': date,
            **metrics
        })
    
    daily_df = pd.DataFrame(daily_data)
    daily_file = f"{output_dir}/{base_filename}_daily_metrics.csv"
    daily_df.to_csv(daily_file, index=False)
    
    # Rankings CSV
    rankings_data = []
    for ranking_name, ranking_info in group_analysis['rankings'].items():
        for rank, (char_name, value) in enumerate(ranking_info['rankings'], 1):
            rankings_data.append({
                'metric': ranking_name,
                'rank': rank,
                'character_name': char_name,
                'value': value
            })
    
    rankings_df = pd.DataFrame(rankings_data)
    rankings_file = f"{output_dir}/{base_filename}_rankings.csv"
    rankings_df.to_csv(rankings_file, index=False)
    
    print(f"✅ Group analysis exported to:")
    print(f"  📄 Character Summary: {char_file}")
    print(f"  📅 Daily Metrics: {daily_file}")
    print(f"  🏅 Rankings: {rankings_file}")
    
    return char_file

def print_character_report(db_manager: DatabaseManager, character_name: str):
    """
    Print a formatted character report
    
    Args:
        db_manager (DatabaseManager): Database manager
        character_name (str): Character name
    """
    summary = get_character_summary(db_manager, character_name)
    
    if 'error' in summary:
        print(f"❌ {summary['error']}")
        return
    
    print(f"\n📊 CHARACTER REPORT: {summary['name']}")
    print("=" * 50)
    print(f"🌍 World: {summary['world']}")
    print(f"🆔 Character ID: {summary['character_id']}")
    print(f"📅 First Tracked: {summary['created_at'].date()}")
    print(f"🔄 Last Updated: {summary['last_updated'].date()}")
    
    # 30-day summary
    print(f"\n📈 30-DAY SUMMARY")
    print("-" * 30)
    
    xp_30d = summary['xp_growth_30d']
    print(f"💫 XP Growth: {xp_30d['total_xp']:,} XP ({xp_30d['level_gained']:+d} levels)")
    print(f"📊 Daily Avg XP: {xp_30d['avg_daily_xp']:,.0f}")
    print(f"📏 Level: {xp_30d['start_level']} → {xp_30d['end_level']}")
    
    death_30d = summary['death_stats_30d']
    print(f"💀 Deaths: {death_30d['total_deaths']} ({death_30d['deaths_per_day']:.2f}/day)")
    if death_30d['most_common_killer']:
        print(f"🗡️ Most Common Killer: {death_30d['most_common_killer']}")
    
    kill_30d = summary['kill_stats_30d']
    print(f"⚔️ Kills: {kill_30d['total_kills']} ({kill_30d['kills_per_day']:.2f}/day)")
    print(f"🎯 Avg Victim Level: {kill_30d['avg_victim_level']:.0f}")
    
    online_30d = summary['online_stats_30d']
    print(f"🕐 Online Time: {online_30d['total_online_hours']:.1f}h ({online_30d['avg_daily_hours']:.1f}h/day)")
    print(f"📅 Days Played: {online_30d['days_played']}/{online_30d['period_days']}")
    
    print("\n" + "=" * 50)