"""
Analytics functions - simple queries for character analysis.
"""
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import func, desc
from database import (Character, CharacterExperience, CharacterOnlineTime, 
                     CharacterDeath, CharacterKill, 
                     CharacterDeltaExperience, CharacterDeltaOnline)


def get_top_xp_players(db, n=10, days=7):
    """Get top N players by total XP gain in last N days"""
    cutoff = datetime.now() - timedelta(days=days)
    
    with db.session() as session:
        results = session.query(
            Character.name,
            func.sum(CharacterExperience.raw_xp_day).label('total_xp')
        ).join(CharacterExperience).filter(
            CharacterExperience.date >= cutoff
        ).group_by(Character.id).order_by(desc('total_xp')).limit(n).all()
        
        return pd.DataFrame(results, columns=['name', 'total_xp'])


def get_top_online_players(db, n=10, days=7):
    """Get top N players by total online time in last N days"""
    cutoff = datetime.now() - timedelta(days=days)
    
    with db.session() as session:
        results = session.query(
            Character.name,
            func.sum(CharacterOnlineTime.online_time_minutes).label('total_minutes')
        ).join(CharacterOnlineTime).filter(
            CharacterOnlineTime.date >= cutoff
        ).group_by(Character.id).order_by(desc('total_minutes')).limit(n).all()
        
        df = pd.DataFrame(results, columns=['name', 'total_minutes'])
        df['total_hours'] = df['total_minutes'] / 60
        return df


def get_top_killers(db, n=10, days=7):
    """Get top N players by number of kills in last N days"""
    cutoff = datetime.now() - timedelta(days=days)
    
    with db.session() as session:
        results = session.query(
            Character.name,
            func.count(CharacterKill.id).label('kills')
        ).join(CharacterKill).filter(
            CharacterKill.kill_time >= cutoff
        ).group_by(Character.id).order_by(desc('kills')).limit(n).all()
        
        return pd.DataFrame(results, columns=['name', 'kills'])


def get_most_deaths(db, n=10, days=7):
    """Get players with most deaths in last N days"""
    cutoff = datetime.now() - timedelta(days=days)
    
    with db.session() as session:
        results = session.query(
            Character.name,
            func.count(CharacterDeath.id).label('deaths')
        ).join(CharacterDeath).filter(
            CharacterDeath.death_time >= cutoff
        ).group_by(Character.id).order_by(desc('deaths')).limit(n).all()
        
        return pd.DataFrame(results, columns=['name', 'deaths'])


def get_character_summary(db, character_name, days=7):
    """Get comprehensive summary for a character"""
    cutoff = datetime.now() - timedelta(days=days)
    
    with db.session() as session:
        char = session.query(Character).filter_by(name=character_name).first()
        
        if not char:
            return None
        
        # XP stats
        xp_total = session.query(func.sum(CharacterExperience.raw_xp_day)).filter(
            CharacterExperience.character_id == char.id,
            CharacterExperience.date >= cutoff
        ).scalar() or 0
        
        # Online stats
        online_total = session.query(func.sum(CharacterOnlineTime.online_time_minutes)).filter(
            CharacterOnlineTime.character_id == char.id,
            CharacterOnlineTime.date >= cutoff
        ).scalar() or 0
        
        # Kill stats
        kills_total = session.query(func.count(CharacterKill.id)).filter(
            CharacterKill.character_id == char.id,
            CharacterKill.kill_time >= cutoff
        ).scalar() or 0
        
        # Death stats
        deaths_total = session.query(func.count(CharacterDeath.id)).filter(
            CharacterDeath.character_id == char.id,
            CharacterDeath.death_time >= cutoff
        ).scalar() or 0
        
        return {
            'name': character_name,
            'world': char.world,
            'period_days': days,
            'total_xp': xp_total,
            'avg_daily_xp': xp_total / days if days > 0 else 0,
            'total_online_minutes': online_total,
            'total_online_hours': online_total / 60,
            'avg_daily_hours': online_total / 60 / days if days > 0 else 0,
            'total_kills': kills_total,
            'total_deaths': deaths_total,
            'kd_ratio': kills_total / deaths_total if deaths_total > 0 else kills_total
        }


def get_top_xp_delta_players(db, n=10, hours=24):
    """Get top N players by XP gain rate (XP per hour)"""
    cutoff = datetime.now() - timedelta(hours=hours)
    
    with db.session() as session:
        results = session.query(
            Character.name,
            func.sum(CharacterDeltaExperience.experience_delta).label('total_xp'),
            func.sum(CharacterDeltaExperience.time_delta_minutes).label('total_minutes'),
            (func.sum(CharacterDeltaExperience.experience_delta) / 
             func.sum(CharacterDeltaExperience.time_delta_minutes) * 60).label('xp_per_hour')
        ).join(CharacterDeltaExperience).filter(
            CharacterDeltaExperience.time_after >= cutoff
        ).group_by(Character.id).order_by(desc('xp_per_hour')).limit(n).all()
        
        return pd.DataFrame(results, columns=['name', 'total_xp', 'total_minutes', 'xp_per_hour'])


def get_top_online_delta_players(db, n=10, hours=24):
    """Get top N players by online time efficiency"""
    cutoff = datetime.now() - timedelta(hours=hours)
    
    with db.session() as session:
        results = session.query(
            Character.name,
            func.sum(CharacterDeltaOnline.online_minutes_delta).label('online_delta'),
            func.sum(CharacterDeltaOnline.time_delta_minutes).label('time_delta'),
            (func.sum(CharacterDeltaOnline.online_minutes_delta) / 
             func.sum(CharacterDeltaOnline.time_delta_minutes) * 100).label('efficiency_pct')
        ).join(CharacterDeltaOnline).filter(
            CharacterDeltaOnline.time_after >= cutoff
        ).group_by(Character.id).order_by(desc('efficiency_pct')).limit(n).all()
        
        return pd.DataFrame(results, columns=['name', 'online_delta', 'time_delta', 'efficiency_pct'])


def get_character_xp_history(db, character_name, days=30):
    """Get daily XP history for a character"""
    cutoff = datetime.now() - timedelta(days=days)
    
    with db.session() as session:
        char = session.query(Character).filter_by(name=character_name).first()
        
        if not char:
            return pd.DataFrame()
        
        results = session.query(
            CharacterExperience.date,
            CharacterExperience.level,
            CharacterExperience.raw_xp_day
        ).filter(
            CharacterExperience.character_id == char.id,
            CharacterExperience.date >= cutoff
        ).order_by(CharacterExperience.date).all()
        
        return pd.DataFrame(results, columns=['date', 'level', 'xp'])


def get_character_online_history(db, character_name, days=30):
    """Get daily online time history for a character"""
    cutoff = datetime.now() - timedelta(days=days)
    
    with db.session() as session:
        char = session.query(Character).filter_by(name=character_name).first()
        
        if not char:
            return pd.DataFrame()
        
        results = session.query(
            CharacterOnlineTime.date,
            CharacterOnlineTime.online_time_minutes
        ).filter(
            CharacterOnlineTime.character_id == char.id,
            CharacterOnlineTime.date >= cutoff
        ).order_by(CharacterOnlineTime.date).all()
        
        df = pd.DataFrame(results, columns=['date', 'minutes'])
        df['hours'] = df['minutes'] / 60
        return df


def get_character_delta_summary(db, character_name, hours=24):
    """Get delta summary for a character"""
    cutoff = datetime.now() - timedelta(hours=hours)
    
    with db.session() as session:
        char = session.query(Character).filter_by(name=character_name).first()
        
        if not char:
            return None
        
        # XP delta stats
        xp_stats = session.query(
            func.sum(CharacterDeltaExperience.experience_delta).label('total_xp'),
            func.sum(CharacterDeltaExperience.level_delta).label('total_levels'),
            func.sum(CharacterDeltaExperience.time_delta_minutes).label('total_minutes')
        ).filter(
            CharacterDeltaExperience.character_id == char.id,
            CharacterDeltaExperience.time_after >= cutoff
        ).first()
        
        # Online delta stats
        online_stats = session.query(
            func.sum(CharacterDeltaOnline.online_minutes_delta).label('online_delta'),
            func.sum(CharacterDeltaOnline.time_delta_minutes).label('time_delta')
        ).filter(
            CharacterDeltaOnline.character_id == char.id,
            CharacterDeltaOnline.time_after >= cutoff
        ).first()
        
        xp_total = xp_stats.total_xp or 0
        xp_minutes = xp_stats.total_minutes or 0
        online_delta = online_stats.online_delta or 0
        time_delta = online_stats.time_delta or 0
        
        return {
            'name': character_name,
            'period_hours': hours,
            'total_xp_delta': xp_total,
            'total_level_delta': xp_stats.total_levels or 0,
            'xp_per_hour': (xp_total / xp_minutes * 60) if xp_minutes > 0 else 0,
            'online_delta_minutes': online_delta,
            'online_efficiency_pct': (xp_total / time_delta * 100) if time_delta > 0 else 0
        }


def report_char_between(db, char_name, timedate_begin, timedate_end):
    """
    Get all deltas between timedate_begin and timedate_end.
    Returns simple lists ready to plot.
    
    Args:
        db: Database instance
        char_name: Character name
        timedate_begin: Start datetime
        timedate_end: End datetime
    
    Returns:
        tuple: (online_deltas, exp_deltas, time_after_online, time_after_exp)
    """
    with db.session() as session:
        char = session.query(Character).filter_by(name=char_name).first()
        
        if not char:
            print(f"Character {char_name} not found.")
            return [], [], [], []
        
        # Get experience deltas where time_after < timedate_end AND time_before > timedate_begin
        exp_results = session.query(
            CharacterDeltaExperience.time_after,
            CharacterDeltaExperience.experience_delta
        ).filter(
            CharacterDeltaExperience.character_id == char.id,
            CharacterDeltaExperience.time_after < timedate_end,
            CharacterDeltaExperience.time_before > timedate_begin
        ).order_by(CharacterDeltaExperience.time_after).all()
        
        # Get online deltas where time_after < timedate_end AND time_before > timedate_begin
        online_results = session.query(
            CharacterDeltaOnline.time_after,
            CharacterDeltaOnline.online_minutes_delta
        ).filter(
            CharacterDeltaOnline.character_id == char.id,
            CharacterDeltaOnline.time_after < timedate_end,
            CharacterDeltaOnline.time_before > timedate_begin
        ).order_by(CharacterDeltaOnline.time_after).all()
        
        # Extract lists for plotting
        time_after_exp = [row.time_after for row in exp_results]
        exp_deltas = [row.experience_delta for row in exp_results]
        
        time_after_online = [row.time_after for row in online_results]
        online_deltas = [row.online_minutes_delta for row in online_results]
        
        return online_deltas, exp_deltas, time_after_online, time_after_exp


def export_to_csv(df, filename):
    """Export dataframe to CSV"""
    df.to_csv(filename, index=False)
    print(f"📊 Exported to {filename}")
