"""
SQLAlchemy-based Database class replacement for fastapi_app.py
This maintains the same interface as the original CSV-based Database class.
"""
import threading
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import json
import os
from database_models import (
    DatabaseManager, Player, Delta, VIP, VIPData, VIPDelta, 
    StatusData, ScrapingConfig
)
from sqlalchemy import desc, and_, func
from sqlalchemy.exc import IntegrityError


class SQLAlchemyDatabase:
    """Database abstraction layer using SQLAlchemy."""
    
    def __init__(self, folder=None):
        if folder is None:
            folder = "var/data"
        
        self.folder = folder
        self.db_path = f"{folder}/ringts.db"
        self.status_data_file = f"{folder}/status_data.json"  # Keep JSON for now
        self.lock = threading.Lock()
        self.reset_done_today = False
        
        # Create folder if it doesn't exist
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        # Initialize SQLAlchemy database
        self.db_manager = DatabaseManager(db_path=self.db_path)
        
        # Initialize configurations
        self._initialize_scraping_config()
        
    def _get_session(self):
        """Get a new database session."""
        return self.db_manager.get_session()
    
    def _initialize_scraping_config(self):
        """Initialize default scraping configuration."""
        session = self._get_session()
        try:
            config = session.query(ScrapingConfig).first()
            if not config:
                default_config = [
                    {
                        'world': os.environ.get('DEFAULT_WORLD', 'Auroria'),
                        'guilds': [os.environ.get('DEFAULT_GUILD', 'Ascended Auroria')]
                    }
                ]
                config = ScrapingConfig(config=default_config)
                session.add(config)
                session.commit()
                print(f"Initialized scraping config with default")
        finally:
            session.close()
    
    def get_exps(self) -> pd.DataFrame:
        """Get all player experience data as DataFrame."""
        with self.lock:
            session = self._get_session()
            try:
                players = session.query(Player).all()
                if not players:
                    return pd.DataFrame(columns=['name', 'exp', 'last update', 'world', 'guild'])
                
                data = [{
                    'name': p.name,
                    'exp': p.exp,
                    'last update': p.last_update,
                    'world': p.world,
                    'guild': p.guild
                } for p in players]
                
                return pd.DataFrame(data)
            finally:
                session.close()
    
    def get_deltas(self) -> pd.DataFrame:
        """Get all deltas as DataFrame."""
        with self.lock:
            session = self._get_session()
            try:
                deltas = session.query(Delta).order_by(Delta.update_time).all()
                if not deltas:
                    return pd.DataFrame(columns=['name', 'deltaexp', 'update time', 'world', 'guild'])
                
                data = [{
                    'name': d.name,
                    'deltaexp': d.deltaexp,
                    'update time': d.update_time,
                    'world': d.world,
                    'guild': d.guild
                } for d in deltas]
                
                return pd.DataFrame(data)
            finally:
                session.close()
    
    def get_status_data(self):
        """Get status data (still using JSON file for now)."""
        with self.lock:
            try:
                with open(self.status_data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except FileNotFoundError:
                return None
    
    def save_status_data(self, data):
        """Save status data (still using JSON file for now)."""
        with self.lock:
            with open(self.status_data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
    
    def get_scraping_config(self):
        """Get scraping configuration."""
        session = self._get_session()
        try:
            config = session.query(ScrapingConfig).first()
            if config:
                return config.config
            return [{'world': 'Auroria', 'guilds': ['Ascended Auroria']}]
        finally:
            session.close()
    
    def save_scraping_config(self, config):
        """Save scraping configuration."""
        session = self._get_session()
        try:
            scraping_config = session.query(ScrapingConfig).first()
            if scraping_config:
                scraping_config.config = config
                scraping_config.updated_at = datetime.utcnow()
            else:
                scraping_config = ScrapingConfig(config=config)
                session.add(scraping_config)
            session.commit()
        finally:
            session.close()
    
    def get_vips(self) -> List[Dict]:
        """Get all VIPs."""
        session = self._get_session()
        try:
            vips = session.query(VIP).all()
            return [{'name': v.name, 'world': v.world} for v in vips]
        finally:
            session.close()
    
    def add_vip(self, name: str, world: str) -> bool:
        """Add a VIP player."""
        session = self._get_session()
        try:
            # Check if already exists
            existing = session.query(VIP).filter_by(name=name, world=world).first()
            if existing:
                return False
            
            vip = VIP(name=name, world=world)
            session.add(vip)
            session.commit()
            return True
        except IntegrityError:
            session.rollback()
            return False
        finally:
            session.close()
    
    def remove_vip(self, name: str, world: str) -> bool:
        """Remove a VIP player."""
        session = self._get_session()
        try:
            vip = session.query(VIP).filter_by(name=name, world=world).first()
            if not vip:
                return False
            
            session.delete(vip)
            session.commit()
            return True
        finally:
            session.close()
    
    def get_vipsdata(self) -> pd.DataFrame:
        """Get VIP data."""
        session = self._get_session()
        try:
            vip_data = session.query(VIPData).all()
            if not vip_data:
                return pd.DataFrame(columns=['name', 'world', 'today_exp', 'today_online'])
            
            data = [{
                'name': v.name,
                'world': v.world,
                'today_exp': v.today_exp,
                'today_online': v.today_online
            } for v in vip_data]
            
            return pd.DataFrame(data)
        finally:
            session.close()
    
    def get_deltavip(self) -> pd.DataFrame:
        """Get VIP deltas."""
        session = self._get_session()
        try:
            vip_deltas = session.query(VIPDelta).order_by(VIPDelta.update_time).all()
            if not vip_deltas:
                return pd.DataFrame(columns=['name', 'world', 'date', 'delta_exp', 'delta_online', 'update_time'])
            
            data = [{
                'name': v.name,
                'world': v.world,
                'date': v.date,
                'delta_exp': v.delta_exp,
                'delta_online': v.delta_online,
                'update_time': v.update_time
            } for v in vip_deltas]
            
            return pd.DataFrame(data)
        finally:
            session.close()
    
    def update_vipdata(self, name: str, world: str, today_exp: int, today_online: float):
        """Update VIP data."""
        session = self._get_session()
        try:
            vip_data = session.query(VIPData).filter_by(name=name, world=world).first()
            if vip_data:
                vip_data.today_exp = today_exp
                vip_data.today_online = today_online
            else:
                vip_data = VIPData(
                    name=name,
                    world=world,
                    today_exp=today_exp,
                    today_online=today_online
                )
                session.add(vip_data)
            session.commit()
        finally:
            session.close()
    
    def add_vip_delta(self, name: str, world: str, date: str, delta_exp: int, 
                      delta_online: float, update_time: datetime):
        """Add VIP delta."""
        session = self._get_session()
        try:
            vip_delta = VIPDelta(
                name=name,
                world=world,
                date=date,
                delta_exp=delta_exp,
                delta_online=delta_online,
                update_time=update_time
            )
            session.add(vip_delta)
            session.commit()
            print(f"VIP delta: {name} ({world}) +{delta_exp} exp, +{delta_online} online")
        finally:
            session.close()
    
    def load(self, folder=None):
        """Initialize database (compatibility method)."""
        if folder is None:
            folder = "/var/data"
        if folder:
            self.folder = folder
            self.db_path = f"{folder}/ringts.db"

            #check if ringts.db exists, if not, run migration
            if not os.path.exists(self.db_path):
                from migrate_to_sqlite import migrate_csvs_to_sqlite
                migrate_csvs_to_sqlite(folder)

            
            self.db_manager = DatabaseManager(db_path=self.db_path)
        
        # Get counts for logging
        session = self._get_session()
        try:
            player_count = session.query(func.count(Player.id)).scalar()
            delta_count = session.query(func.count(Delta.id)).scalar()
            print(f"Database initialized: {player_count} players, {delta_count} deltas")
        finally:
            session.close()
    
    def save(self):
        """Save database (compatibility method - SQLAlchemy auto-commits)."""
        print("Database persisted to SQLite")
    
    def update(self, df: pd.DataFrame, update_time: datetime):
        """Update player EXP data and record deltas."""
        # Import here to avoid circular dependency
        import queue as q
        
        with self.lock:
            session = self._get_session()
            try:
                # Get previous update time from deltas
                last_delta = session.query(Delta).filter(
                    Delta.update_time < update_time
                ).order_by(desc(Delta.update_time)).first()
                
                prev_update_time = last_delta.update_time if last_delta else update_time
                
                # Get existing players as dict
                players = session.query(Player).all()
                player_dict = {(p.name, p.world, p.guild): p for p in players}
                
                # Import delta_queue and log_console from fastapi_app
                from fastapi_app import delta_queue, log_console, clean_memory
                
                # Process updates
                for row in df.itertuples(index=False):
                    name = row.name
                    exp = int(row.exp)
                    last_update = getattr(row, 'last_update', getattr(row, '_2', datetime.utcnow()))
                    world = getattr(row, 'world', os.environ.get('DEFAULT_WORLD', 'Auroria'))
                    guild = getattr(row, 'guild', os.environ.get('DEFAULT_GUILD', 'Ascended Auroria'))
                    
                    player_key = (name, world, guild)
                    
                    if player_key in player_dict:
                        # Update existing player
                        player = player_dict[player_key]
                        prev_exp = player.exp
                        deltaexp = exp - prev_exp
                        
                        if deltaexp != 0:
                            # Add delta
                            delta = Delta(
                                name=name,
                                deltaexp=deltaexp,
                                update_time=update_time,
                                world=world,
                                guild=guild
                            )
                            try:
                                session.add(delta)
                                log_console(f"EXP gain: {name} +{deltaexp} ({world} - {guild})")
                            except IntegrityError:
                                session.rollback()
                                # Update existing delta
                                existing_delta = session.query(Delta).filter_by(
                                    name=name, update_time=update_time
                                ).first()
                                if existing_delta:
                                    existing_delta.deltaexp = deltaexp
                                    log_console(f"Updated duplicate for {name} at {update_time}", "INFO")
                            
                            delta_queue.put({
                                'name': name,
                                'deltaexp': int(deltaexp),
                                'update_time': update_time.isoformat(),
                                'prev_update_time': prev_update_time.isoformat(),
                                'world': world,
                                'guild': guild
                            })
                        
                        # Update player exp
                        player.exp = exp
                        player.last_update = last_update
                    else:
                        # New player
                        player = Player(
                            name=name,
                            exp=exp,
                            last_update=last_update,
                            world=world,
                            guild=guild
                        )
                        session.add(player)
                        
                        # Add initial delta
                        delta = Delta(
                            name=name,
                            deltaexp=exp,
                            update_time=update_time,
                            world=world,
                            guild=guild
                        )
                        try:
                            session.add(delta)
                            log_console(f"New player: {name} with {exp} EXP ({world} - {guild})")
                        except IntegrityError:
                            session.rollback()
                        
                        delta_queue.put({
                            'name': name,
                            'deltaexp': int(exp),
                            'update_time': update_time.isoformat(),
                            'prev_update_time': prev_update_time.isoformat(),
                            'world': world,
                            'guild': guild
                        })
                
                session.commit()
                clean_memory()
                
            except Exception as e:
                session.rollback()
                from fastapi_app import log_console
                log_console(f"Database update error: {e}", "ERROR")
                raise
            finally:
                session.close()
