"""
SQLAlchemy database models for Ring TS application.
"""
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, JSON, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from typing import List, Dict, Optional
import os

Base = declarative_base()


class Player(Base):
    """Player experience tracking table."""
    __tablename__ = 'players'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    exp = Column(Integer, nullable=False, default=0)
    last_update = Column(DateTime, nullable=False, default=datetime.utcnow)
    world = Column(String, nullable=False)
    guild = Column(String, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('name', 'world', 'guild', name='uix_player_world_guild'),
        Index('ix_player_name', 'name'),
        Index('ix_player_world', 'world'),
        Index('ix_player_guild', 'guild'),
    )


class Delta(Base):
    """Player experience delta tracking table."""
    __tablename__ = 'deltas'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    deltaexp = Column(Integer, nullable=False)
    update_time = Column(DateTime, nullable=False)
    world = Column(String, nullable=False)
    guild = Column(String, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('name', 'update_time', name='uix_delta_name_time'),
        Index('ix_delta_name', 'name'),
        Index('ix_delta_time', 'update_time'),
        Index('ix_delta_world', 'world'),
    )


class VIP(Base):
    """VIP players list."""
    __tablename__ = 'vips'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    world = Column(String, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('name', 'world', name='uix_vip_name_world'),
        Index('ix_vip_name', 'name'),
    )


class VIPData(Base):
    """VIP player daily data."""
    __tablename__ = 'vip_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    world = Column(String, nullable=False)
    today_exp = Column(Integer, nullable=False, default=0)
    today_online = Column(Float, nullable=False, default=0.0)
    
    __table_args__ = (
        UniqueConstraint('name', 'world', name='uix_vipdata_name_world'),
    )


class VIPDelta(Base):
    """VIP player delta tracking."""
    __tablename__ = 'vip_deltas'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    world = Column(String, nullable=False)
    date = Column(String, nullable=False)  # Date string
    delta_exp = Column(Integer, nullable=False)
    delta_online = Column(Float, nullable=False)
    update_time = Column(DateTime, nullable=False)
    
    __table_args__ = (
        Index('ix_vipdelta_name', 'name'),
        Index('ix_vipdelta_time', 'update_time'),
    )


class Maker(Base):
    """Maker players list."""
    __tablename__ = 'makers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    world = Column(String, nullable=False)
    
    __table_args__ = (
        UniqueConstraint('name', 'world', name='uix_maker_name_world'),
        Index('ix_maker_name', 'name'),
    )


class MakerData(Base):
    """Maker player daily data."""
    __tablename__ = 'maker_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    world = Column(String, nullable=False)
    today_exp = Column(Integer, nullable=False, default=0)
    today_online = Column(Float, nullable=False, default=0.0)
    
    __table_args__ = (
        UniqueConstraint('name', 'world', name='uix_makerdata_name_world'),
    )


class MakerDelta(Base):
    """Maker player delta tracking."""
    __tablename__ = 'maker_deltas'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    world = Column(String, nullable=False)
    date = Column(String, nullable=False)  # Date string
    delta_exp = Column(Integer, nullable=False)
    delta_online = Column(Float, nullable=False)
    update_time = Column(DateTime, nullable=False)
    
    __table_args__ = (
        Index('ix_makerdelta_name', 'name'),
        Index('ix_makerdelta_time', 'update_time'),
    )


class StatusData(Base):
    """System status data storage."""
    __tablename__ = 'status_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    world = Column(String, nullable=False, unique=True)
    last_update = Column(DateTime)
    last_check = Column(DateTime)
    data = Column(JSON)  # Store additional data as JSON


class ScrapingConfig(Base):
    """Scraping configuration storage."""
    __tablename__ = 'scraping_config'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    config = Column(JSON, nullable=False)  # Store entire config as JSON
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DatabaseManager:
    """Database manager using SQLAlchemy."""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = "var/data/ringts.db"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False, 
                                     connect_args={'check_same_thread': False})
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
    
    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()
    
    def close(self):
        """Close database connection."""
        self.engine.dispose()
