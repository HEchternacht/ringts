"""
Database models and manager for Tibia character tracking system.
Simplified and streamlined version.
"""
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, BigInteger, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
from contextlib import contextmanager

Base = declarative_base()


class Character(Base):
    """Character table - stores basic character info"""
    __tablename__ = 'characters'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    world = Column(String(100), nullable=False, index=True)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    deaths = relationship("CharacterDeath", back_populates="character")
    kills = relationship("CharacterKill", back_populates="character")
    online_times = relationship("CharacterOnlineTime", back_populates="character")
    experiences = relationship("CharacterExperience", back_populates="character")
    delta_experiences = relationship("CharacterDeltaExperience", back_populates="character")
    delta_onlines = relationship("CharacterDeltaOnline", back_populates="character")


class ScrapingSession(Base):
    """Scraping session - tracks when data was collected"""
    __tablename__ = 'scraping_sessions'
    
    id = Column(Integer, primary_key=True)
    world = Column(String(100), nullable=False, index=True)
    session_timestamp = Column(DateTime, nullable=False, default=func.now())
    
    # Status update times (optional metadata)
    deaths_kills_update = Column(String(100))
    online_update = Column(String(100))
    ranking_update = Column(String(100))
    
    # Relationships
    deaths = relationship("CharacterDeath", back_populates="scraping_session")
    kills = relationship("CharacterKill", back_populates="scraping_session")
    online_times = relationship("CharacterOnlineTime", back_populates="scraping_session")
    experiences = relationship("CharacterExperience", back_populates="scraping_session")
    delta_experiences = relationship("CharacterDeltaExperience", back_populates="scraping_session")
    delta_onlines = relationship("CharacterDeltaOnline", back_populates="scraping_session")


class CharacterDeath(Base):
    """Character deaths"""
    __tablename__ = 'character_deaths'
    
    id = Column(Integer, primary_key=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    death_time = Column(DateTime, nullable=False, index=True)
    level_at_death = Column(Integer, nullable=False)
    killed_by = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=func.now())
    
    character = relationship("Character", back_populates="deaths")
    scraping_session = relationship("ScrapingSession", back_populates="deaths")
    
    __table_args__ = (
        UniqueConstraint('character_id', 'death_time', 'killed_by', name='unique_death'),
        Index('idx_death_time', 'character_id', 'death_time'),
    )


class CharacterKill(Base):
    """Character kills"""
    __tablename__ = 'character_kills'
    
    id = Column(Integer, primary_key=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    kill_time = Column(DateTime, nullable=False, index=True)
    victim_name = Column(String(255), nullable=False)
    victim_level = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=func.now())
    
    character = relationship("Character", back_populates="kills")
    scraping_session = relationship("ScrapingSession", back_populates="kills")
    
    __table_args__ = (
        UniqueConstraint('character_id', 'kill_time', 'victim_name', name='unique_kill'),
        Index('idx_kill_time', 'character_id', 'kill_time'),
    )


class CharacterOnlineTime(Base):
    """Daily online time records"""
    __tablename__ = 'character_online_times'
    
    id = Column(Integer, primary_key=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    online_time_minutes = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=func.now())
    
    character = relationship("Character", back_populates="online_times")
    scraping_session = relationship("ScrapingSession", back_populates="online_times")
    
    __table_args__ = (
        UniqueConstraint('character_id', 'date', name='unique_online_time'),
        Index('idx_online_date', 'character_id', 'date'),
    )


class CharacterExperience(Base):
    """Daily experience records"""
    __tablename__ = 'character_experiences'
    
    id = Column(Integer, primary_key=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    level = Column(Integer, nullable=False)
    level_delta = Column(Integer, nullable=False)
    raw_xp_day = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, default=func.now())
    
    character = relationship("Character", back_populates="experiences")
    scraping_session = relationship("ScrapingSession", back_populates="experiences")
    
    __table_args__ = (
        UniqueConstraint('character_id', 'date', name='unique_experience'),
        Index('idx_exp_date', 'character_id', 'date'),
    )


class CharacterDeltaExperience(Base):
    """Experience changes between scraping sessions"""
    __tablename__ = 'c'
    
    id = Column(Integer, primary_key=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    # Before state
    time_before = Column(DateTime, nullable=False)
    experience_before = Column(BigInteger, nullable=False)
    level_before = Column(Integer, nullable=False)
    
    # After state
    time_after = Column(DateTime, nullable=False, index=True)
    experience_after = Column(BigInteger, nullable=False)
    level_after = Column(Integer, nullable=False)
    
    # Calculated deltas
    experience_delta = Column(BigInteger, nullable=False)
    level_delta = Column(Integer, nullable=False)
    time_delta_minutes = Column(Integer, nullable=False)
    
    created_at = Column(DateTime, default=func.now())
    
    character = relationship("Character", back_populates="delta_experiences")
    scraping_session = relationship("ScrapingSession", back_populates="delta_experiences")
    
    __table_args__ = (
        Index('idx_delta_exp_time', 'character_id', 'time_after'),
    )


class CharacterDeltaOnline(Base):
    """Online time changes between scraping sessions"""
    __tablename__ = 'character_delta_onlines'
    
    id = Column(Integer, primary_key=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    # Before state
    time_before = Column(DateTime, nullable=False)
    online_minutes_before = Column(Integer, nullable=False)
    
    # After state
    time_after = Column(DateTime, nullable=False, index=True)
    online_minutes_after = Column(Integer, nullable=False)
    
    # Calculated deltas
    online_minutes_delta = Column(Integer, nullable=False)
    time_delta_minutes = Column(Integer, nullable=False)
    
    created_at = Column(DateTime, default=func.now())
    
    character = relationship("Character", back_populates="delta_onlines")
    scraping_session = relationship("ScrapingSession", back_populates="delta_onlines")
    
    __table_args__ = (
        Index('idx_delta_online_time', 'character_id', 'time_after'),
    )


class Database:
    """Simple database manager"""
    
    def __init__(self, db_path='tibia_scraper.db'):
        """Initialize database connection"""
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)
        self.Session = sessionmaker(bind=self.engine)
    
    def create_tables(self):
        """Create all tables"""
        Base.metadata.create_all(self.engine)
    

    def drop_tables(self):
        """Drop all tables (use with caution)"""
        Base.metadata.drop_all(self.engine)
    @contextmanager
    def session(self):
        """Context manager for database sessions"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
