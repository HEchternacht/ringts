from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Text, BigInteger, Boolean, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class Character(Base):
    __tablename__ = 'characters'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    world = Column(String(100), nullable=False, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    deaths = relationship("CharacterDeath", back_populates="character")
    kills = relationship("CharacterKill", back_populates="character")
    online_times = relationship("CharacterOnlineTime", back_populates="character")
    experiences = relationship("CharacterExperience", back_populates="character")
    delta_experiences = relationship("CharacterDeltaExperience")
    delta_onlines = relationship("CharacterDeltaOnline")
    
    def __repr__(self):
        return f"<Character(name='{self.name}', world='{self.world}')>"

class ScrapingSession(Base):
    __tablename__ = 'scraping_sessions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    world = Column(String(100), nullable=False, index=True)
    session_timestamp = Column(DateTime, nullable=False, default=func.now())
    
    # Last update times from status endpoint
    last_update_deaths_kills = Column(String(100))  # e.g., "Hoje às 09:17"
    time_outdated_deaths_kills = Column(String(50))  # e.g., "6 min"
    status_deaths_kills = Column(String(50))  # e.g., "Atualizado"
    
    last_update_online = Column(String(100))
    time_outdated_online = Column(String(50))
    status_online = Column(String(50))
    
    last_update_ranking = Column(String(100))
    time_outdated_ranking = Column(String(50))
    status_ranking = Column(String(50))
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    deaths = relationship("CharacterDeath", back_populates="scraping_session")
    kills = relationship("CharacterKill", back_populates="scraping_session")
    online_times = relationship("CharacterOnlineTime", back_populates="scraping_session")
    experiences = relationship("CharacterExperience", back_populates="scraping_session")
    delta_experiences = relationship("CharacterDeltaExperience")
    delta_onlines = relationship("CharacterDeltaOnline")
    
    def __repr__(self):
        return f"<ScrapingSession(world='{self.world}', timestamp='{self.session_timestamp}')>"

class CharacterDeath(Base):
    __tablename__ = 'character_deaths'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    death_time = Column(DateTime, nullable=False, index=True)
    level_at_death = Column(Integer, nullable=False)
    killed_by = Column(String(255), nullable=False)
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    character = relationship("Character", back_populates="deaths")
    scraping_session = relationship("ScrapingSession", back_populates="deaths")
    
    # Unique constraint to prevent duplicate death records
    __table_args__ = (
        UniqueConstraint('character_id', 'death_time', 'killed_by', name='unique_character_death'),
        Index('idx_character_death_time', 'character_id', 'death_time'),
    )
    
    def __repr__(self):
        return f"<CharacterDeath(character_id={self.character_id}, level={self.level_at_death}, killed_by='{self.killed_by}')>"

class CharacterKill(Base):
    __tablename__ = 'character_kills'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    kill_time = Column(DateTime, nullable=False, index=True)
    victim_name = Column(String(255), nullable=False)
    victim_level = Column(Integer, nullable=False)
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    character = relationship("Character", back_populates="kills")
    scraping_session = relationship("ScrapingSession", back_populates="kills")
    
    # Unique constraint to prevent duplicate kill records
    __table_args__ = (
        UniqueConstraint('character_id', 'kill_time', 'victim_name', name='unique_character_kill'),
        Index('idx_character_kill_time', 'character_id', 'kill_time'),
    )
    
    def __repr__(self):
        return f"<CharacterKill(character_id={self.character_id}, victim='{self.victim_name}', level={self.victim_level})>"

class CharacterOnlineTime(Base):
    __tablename__ = 'character_online_times'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    date = Column(DateTime, nullable=False, index=True)  # Date for the online time record
    online_time_minutes = Column(Integer, nullable=False)  # Converted from "3h 10m" format
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    character = relationship("Character", back_populates="online_times")
    scraping_session = relationship("ScrapingSession", back_populates="online_times")
    
    # Unique constraint to prevent duplicate online time records
    __table_args__ = (
        UniqueConstraint('character_id', 'date', name='unique_character_online_time'),
        Index('idx_character_online_date', 'character_id', 'date'),
    )
    
    def __repr__(self):
        return f"<CharacterOnlineTime(character_id={self.character_id}, date='{self.date}', minutes={self.online_time_minutes})>"

class CharacterExperience(Base):
    __tablename__ = 'character_experiences'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    date = Column(DateTime, nullable=False, index=True)  # Date for the experience record
    level = Column(Integer, nullable=False)
    level_delta = Column(Integer, nullable=False)  # Change in level from previous day
    raw_xp_day = Column(BigInteger, nullable=False)  # Raw XP gained that day
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    character = relationship("Character", back_populates="experiences")
    scraping_session = relationship("ScrapingSession", back_populates="experiences")
    
    # Unique constraint to prevent duplicate experience records
    __table_args__ = (
        UniqueConstraint('character_id', 'date', name='unique_character_experience'),
        Index('idx_character_exp_date', 'character_id', 'date'),
    )
    
    def __repr__(self):
        return f"<CharacterExperience(character_id={self.character_id}, level={self.level}, xp={self.raw_xp_day})>"

class CharacterDeltaExperience(Base):
    __tablename__ = 'character_delta_experiences'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    # Before/after tracking
    scraping_time_before = Column(DateTime, nullable=True, index=True)
    status_time_before = Column(DateTime, nullable=True, index=True)
    experience_before = Column(BigInteger, nullable=True)
    level_before = Column(Integer, nullable=True)
    
    scraping_time_after = Column(DateTime, nullable=False, index=True)
    status_time_after = Column(DateTime, nullable=False, index=True)
    experience_after = Column(BigInteger, nullable=False)
    level_after = Column(Integer, nullable=False)
    
    # Deltas
    experience_delta = Column(BigInteger, nullable=False)  # Difference in XP
    level_delta = Column(Integer, nullable=False)  # Difference in levels
    time_delta_minutes = Column(Integer, nullable=False)  # Time between measurements in minutes
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    character = relationship("Character")
    scraping_session = relationship("ScrapingSession")
    
    # Index for time-based queries
    __table_args__ = (
        Index('idx_character_delta_exp_time', 'character_id', 'scraping_time_after'),
        Index('idx_delta_exp_status_time', 'character_id', 'status_time_after'),
    )
    
    def __repr__(self):
        return f"<CharacterDeltaExperience(character_id={self.character_id}, xp_delta={self.experience_delta}, level_delta={self.level_delta})>"

class CharacterDeltaOnline(Base):
    __tablename__ = 'character_delta_onlines'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(Integer, ForeignKey('characters.id'), nullable=False, index=True)
    scraping_session_id = Column(Integer, ForeignKey('scraping_sessions.id'), nullable=False, index=True)
    
    # Before/after tracking
    scraping_time_before = Column(DateTime, nullable=True, index=True)
    status_time_before = Column(DateTime, nullable=True, index=True)
    online_minutes_before = Column(Integer, nullable=True)
    
    scraping_time_after = Column(DateTime, nullable=False, index=True)
    status_time_after = Column(DateTime, nullable=False, index=True)
    online_minutes_after = Column(Integer, nullable=False)
    
    # Deltas
    online_minutes_delta = Column(Integer, nullable=False)  # Difference in online time
    time_delta_minutes = Column(Integer, nullable=False)  # Time between measurements in minutes
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    character = relationship("Character")
    scraping_session = relationship("ScrapingSession")
    
    # Index for time-based queries
    __table_args__ = (
        Index('idx_character_delta_online_time', 'character_id', 'scraping_time_after'),
        Index('idx_delta_online_status_time', 'character_id', 'status_time_after'),
    )
    
    def __repr__(self):
        return f"<CharacterDeltaOnline(character_id={self.character_id}, online_delta={self.online_minutes_delta})>"

# Database connection and session setup
class DatabaseManager:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
    def create_tables(self):
        """Create all tables in the database"""
        Base.metadata.create_all(bind=self.engine)
        
    def drop_tables(self):
        """Drop all tables in the database"""
        Base.metadata.drop_all(bind=self.engine)
        
    def get_session(self):
        """Get a database session"""
        return self.SessionLocal()
    
    def close_session(self, session):
        """Close a database session"""
        session.close()

# Example usage and connection string
def get_database_manager(database_path='tibia_scraper.db'):
    """
    Get a DatabaseManager instance with SQLite connection
    
    Args:
        database_path (str): Path to SQLite database file (will be created if it doesn't exist)
    
    Returns:
        DatabaseManager: Database manager instance
    """
    connection_string = f"sqlite:///{database_path}"
    return DatabaseManager(connection_string)

if __name__ == "__main__":
    # Example usage with SQLite
    db_manager = get_database_manager('tibia_scraper.db')
    db_manager.create_tables()
    print("✅ SQLite database tables created successfully!")
