"""
Pydantic models for request/response validation in FastAPI application.
"""
from typing import List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime


# Request Models
class GraphRequest(BaseModel):
    """Request model for graph generation"""
    names: List[str] = Field(..., description="List of player names")
    datetime1: Optional[str] = Field(None, description="Start datetime (ISO format)")
    datetime2: Optional[str] = Field(None, description="End datetime (ISO format)")


class StatsRequest(BaseModel):
    """Request model for player statistics"""
    names: List[str] = Field(..., description="List of player names")
    datetime1: Optional[str] = Field(None, description="Start datetime (ISO format)")
    datetime2: Optional[str] = Field(None, description="End datetime (ISO format)")


class RankingsTableRequest(BaseModel):
    """Request model for rankings table"""
    datetime1: Optional[str] = Field(None, description="Start datetime (ISO format)")
    datetime2: Optional[str] = Field(None, description="End datetime (ISO format)")


class VIPAddRequest(BaseModel):
    """Request model for adding VIP player"""
    name: str = Field(..., description="Player name")
    world: str = Field(..., description="World name")


class VIPRemoveRequest(BaseModel):
    """Request model for removing VIP player"""
    name: str = Field(..., description="Player name")
    world: str = Field(..., description="World name")


class VIPGraphRequest(BaseModel):
    """Request model for VIP graph generation"""
    name: str = Field(..., description="Player name")
    world: str = Field(..., description="World name")


class GuildConfig(BaseModel):
    """Configuration for a single world with its guilds"""
    world: str = Field(..., description="World name")
    guilds: List[str] = Field(..., description="List of guild names")


class ScrapingConfigRequest(BaseModel):
    """Request model for updating scraping configuration"""
    password: str = Field(..., description="Admin password")
    config: List[GuildConfig] = Field(..., description="Scraping configuration")


class UploadRequest(BaseModel):
    """Base model for file upload requests (password only)"""
    password: str = Field(..., description="Admin password")


# Response Models
class PlayerStats(BaseModel):
    """Statistics for a single player"""
    name: str
    total_exp: int = Field(alias="Total EXP")
    average_exp: float = Field(alias="Average EXP")
    updates: int = Field(alias="Updates")
    max_exp: int = Field(alias="Max EXP")
    min_exp: int = Field(alias="Min EXP")

    class Config:
        populate_by_name = True


class PlayerComparison(BaseModel):
    """Comparison data for a player"""
    name: str
    rank: int
    total_players: int
    percentile: float
    total_exp_period: int
    current_total_exp: int


class GraphResponse(BaseModel):
    """Response model for graph endpoint"""
    graph: str = Field(..., description="Plotly graph JSON")
    stats: List[dict]
    comparison: List[PlayerComparison]


class DateRangeResponse(BaseModel):
    """Response model for date range endpoint"""
    min: Optional[str]
    max: Optional[str]


class TopPlayer(BaseModel):
    """Top player model"""
    name: str
    total_exp: int


class DeltaUpdate(BaseModel):
    """Delta update model"""
    name: str
    deltaexp: int
    update_time: str
    prev_update_time: str
    world: str
    guild: str


class RankingEntry(BaseModel):
    """Ranking table entry"""
    name: str
    total_exp: int
    updates: int
    avg_exp: float
    max_exp: int
    min_exp: int


class ScraperStatus(BaseModel):
    """Scraper status model"""
    running: bool
    state: str
    last_update: Optional[str]
    last_check: str


class VIPEntry(BaseModel):
    """VIP player entry"""
    name: str
    world: str


class VIPDelta(BaseModel):
    """VIP delta entry"""
    name: str
    world: str
    delta_exp: int
    delta_online: int
    update_time: str
    prev_update_time: str
    date: str


class VIPGraphStats(BaseModel):
    """VIP graph statistics"""
    total_exp: int
    avg_exp: float
    max_exp: int
    total_online: int
    avg_online: float
    updates: int


class VIPGraphResponse(BaseModel):
    """Response model for VIP graph"""
    success: bool
    graph_data: str
    stats: VIPGraphStats


class ErrorResponse(BaseModel):
    """Generic error response"""
    error: str
    message: Optional[str] = None
    status: int


class SuccessResponse(BaseModel):
    """Generic success response"""
    success: bool
    message: Optional[str] = None
