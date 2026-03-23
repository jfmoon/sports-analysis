from pydantic import BaseModel, ConfigDict
from typing import List
from datetime import datetime


class WTARatings(BaseModel):
    """The 12 style dimensions on a 1.0 to 10.0 scale."""
    forehand: float = 5.0
    backhand: float = 5.0
    serve: float = 5.0
    netPlay: float = 5.0
    movement: float = 5.0
    spinHeavy: float = 5.0
    consistency: float = 5.0
    aggression: float = 5.0
    mentalGame: float = 5.0
    returnGame: float = 5.0
    variety: float = 5.0
    riskTaking: float = 5.0


class ArchetypeScore(BaseModel):
    """The computed score for a specific style archetype."""
    name: str
    score: float


class WTAPlayerArchetype(BaseModel):
    """The full classification record for a single player."""
    name: str
    emoji: str = "🎾"
    ratings: WTARatings
    primary_archetype: str
    secondary_archetype: str
    archetype_scores: List[ArchetypeScore]


class WTAArchetypeSnapshot(BaseModel):
    """The final collection written to GCS for frontend consumption."""
    model_config = ConfigDict(extra='ignore')
    updated: datetime
    player_count: int
    players: List[WTAPlayerArchetype]
