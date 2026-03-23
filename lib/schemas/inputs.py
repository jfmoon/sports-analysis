"""
inputs.py — Pydantic v2 input schemas for the Sports Analysis layer.

Each model mirrors the exact JSON structure written by the scraper layer.
Use these to validate and parse raw GCS snapshot data before any processing.

Source bucket: sports-data-scraper-491116
GCS paths:
    cbb/kenpom.json
    cbb/scores.json
    cbb/odds.json
    cbb/evanmiya.json   (disabled scraper — schema defined for future use)
    tennis/odds.json
    tennis/players.json
    tennis/matches.json (disabled scraper — schema defined for future use)
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, field_validator


# ---------------------------------------------------------------------------
# CBB — KenPom (cbb/kenpom.json)
# ---------------------------------------------------------------------------

class KenPomTeam(BaseModel):
    name: str
    kenpom_rank: int
    adj_o: float            # Adjusted offensive efficiency
    adj_d: float            # Adjusted defensive efficiency
    adj_t: float            # Adjusted tempo
    three_p_pct: float      # 3-point percentage
    three_par: float        # 3-point attempt rate
    ftr: float              # Free throw rate
    to_pct: float           # Turnover percentage
    orb_pct: float          # Offensive rebound percentage
    block_pct: float        # Block percentage
    steal_pct: float        # Steal percentage
    opp_3p_pct: float       # Opponent 3-point percentage allowed
    experience: float       # Roster experience (years)
    source: str
    fetched_at: datetime

    @property
    def adj_em(self) -> float:
        """Adjusted efficiency margin — primary KenPom composite."""
        return round(self.adj_o - self.adj_d, 1)


class KenPomSnapshot(BaseModel):
    updated: datetime
    team_count: int
    teams: list[KenPomTeam]

    def get_team(self, name: str) -> Optional[KenPomTeam]:
        """Case-insensitive team lookup by name."""
        name_lower = name.lower()
        return next((t for t in self.teams if t.name.lower() == name_lower), None)


# ---------------------------------------------------------------------------
# CBB — ESPN scores (cbb/scores.json)
# ---------------------------------------------------------------------------

class ESPNGame(BaseModel):
    home_team: str
    away_team: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    status: str             # "final" | "in_progress" | "scheduled"
    source: str
    fetched_at: datetime


class ESPNSnapshot(BaseModel):
    updated: datetime
    games: list[ESPNGame]

    def get_game(self, home: str, away: str) -> Optional[ESPNGame]:
        h, a = home.lower(), away.lower()
        return next(
            (g for g in self.games
             if g.home_team.lower() == h and g.away_team.lower() == a),
            None
        )


# ---------------------------------------------------------------------------
# CBB — Action Network odds (cbb/odds.json)
# ---------------------------------------------------------------------------

class ActionNetworkGame(BaseModel):
    home_team: str
    away_team: str
    spread: Optional[float] = None      # Negative = home favored
    total: Optional[float] = None       # Over/under line
    home_ml: Optional[float] = None     # Moneyline (American format)
    away_ml: Optional[float] = None
    source: str
    fetched_at: datetime

    @field_validator("home_ml", "away_ml", mode="before")
    @classmethod
    def coerce_ml(cls, v):
        return float(v) if v is not None else None


class ActionNetworkSnapshot(BaseModel):
    updated: datetime
    games: list[ActionNetworkGame]

    def get_game(self, home: str, away: str) -> Optional[ActionNetworkGame]:
        h, a = home.lower(), away.lower()
        return next(
            (g for g in self.games
             if g.home_team.lower() == h and g.away_team.lower() == a),
            None
        )


# ---------------------------------------------------------------------------
# CBB — EvanMiya (cbb/evanmiya.json) — scraper currently disabled
# ---------------------------------------------------------------------------

class EvanMiyaTeam(BaseModel):
    name: str
    rank: Optional[int] = None
    off_rating: Optional[float] = None
    def_rating: Optional[float] = None
    pace: Optional[float] = None
    source: str
    fetched_at: datetime


class EvanMiyaSnapshot(BaseModel):
    updated: datetime
    teams: list[EvanMiyaTeam]

    def get_team(self, name: str) -> Optional[EvanMiyaTeam]:
        name_lower = name.lower()
        return next((t for t in self.teams if t.name.lower() == name_lower), None)


# ---------------------------------------------------------------------------
# Tennis — The Odds API (tennis/odds.json)
# ---------------------------------------------------------------------------

class TennisOddsMatch(BaseModel):
    match_id: str
    tournament: str
    p1_name: str
    p2_name: str
    p1_ml: float            # Decimal odds (European format)
    p2_ml: float
    bookmaker: str
    commence_time: datetime
    source: str
    fetched_at: datetime

    @property
    def p1_implied_prob(self) -> float:
        """Implied win probability for p1."""
        return round(1 / self.p1_ml, 4)

    @property
    def p2_implied_prob(self) -> float:
        return round(1 / self.p2_ml, 4)

    @property
    def vig(self) -> float:
        """Overround — excess above 1.0 is the book's margin."""
        return round(self.p1_implied_prob + self.p2_implied_prob - 1.0, 4)

    @property
    def no_vig_p1(self) -> float:
        """Vig-removed implied probability for p1."""
        total = self.p1_implied_prob + self.p2_implied_prob
        return round(self.p1_implied_prob / total, 4)

    @property
    def no_vig_p2(self) -> float:
        total = self.p1_implied_prob + self.p2_implied_prob
        return round(self.p2_implied_prob / total, 4)


class TennisOddsSnapshot(BaseModel):
    updated: datetime
    odds: list[TennisOddsMatch]

    def get_match(self, p1: str, p2: str) -> Optional[TennisOddsMatch]:
        p1l, p2l = p1.lower(), p2.lower()
        return next(
            (m for m in self.odds
             if m.p1_name.lower() == p1l and m.p2_name.lower() == p2l),
            None
        )

    def get_match_by_id(self, match_id: str) -> Optional[TennisOddsMatch]:
        return next((m for m in self.odds if m.match_id == match_id), None)


# ---------------------------------------------------------------------------
# Tennis — TennisAbstract players (tennis/players.json)
# ---------------------------------------------------------------------------

class TennisPlayer(BaseModel):
    name: str
    slug: str
    rank: Optional[int] = None
    serve_pct: Optional[float] = None       # First serve in percentage
    return_pct: Optional[float] = None      # Return points won percentage
    surface_stats: Optional[dict] = None    # Keyed by surface: hard/clay/grass
    source: str
    fetched_at: datetime


class TennisAbstractSnapshot(BaseModel):
    updated: datetime
    players: list[TennisPlayer]

    def get_player(self, name: str) -> Optional[TennisPlayer]:
        name_lower = name.lower()
        return next(
            (p for p in self.players if p.name.lower() == name_lower),
            None
        )

    def get_by_slug(self, slug: str) -> Optional[TennisPlayer]:
        return next((p for p in self.players if p.slug == slug), None)


# ---------------------------------------------------------------------------
# Tennis — Sofascore matches (tennis/matches.json) — scraper currently disabled
# ---------------------------------------------------------------------------

class SofascoreMatch(BaseModel):
    match_id: str
    tournament: str
    p1_name: str
    p2_name: str
    status: Optional[str] = None    # "finished" | "inprogress" | "notstarted"
    p1_sets: Optional[int] = None
    p2_sets: Optional[int] = None
    source: str
    fetched_at: datetime


class SofascoreSnapshot(BaseModel):
    updated: datetime
    matches: list[SofascoreMatch]


# ---------------------------------------------------------------------------
# GCS path → snapshot model registry
# Used by storage.py to auto-select the correct parser per trigger path
# ---------------------------------------------------------------------------

GCS_PATH_REGISTRY: dict[str, type] = {
    "cbb/kenpom.json":      KenPomSnapshot,
    "cbb/scores.json":      ESPNSnapshot,
    "cbb/odds.json":        ActionNetworkSnapshot,
    "cbb/evanmiya.json":    EvanMiyaSnapshot,
    "tennis/odds.json":     TennisOddsSnapshot,
    "tennis/players.json":  TennisAbstractSnapshot,
    "tennis/matches.json":  SofascoreSnapshot,
}
