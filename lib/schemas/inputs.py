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
from typing import Optional, Any
from pydantic import BaseModel, field_validator


# ---------------------------------------------------------------------------
# CBB — KenPom (cbb/kenpom.json)
# ---------------------------------------------------------------------------

class KenPomTeam(BaseModel):
    name: str
    kenpom_rank: int
    adj_o: float
    adj_d: float
    adj_t: float
    three_p_pct: float
    three_par: float
    ftr: float
    to_pct: float
    orb_pct: float
    block_pct: float
    steal_pct: float
    opp_3p_pct: float
    experience: float
    source: str
    fetched_at: datetime

    @property
    def adj_em(self) -> float:
        return round(self.adj_o - self.adj_d, 1)


class KenPomSnapshot(BaseModel):
    updated: datetime
    team_count: int
    teams: list[KenPomTeam]

    def get_team(self, name: str) -> Optional[KenPomTeam]:
        name_lower = name.lower()
        return next((t for t in self.teams if t.name.lower() == name_lower), None)


# ---------------------------------------------------------------------------
# CBB — ESPN scores (cbb/scores.json)
# Actual fields: espn_id, date, state, completed,
#                t1_name, t1_score, t1_winner,
#                t2_name, t2_score, t2_winner,
#                source, fetched_at
# ---------------------------------------------------------------------------

class ESPNGame(BaseModel):
    espn_id: str
    date: Optional[str] = None
    state: Optional[str] = None         # "pre" | "in" | "post"
    completed: Optional[bool] = None
    t1_name: str                        # home team
    t1_score: Optional[int] = None
    t1_winner: Optional[bool] = None
    t2_name: str                        # away team
    t2_score: Optional[int] = None
    t2_winner: Optional[bool] = None
    source: str
    fetched_at: datetime

    @property
    def home_team(self) -> str:
        return self.t1_name

    @property
    def away_team(self) -> str:
        return self.t2_name

    @property
    def is_final(self) -> bool:
        return self.completed is True


class ESPNSnapshot(BaseModel):
    updated: datetime
    game_count: int
    games: list[ESPNGame]

    def get_game(self, t1: str, t2: str) -> Optional[ESPNGame]:
        a, b = t1.lower(), t2.lower()
        return next(
            (g for g in self.games
             if g.t1_name.lower() == a and g.t2_name.lower() == b),
            None
        )


# ---------------------------------------------------------------------------
# CBB — Action Network odds (cbb/odds.json)
# Top-level key is "odds" not "games"
# ---------------------------------------------------------------------------

class ActionNetworkGame(BaseModel):
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    spread: Optional[float] = None
    total: Optional[float] = None
    home_ml: Optional[float] = None
    away_ml: Optional[float] = None
    source: Optional[str] = None
    fetched_at: Optional[datetime] = None

    model_config = {"extra": "allow"}  # absorb any extra fields gracefully

    @field_validator("home_ml", "away_ml", mode="before")
    @classmethod
    def coerce_ml(cls, v):
        return float(v) if v is not None else None


class ActionNetworkSnapshot(BaseModel):
    updated: datetime
    source: Optional[str] = None
    odds: Any = []                      # empty dict {} when no games, list when games exist

    @field_validator("odds", mode="before")
    @classmethod
    def normalize_odds(cls, v):
        """Scraper writes {} instead of [] when no active lines. Normalize to list."""
        if isinstance(v, dict):
            return list(v.values()) if v else []
        return v

    def get_games(self) -> list[ActionNetworkGame]:
        if not isinstance(self.odds, list):
            return []
        return [ActionNetworkGame(**g) if isinstance(g, dict) else g for g in self.odds]

    def get_game(self, home: str, away: str) -> Optional[ActionNetworkGame]:
        h, a = home.lower(), away.lower()
        return next(
            (g for g in self.get_games()
             if g.home_team and g.away_team
             and g.home_team.lower() == h and g.away_team.lower() == a),
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
    p1_ml: float
    p2_ml: float
    bookmaker: str
    commence_time: datetime
    source: str
    fetched_at: datetime

    @property
    def p1_implied_prob(self) -> float:
        return round(1 / self.p1_ml, 4)

    @property
    def p2_implied_prob(self) -> float:
        return round(1 / self.p2_ml, 4)

    @property
    def vig(self) -> float:
        return round(self.p1_implied_prob + self.p2_implied_prob - 1.0, 4)

    @property
    def no_vig_p1(self) -> float:
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
# Actual fields: name, slug, country, emoji, rank, lastUpdated,
#                ratings, elo, recentMatches, dataAvailability
# ---------------------------------------------------------------------------

class TennisPlayer(BaseModel):
    name: str
    slug: str
    country: Optional[str] = None
    emoji: Optional[str] = None
    rank: Optional[int] = None
    lastUpdated: Optional[str] = None
    ratings: Optional[dict] = None
    elo: Optional[dict] = None
    recentMatches: Optional[list] = None
    dataAvailability: Optional[dict] = None

    model_config = {"extra": "allow"}  # absorb any future TA fields

    @property
    def serve_rating(self) -> Optional[float]:
        """Pull serve rating from ratings dict if available."""
        if self.ratings:
            return self.ratings.get("serve")
        return None

    @property
    def return_rating(self) -> Optional[float]:
        if self.ratings:
            return self.ratings.get("return")
        return None

    @property
    def elo_rating(self) -> Optional[float]:
        if self.elo:
            return self.elo.get("overall")
        return None


class TennisAbstractSnapshot(BaseModel):
    updated: datetime
    player_count: int
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
    status: Optional[str] = None
    p1_sets: Optional[int] = None
    p2_sets: Optional[int] = None
    source: str
    fetched_at: datetime


class SofascoreSnapshot(BaseModel):
    updated: datetime
    matches: list[SofascoreMatch]


# ---------------------------------------------------------------------------
# GCS path → snapshot model registry
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
