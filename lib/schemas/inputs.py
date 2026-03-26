"""
inputs.py — Pydantic v2 input schemas for the Sports Analysis layer.

Each model mirrors the exact JSON structure written by the scraper layer.
Use these to validate and parse raw GCS snapshot data before any processing.

Source bucket: sports-data-scraper-491116

GCS paths:
  cbb/kenpom.json
  cbb/scores.json
  cbb/odds.json
  cbb/evanmiya.json     (disabled scraper — schema defined for future use)
  tennis/odds.json
  tennis/players.json   ← raw_stats (float) as of 2026-03-23; was ratings (int)
  tennis/matches.json   (disabled scraper — schema defined for future use)
  mlb/probables.json
  mlb/pitchers.json
  mlb/teams.json
  mlb/bullpen.json
  mlb/statcast_pitchers.json
  mlb/statcast_hitters.json
  mlb/weather.json
  mlb/lineups.json
  mlb/odds.json
"""

from datetime import datetime
from typing import Optional, Any, List
from pydantic import BaseModel, Field, field_validator


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
# ---------------------------------------------------------------------------

class ESPNGame(BaseModel):
    espn_id: str
    date: Optional[str] = None
    state: Optional[str] = None        # "pre" | "in" | "post"
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
    model_config = {"extra": "allow"}

    @field_validator("home_ml", "away_ml", mode="before")
    @classmethod
    def coerce_ml(cls, v):
        return float(v) if v is not None else None


class ActionNetworkSnapshot(BaseModel):
    updated: datetime
    source: Optional[str] = None
    odds: Any = []

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
#
# Schema change 2026-03-23:
#   - Added `raw_stats`: raw float percentages from the scraper
#   - Retained `ratings` as Optional for backward compatibility with any
#     GCS files written before this change (will be None for new files)
#   - `wta_archetype_classifier.py` uses raw_stats when present,
#     falls back to ratings for legacy files
# ---------------------------------------------------------------------------

class TennisPlayer(BaseModel):
    name: str
    slug: str
    country: Optional[str] = None
    emoji: Optional[str] = None
    rank: Optional[int] = None
    lastUpdated: Optional[str] = None
    raw_stats: Optional[dict[str, Optional[float]]] = None  # new: raw float stats
    ratings: Optional[dict] = None                          # legacy: pre-scored 1-10 ints
    elo: Optional[dict] = None
    recentMatches: Optional[list] = None
    dataAvailability: Optional[dict] = None
    model_config = {"extra": "allow"}                       # absorb any future TA fields

    @property
    def has_raw_stats(self) -> bool:
        """True if this player record uses the new raw_stats format."""
        return self.raw_stats is not None and len(self.raw_stats) > 0

    @property
    def elo_rating(self) -> Optional[float]:
        if self.elo:
            return self.elo.get("elo")
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
# MLB — Probables (mlb/probables.json)
# ---------------------------------------------------------------------------

class MlbGame(BaseModel):
    game_id: str
    date: str
    commence_time: str
    away_team: str
    home_team: str
    away_pitcher: Optional[str] = None
    home_pitcher: Optional[str] = None
    away_hand: Optional[str] = None         # "L" | "R" | "S" | None
    home_hand: Optional[str] = None
    away_pitcher_id: Optional[int] = None   # MLB Stats API player ID
    home_pitcher_id: Optional[int] = None
    away_confirmed: bool = False
    home_confirmed: bool = False
    source: Optional[str] = None
    fetched_at: Optional[str] = None


class MlbProbablesSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    game_count: Optional[int] = None
    games: List[MlbGame] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — Fangraphs pitchers (mlb/pitchers.json)
# ---------------------------------------------------------------------------

class MlbPitcher(BaseModel):
    pitcher_id: str
    name: str
    team: str
    throws: str                             # "L" | "R" | "S"
    season: int
    games: Optional[int] = None
    games_started: Optional[int] = None
    innings_pitched: Optional[float] = None
    era: Optional[float] = None
    fip: Optional[float] = None
    xfip: Optional[float] = None
    siera: Optional[float] = None
    whip: Optional[float] = None
    k_pct: Optional[float] = None
    bb_pct: Optional[float] = None
    k_minus_bb_pct: Optional[float] = None
    gb_pct: Optional[float] = None
    hard_hit_pct: Optional[float] = None
    barrel_pct: Optional[float] = None
    hr_per_9: Optional[float] = None
    swstr_pct: Optional[float] = None
    source: Optional[str] = None
    fetched_at: Optional[str] = None


class MlbPitchersSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    season: Optional[int] = None
    pitcher_count: Optional[int] = None
    pitchers: List[MlbPitcher] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — Fangraphs team splits (mlb/teams.json)
# ---------------------------------------------------------------------------

class MlbTeamSplit(BaseModel):
    team: str
    season: int
    split: str                              # "overall" | "vs_lhp" | "vs_rhp"
    pa: Optional[int] = None
    avg: Optional[float] = None
    obp: Optional[float] = None
    slg: Optional[float] = None
    ops: Optional[float] = None
    iso: Optional[float] = None
    woba: Optional[float] = None
    # NOTE: wrc_plus is already park-adjusted.
    # Never use in park-factor or run-environment calculations — display only.
    wrc_plus: Optional[float] = None
    k_pct: Optional[float] = None
    bb_pct: Optional[float] = None
    barrel_pct: Optional[float] = None
    hard_hit_pct: Optional[float] = None
    gb_pct: Optional[float] = None
    fb_pct: Optional[float] = None
    swstr_pct: Optional[float] = None
    source: Optional[str] = None
    fetched_at: Optional[str] = None


class MlbTeamsSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    team_count: Optional[int] = None
    season: Optional[int] = None
    splits_available: List[str] = Field(default_factory=list)
    teams: List[MlbTeamSplit] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — Fangraphs bullpen (mlb/bullpen.json)
# ---------------------------------------------------------------------------

class MlbBullpen(BaseModel):
    team: str
    season: int
    innings_pitched: Optional[float] = None
    games: Optional[int] = None
    era: Optional[float] = None
    fip: Optional[float] = None
    xfip: Optional[float] = None
    siera: Optional[float] = None
    whip: Optional[float] = None
    k_pct: Optional[float] = None
    bb_pct: Optional[float] = None
    k_minus_bb_pct: Optional[float] = None
    gb_pct: Optional[float] = None
    hard_hit_pct: Optional[float] = None
    barrel_pct: Optional[float] = None
    hr_per_9: Optional[float] = None
    lob_pct: Optional[float] = None
    swstr_pct: Optional[float] = None


class MlbBullpenSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    team_count: Optional[int] = None
    season: Optional[int] = None
    pitching_role: Optional[str] = None    # "reliever"
    bullpens: List[MlbBullpen] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — Baseball Savant statcast pitchers (mlb/statcast_pitchers.json)
# ---------------------------------------------------------------------------

class MlbStatcastPitcher(BaseModel):
    # player_id is str in GCS (scraper casts from int at write time).
    # Join via: str(probables.away_pitcher_id) == statcast_pitcher.player_id
    player_id: str
    name: str
    team: str
    season: int
    pa: Optional[int] = None
    xera: Optional[float] = None
    xba: Optional[float] = None
    xslg: Optional[float] = None
    xwoba: Optional[float] = None
    whiff_pct: Optional[float] = None
    k_pct: Optional[float] = None
    bb_pct: Optional[float] = None
    barrel_pct: Optional[float] = None
    hard_hit_pct: Optional[float] = None
    avg_exit_velocity: Optional[float] = None
    source: Optional[str] = None
    fetched_at: Optional[str] = None


class MlbStatcastPitchersSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    season: Optional[int] = None
    pitcher_count: Optional[int] = None
    pitchers: List[MlbStatcastPitcher] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — Baseball Savant statcast hitters (mlb/statcast_hitters.json)
# ---------------------------------------------------------------------------

class MlbStatcastHitter(BaseModel):
    player_id: str
    name: str
    team: str
    season: int
    pa: Optional[int] = None
    xba: Optional[float] = None
    xslg: Optional[float] = None
    xwoba: Optional[float] = None
    barrel_pct: Optional[float] = None
    hard_hit_pct: Optional[float] = None
    avg_exit_velocity: Optional[float] = None
    whiff_pct: Optional[float] = None
    k_pct: Optional[float] = None
    bb_pct: Optional[float] = None
    source: Optional[str] = None
    fetched_at: Optional[str] = None


class MlbStatcastHittersSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    season: Optional[int] = None
    hitter_count: Optional[int] = None
    hitters: List[MlbStatcastHitter] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — Open-Meteo weather (mlb/weather.json)
# ---------------------------------------------------------------------------

class MlbGameWeather(BaseModel):
    game_id: str
    date: str
    away_team: str
    home_team: str
    stadium: str
    city: str
    state: str
    is_dome: bool
    is_retractable: bool
    # All fields below are None when is_dome=True (Tropicana Field only).
    temperature_f: Optional[float] = None
    wind_mph: Optional[float] = None
    wind_direction: Optional[str] = None    # 8-pt compass: N/NE/E/SE/S/SW/W/NW
    precip_pct: Optional[float] = None
    humidity_pct: Optional[float] = None
    conditions: Optional[str] = None
    source: Optional[str] = None
    fetched_at: Optional[str] = None


class MlbWeatherSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    game_count: Optional[int] = None
    games: List[MlbGameWeather] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — MLB Stats API lineups (mlb/lineups.json)
# ---------------------------------------------------------------------------

class MlbStarter(BaseModel):
    batting_order: int
    player_name: str
    player_id: int
    position: str
    bats: Optional[str] = None             # "L" | "R" | "S" | None


class MlbLineupGame(BaseModel):
    game_id: str
    date: str
    commence_time: str
    away_team: str
    home_team: str
    away_confirmed: bool = False
    home_confirmed: bool = False
    away_lineup: List[MlbStarter] = Field(default_factory=list)
    home_lineup: List[MlbStarter] = Field(default_factory=list)
    source: Optional[str] = None
    fetched_at: Optional[str] = None


class MlbLineupsSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    game_count: Optional[int] = None
    games: List[MlbLineupGame] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MLB — Action Network odds (mlb/odds.json)
# ---------------------------------------------------------------------------

class MlbOddsGame(BaseModel):
    # game_id normalized to str at load time via field_validator.
    # Source is int from Action Network — never join on this ID across snapshots.
    # Odds join key is (away_team, home_team) raw Action Network strings.
    game_id: str
    sport: str
    status: str
    commence_time: str
    away_team: str          # raw Action Network string — no canonical crosswalk yet
    home_team: str
    bookmaker: str
    away_ml: int
    home_ml: int
    away_spread: Optional[float] = None
    away_spread_odds: Optional[int] = None
    home_spread: Optional[float] = None
    home_spread_odds: Optional[int] = None
    total: Optional[float] = None
    over_odds: Optional[int] = None
    under_odds: Optional[int] = None

    @field_validator("game_id", mode="before")
    @classmethod
    def coerce_game_id_to_str(cls, v: object) -> str:
        return str(v)


class MlbOddsSnapshot(BaseModel):
    schema_version: int = 0
    generated_at: Optional[str] = None
    scraper_key: Optional[str] = None
    record_count: Optional[int] = None
    warnings: List[str] = Field(default_factory=list)
    updated: Optional[str] = None
    sport: Optional[str] = None
    game_count: Optional[int] = None
    odds: List[MlbOddsGame] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# GCS path → snapshot model registry
# ALL new paths must be registered here before any job can read them.
# ---------------------------------------------------------------------------

GCS_PATH_REGISTRY: dict[str, type] = {
    # CBB
    "cbb/kenpom.json":              KenPomSnapshot,
    "cbb/scores.json":              ESPNSnapshot,
    "cbb/odds.json":                ActionNetworkSnapshot,
    "cbb/evanmiya.json":            EvanMiyaSnapshot,
    # Tennis
    "tennis/odds.json":             TennisOddsSnapshot,
    "tennis/players.json":          TennisAbstractSnapshot,
    "tennis/matches.json":          SofascoreSnapshot,
    # MLB
    "mlb/probables.json":           MlbProbablesSnapshot,
    "mlb/pitchers.json":            MlbPitchersSnapshot,
    "mlb/teams.json":               MlbTeamsSnapshot,
    "mlb/bullpen.json":             MlbBullpenSnapshot,
    "mlb/statcast_pitchers.json":   MlbStatcastPitchersSnapshot,
    "mlb/statcast_hitters.json":    MlbStatcastHittersSnapshot,
    "mlb/weather.json":             MlbWeatherSnapshot,
    "mlb/lineups.json":             MlbLineupsSnapshot,
    "mlb/odds.json":                MlbOddsSnapshot,
}
