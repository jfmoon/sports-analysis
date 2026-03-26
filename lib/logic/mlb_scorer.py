# lib/logic/mlb_scorer.py
# Ballpark factor and run environment scoring logic for MLB projector.
# All functions are pure (no GCS I/O, no logging side-effects).
# Import this module from jobs/mlb_projector.py.

from __future__ import annotations

import math
import unicodedata
from typing import Optional


# ---------------------------------------------------------------------------
# CANONICAL_MLB_TEAMS
# Source of truth for park metadata integrity check.
# Must match exactly what scrapers/mlb/names.py uses as canonical names.
# Defined independently of PARK_METADATA — these are two separate facts.
# If the scraper renames a franchise, update BOTH this set AND PARK_METADATA.
# ---------------------------------------------------------------------------

CANONICAL_MLB_TEAMS: frozenset[str] = frozenset({
    "Arizona Diamondbacks",
    "Atlanta Braves",
    "Baltimore Orioles",
    "Boston Red Sox",
    "Chicago Cubs",
    "Chicago White Sox",
    "Cincinnati Reds",
    "Cleveland Guardians",
    "Colorado Rockies",
    "Detroit Tigers",
    "Houston Astros",
    "Kansas City Royals",
    "Los Angeles Angels",
    "Los Angeles Dodgers",
    "Miami Marlins",
    "Milwaukee Brewers",
    "Minnesota Twins",
    "New York Mets",
    "New York Yankees",
    "Athletics",               # Franchise relocated from Oakland; verify against
                               # scrapers/mlb/names.py canonical — update if different
    "Philadelphia Phillies",
    "Pittsburgh Pirates",
    "San Diego Padres",
    "San Francisco Giants",
    "Seattle Mariners",
    "St. Louis Cardinals",
    "Tampa Bay Rays",
    "Texas Rangers",
    "Toronto Blue Jays",
    "Washington Nationals",
})

# ---------------------------------------------------------------------------
# PARK_METADATA
# Keyed by canonical team name (must match CANONICAL_MLB_TEAMS exactly).
#
# cf_deg: degrees from home plate to center field.
#   0 = CF points North, 90 = East, 180 = South, 270 = West.
#   Used to compute wind vector relative to the field axis.
#
# base_pf: run park factor multiplier (1.0 = neutral).
#   Source: multi-year Fangraphs park factor averages (approximate).
#   Tune annually against observed data.
#
# base_hr: HR park factor multiplier (separate from run factor).
#   Source: multi-year Fangraphs HR park factor averages (approximate).
#
# exposure: qualitative wind exposure rating.
#   "none" = fully enclosed dome. "extreme" = fully open, known wind park.
#   Used for confidence dampening when wind data is uncertain.
# ---------------------------------------------------------------------------

PARK_METADATA: dict[str, dict] = {
    "Arizona Diamondbacks": {
        "stadium": "Chase Field",
        "cf_deg": 0,
        "base_pf": 1.02,
        "base_hr": 1.05,
        "exposure": "low",          # retractable roof
    },
    "Atlanta Braves": {
        "stadium": "Truist Park",
        "cf_deg": 23,
        "base_pf": 1.00,
        "base_hr": 1.02,
        "exposure": "moderate",
    },
    "Baltimore Orioles": {
        "stadium": "Camden Yards",
        "cf_deg": 30,
        "base_pf": 0.98,
        "base_hr": 1.00,
        "exposure": "moderate",
    },
    "Boston Red Sox": {
        "stadium": "Fenway Park",
        "cf_deg": 45,
        "base_pf": 1.06,
        "base_hr": 0.95,            # Green Monster suppresses HR, boosts doubles
        "exposure": "high",
    },
    "Chicago Cubs": {
        "stadium": "Wrigley Field",
        "cf_deg": 22,
        "base_pf": 1.01,
        "base_hr": 1.05,
        "exposure": "extreme",      # Lake Michigan wind dominant factor
    },
    "Chicago White Sox": {
        "stadium": "Guaranteed Rate Field",
        "cf_deg": 180,
        "base_pf": 1.00,
        "base_hr": 1.04,
        "exposure": "moderate",
    },
    "Cincinnati Reds": {
        "stadium": "Great American Ball Park",
        "cf_deg": 147,
        "base_pf": 1.03,
        "base_hr": 1.12,            # Known HR-friendly environment
        "exposure": "moderate",
    },
    "Cleveland Guardians": {
        "stadium": "Progressive Field",
        "cf_deg": 355,
        "base_pf": 0.99,
        "base_hr": 0.97,
        "exposure": "moderate",
    },
    "Colorado Rockies": {
        "stadium": "Coors Field",
        "cf_deg": 350,
        "base_pf": 1.32,
        "base_hr": 1.15,            # Altitude effect dominates
        "exposure": "high",
    },
    "Detroit Tigers": {
        "stadium": "Comerica Park",
        "cf_deg": 345,
        "base_pf": 0.96,
        "base_hr": 0.88,            # Deep CF suppresses HR
        "exposure": "moderate",
    },
    "Houston Astros": {
        "stadium": "Minute Maid Park",
        "cf_deg": 0,
        "base_pf": 0.98,
        "base_hr": 1.01,
        "exposure": "low",          # retractable roof
    },
    "Kansas City Royals": {
        "stadium": "Kauffman Stadium",
        "cf_deg": 5,
        "base_pf": 1.02,
        "base_hr": 0.98,
        "exposure": "moderate",
    },
    "Los Angeles Angels": {
        "stadium": "Angel Stadium",
        "cf_deg": 52,
        "base_pf": 0.99,
        "base_hr": 1.00,
        "exposure": "moderate",
    },
    "Los Angeles Dodgers": {
        "stadium": "Dodger Stadium",
        "cf_deg": 15,
        "base_pf": 0.94,
        "base_hr": 0.93,
        "exposure": "moderate",
    },
    "Miami Marlins": {
        "stadium": "loanDepot park",
        "cf_deg": 0,
        "base_pf": 0.92,
        "base_hr": 0.88,
        "exposure": "low",          # retractable roof
    },
    "Milwaukee Brewers": {
        "stadium": "American Family Field",
        "cf_deg": 0,
        "base_pf": 1.00,
        "base_hr": 1.01,
        "exposure": "low",          # retractable roof
    },
    "Minnesota Twins": {
        "stadium": "Target Field",
        "cf_deg": 15,
        "base_pf": 0.97,
        "base_hr": 0.96,
        "exposure": "moderate",
    },
    "New York Mets": {
        "stadium": "Citi Field",
        "cf_deg": 40,
        "base_pf": 0.93,
        "base_hr": 0.91,
        "exposure": "moderate",
    },
    "New York Yankees": {
        "stadium": "Yankee Stadium",
        "cf_deg": 20,
        "base_pf": 1.02,
        "base_hr": 1.20,            # Short RF porch is a significant HR factor
        "exposure": "moderate",
    },
    # NOTE: Oakland Athletics relocated to Sacramento. The canonical name in
    # scrapers/mlb/names.py as of 2026 may be "Athletics" or another name.
    # Verify against scrapers/mlb/names.py and update this key to match exactly.
    # The startup integrity check will catch any mismatch at runtime.
    "Athletics": {
        "stadium": "Sutter Health Park",
        "cf_deg": 0,
        "base_pf": 1.00,           # Insufficient historical data — set neutral
        "base_hr": 1.00,
        "exposure": "moderate",
    },
    "Philadelphia Phillies": {
        "stadium": "Citizens Bank Park",
        "cf_deg": 10,
        "base_pf": 1.01,
        "base_hr": 1.06,
        "exposure": "moderate",
    },
    "Pittsburgh Pirates": {
        "stadium": "PNC Park",
        "cf_deg": 75,
        "base_pf": 0.96,
        "base_hr": 0.94,
        "exposure": "moderate",
    },
    "San Diego Padres": {
        "stadium": "Petco Park",
        "cf_deg": 20,
        "base_pf": 0.91,
        "base_hr": 0.87,
        "exposure": "high",         # Marine layer and ocean air suppress offense
    },
    "San Francisco Giants": {
        "stadium": "Oracle Park",
        "cf_deg": 35,
        "base_pf": 0.92,
        "base_hr": 0.85,
        "exposure": "extreme",      # Bay wind is the dominant factor; highly variable
    },
    "Seattle Mariners": {
        "stadium": "T-Mobile Park",
        "cf_deg": 0,
        "base_pf": 0.91,
        "base_hr": 0.90,
        "exposure": "moderate",     # retractable roof
    },
    "St. Louis Cardinals": {
        "stadium": "Busch Stadium",
        "cf_deg": 150,
        "base_pf": 0.94,
        "base_hr": 0.93,
        "exposure": "moderate",
    },
    "Tampa Bay Rays": {
        "stadium": "Tropicana Field",
        "cf_deg": 0,
        "base_pf": 0.93,
        "base_hr": 0.91,
        "exposure": "none",         # Fully enclosed dome — wind has zero effect
    },
    "Texas Rangers": {
        "stadium": "Globe Life Field",
        "cf_deg": 0,
        "base_pf": 1.00,
        "base_hr": 1.02,
        "exposure": "low",          # retractable roof
    },
    "Toronto Blue Jays": {
        "stadium": "Rogers Centre",
        "cf_deg": 0,
        "base_pf": 0.99,
        "base_hr": 1.00,
        "exposure": "low",          # retractable roof
    },
    "Washington Nationals": {
        "stadium": "Nationals Park",
        "cf_deg": 160,
        "base_pf": 0.98,
        "base_hr": 0.97,
        "exposure": "moderate",
    },
}


# ---------------------------------------------------------------------------
# WIND_DIR_MAP
# Maps compass string to degrees (0=N, 90=E, 180=S, 270=W).
# Covers both 8-point (scraper output) and 16-point compass strings.
# Any string not in this map is treated as unknown — see calculate_wind_effect.
# ---------------------------------------------------------------------------

WIND_DIR_MAP: dict[str, float] = {
    "N": 0.0,    "NNE": 22.5,  "NE": 45.0,   "ENE": 67.5,
    "E": 90.0,   "ESE": 112.5, "SE": 135.0,  "SSE": 157.5,
    "S": 180.0,  "SSW": 202.5, "SW": 225.0,  "WSW": 247.5,
    "W": 270.0,  "WNW": 292.5, "NW": 315.0,  "NNW": 337.5,
}


# ---------------------------------------------------------------------------
# HANDEDNESS SPLIT MAP
# ---------------------------------------------------------------------------

HAND_TO_SPLIT: dict[str, str] = {
    "R": "vs_rhp",
    "L": "vs_lhp",
}


# ---------------------------------------------------------------------------
# check_park_metadata_integrity
# Call once at job startup BEFORE any GCS reads.
# Raises ValueError if PARK_METADATA is missing any canonical team.
# canonical_teams must be provided independently — never pass set(PARK_METADATA.keys()).
# ---------------------------------------------------------------------------

def check_park_metadata_integrity(canonical_teams: frozenset[str]) -> None:
    """Hard-fail on startup if PARK_METADATA is missing any canonical MLB team."""
    missing = canonical_teams - set(PARK_METADATA.keys())
    if missing:
        raise ValueError(
            f"PARK_METADATA is missing canonical teams: {sorted(missing)}. "
            "Update lib/logic/mlb_scorer.py PARK_METADATA to match "
            "scrapers/mlb/names.py canonical team names exactly."
        )
    extra = set(PARK_METADATA.keys()) - canonical_teams
    if extra:
        # Extra keys are not fatal but indicate stale metadata (e.g. old franchise name).
        # Caller should log this as WARNING.
        raise ValueError(
            f"PARK_METADATA contains non-canonical team keys: {sorted(extra)}. "
            "Remove stale entries or update CANONICAL_MLB_TEAMS."
        )


# ---------------------------------------------------------------------------
# normalize_pitcher_name
# Strips accents, lowercases, removes punctuation and common suffixes.
# Used as the Fangraphs name join key — Fangraphs has no shared ID with MLB API.
# ---------------------------------------------------------------------------

def normalize_pitcher_name(name: Optional[str]) -> str:
    """Return a normalized join key for pitcher name matching across sources."""
    if not name:
        return ""
    # NFD decompose then strip combining marks (removes accents).
    # José Berríos -> jose berrios, Shohei Ohtani -> shohei ohtani
    normalized = "".join(
        c for c in unicodedata.normalize("NFD", name)
        if unicodedata.category(c) != "Mn"
    )
    normalized = normalized.lower()
    normalized = normalized.replace(".", "")
    # Strip common name suffixes that Fangraphs may include or omit inconsistently.
    for suffix in (" jr", " sr", " ii", " iii", " iv"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
    return normalized.strip()


# ---------------------------------------------------------------------------
# calculate_wind_effect
# Returns (multiplier: float, status: str).
# The caller MUST use status in the output components block — never hardcode "calculated".
#
# Status values:
#   "calculated"                — vector math ran; multiplier is meaningful
#   "calm"                      — wind < 2 mph; effect treated as zero
#   "no_wind_data"              — wind_mph is None
#   "skipped_unknown_direction" — direction string not in WIND_DIR_MAP
#   "dome"                      — not returned here; caller sets this for dome games
#
# Heuristic coefficients:
#   10 mph dead out (vector=1.0)  ≈ +10% run environment shift
#   10 mph dead in  (vector=-1.0) ≈  -8% run environment shift
#   Asymmetry reflects that balls in play are more suppressed than boosted
#   per unit of headwind vs tailwind.
#   Source: BallparkPal methodology approximation.
#   Tune against historical Coors/Oracle/Wrigley splits once season data accrues.
# ---------------------------------------------------------------------------

def calculate_wind_effect(
    wind_mph: Optional[float],
    wind_dir_str: Optional[str],
    cf_deg: int,
) -> tuple[float, str]:
    """Calculate run environment multiplier from wind vector relative to CF axis."""
    if wind_mph is None:
        return 1.0, "no_wind_data"
    if wind_mph < 2.0:
        return 1.0, "calm"
    if not wind_dir_str or wind_dir_str not in WIND_DIR_MAP:
        return 1.0, "skipped_unknown_direction"

    wind_deg = WIND_DIR_MAP[wind_dir_str]
    # cos(diff): +1 = blowing dead out toward CF, -1 = blowing dead in from CF
    angle_diff = math.radians(wind_deg - cf_deg)
    vector = math.cos(angle_diff)

    if vector > 0:
        # Tailwind (blowing out): boost runs
        multiplier = 1.0 + (vector * wind_mph * 0.01)
    else:
        # Headwind (blowing in): suppress runs (smaller coefficient — asymmetric)
        multiplier = 1.0 + (vector * wind_mph * 0.008)

    return round(multiplier, 4), "calculated"


# ---------------------------------------------------------------------------
# calculate_temp_effect
# Standard carry heuristic: ~1% run shift per 10°F from neutral (70°F).
# Moist air is slightly less dense (aids carry); not modelled here — temp
# is the dominant factor and humidity effect is second-order.
# ---------------------------------------------------------------------------

def calculate_temp_effect(temp_f: Optional[float]) -> float:
    """Return run environment multiplier from temperature relative to 70°F neutral."""
    if temp_f is None:
        return 1.0
    return round(1.0 + ((temp_f - 70.0) / 10.0 * 0.01), 4)


# ---------------------------------------------------------------------------
# get_team_split
# Returns (record | None, split_used: str, fallback_reason: str | None).
# Caller provides the opposing pitcher's hand to determine which split to use.
# ---------------------------------------------------------------------------

def get_team_split(
    team: str,
    opposing_pitcher_hand: Optional[str],
    teams_lookup: dict,
) -> tuple:
    """
    Look up a team's offensive split record based on the opposing pitcher's hand.

    Returns:
        (MlbTeamSplit | None, split_key: str, fallback_reason: str | None)

    fallback_reason is non-None when the preferred split was unavailable or
    the pitcher hand was unknown/switch — useful for per-game diagnostics.
    """
    preferred_split = HAND_TO_SPLIT.get(opposing_pitcher_hand or "")
    fallback_reason: Optional[str] = None

    if preferred_split is None:
        # Unknown or switch-pitcher hand — go straight to overall
        preferred_split = "overall"
        fallback_reason = f"hand_{opposing_pitcher_hand}_no_split"

    record = teams_lookup.get((team, preferred_split))

    if record is None and preferred_split != "overall":
        # Preferred handedness split not found (pre-season or data gap) — fall back
        record = teams_lookup.get((team, "overall"))
        fallback_reason = f"split_{preferred_split}_not_found"

    return record, preferred_split, fallback_reason


# ---------------------------------------------------------------------------
# compute_source_join_rate
# Returns fraction of provided objects that are non-None (0.0–1.0).
# This measures source-join coverage, not field-level stat population.
# Name is intentionally specific to avoid confusion with field-level coverage.
# ---------------------------------------------------------------------------

def compute_source_join_rate(objects: list) -> float:
    """Return the fraction of join results that are non-None."""
    if not objects:
        return 0.0
    populated = sum(1 for o in objects if o is not None)
    return round(populated / len(objects), 3)
