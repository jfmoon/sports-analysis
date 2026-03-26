# jobs/mlb_projector.py
# MLB Transform layer — reads all MLB GCS snapshots, projects game context,
# writes mlb/projections.json to sports-processed-data-491116.
#
# Run as module (required for lib/ relative imports):
#   python -m jobs.mlb_projector
#
# Trigger: any mlb/* GCS path. Spine is always mlb/probables.json.

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

from lib.storage import AnalysisStorage
from lib.logic.mlb_scorer import (
    CANONICAL_MLB_TEAMS,
    PARK_METADATA,
    calculate_temp_effect,
    calculate_wind_effect,
    check_park_metadata_integrity,
    compute_source_join_rate,
    get_team_split,
    normalize_pitcher_name,
)
from lib.schemas.inputs import (
    MlbBullpenSnapshot,
    MlbLineupsSnapshot,
    MlbOddsSnapshot,
    MlbPitchersSnapshot,
    MlbProbablesSnapshot,
    MlbStatcastHittersSnapshot,
    MlbStatcastPitchersSnapshot,
    MlbTeamsSnapshot,
    MlbWeatherSnapshot,
)


# ---------------------------------------------------------------------------
# Valid trigger paths for this job
# ---------------------------------------------------------------------------

VALID_TRIGGERS = frozenset({
    "mlb/probables.json",
    "mlb/odds.json",
    "mlb/pitchers.json",
    "mlb/teams.json",
    "mlb/bullpen.json",
    "mlb/statcast_pitchers.json",
    "mlb/statcast_hitters.json",
    "mlb/weather.json",
    "mlb/lineups.json",
})

# Staleness threshold for spine (probables) when triggered by a secondary source
SPINE_STALE_SECONDS = 4 * 60 * 60  # 4 hours


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def setup_logger(name: str) -> logging.Logger:
    """Configure stdout logger with message-ID prefix. Copy of cbb_projector pattern."""
    msg_id = os.environ.get("TRIGGER_MESSAGE_ID", "unknown")
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(f"[{msg_id}] %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    return logger


def _safe_read(storage: AnalysisStorage, path: str, logger: logging.Logger):
    """Read a secondary GCS snapshot. Returns None on any failure — never raises."""
    try:
        return storage.read_snapshot(path)
    except Exception as exc:
        logger.warning("Could not read %s: %s — continuing without it", path, exc)
        return None


def _check_spine_staleness(probables: MlbProbablesSnapshot, trigger_path: str, logger: logging.Logger) -> None:
    """Warn if the probables spine is stale when triggered by a secondary source."""
    if trigger_path == "mlb/probables.json":
        return
    if not probables.generated_at:
        return
    try:
        gen_time = datetime.fromisoformat(probables.generated_at)
        # Ensure tz-aware for safe comparison
        if gen_time.tzinfo is None:
            gen_time = gen_time.replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        age_seconds = (now - gen_time).total_seconds()
        if age_seconds > SPINE_STALE_SECONDS:
            logger.warning(
                "Probables spine is stale: generated_at=%s (%.1f hours ago). "
                "Triggered by %s — projections may be for a stale schedule.",
                probables.generated_at,
                age_seconds / 3600,
                trigger_path,
            )
    except (ValueError, TypeError) as exc:
        logger.warning("Could not parse probables.generated_at '%s': %s", probables.generated_at, exc)


def _build_data_quality(
    missing: list[str],
    fg_is_preseason: bool,
    sc_is_preseason: bool,
) -> str:
    """
    Derive a single data_quality label from the missing sources list.

    Priority order (first match wins):
      preseason_empty → no_statcast → no_fangraphs → probables_only → no_odds → partial → full
    """
    if not missing:
        return "full"
    missing_set = set(missing)
    if fg_is_preseason or sc_is_preseason:
        return "preseason_empty"
    if missing_set <= {"statcast_pitchers", "statcast_hitters"}:
        return "no_statcast"
    if missing_set == {"fangraphs"}:
        return "no_fangraphs"
    if missing_set == {"odds"}:
        return "no_odds"
    # probables_only: all secondaries absent
    all_secondaries = {
        "fangraphs", "statcast_pitchers", "statcast_hitters",
        "teams", "bullpen", "weather", "lineups", "odds",
    }
    if missing_set >= all_secondaries:
        return "probables_only"
    return "partial"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger = setup_logger("mlb_projector")
    msg_id = os.environ.get("TRIGGER_MESSAGE_ID", "unknown")
    trigger_path = os.environ.get("TRIGGER_GCS_PATH", "")

    # 1. Validate trigger path
    if trigger_path not in VALID_TRIGGERS:
        logger.error("Invalid trigger path: '%s'. Expected one of %s", trigger_path, sorted(VALID_TRIGGERS))
        sys.exit(1)

    # 2. Park metadata integrity check — must pass before any GCS I/O
    try:
        check_park_metadata_integrity(CANONICAL_MLB_TEAMS)
    except ValueError as exc:
        logger.error("Park metadata integrity failure: %s", exc)
        sys.exit(1)

    storage = AnalysisStorage.from_env()

    # 3. Read spine (probables)
    try:
        if trigger_path == "mlb/probables.json":
            probables: MlbProbablesSnapshot = storage.read_trigger_snapshot()
        else:
            probables: MlbProbablesSnapshot = storage.read_snapshot("mlb/probables.json")
    except Exception as exc:
        logger.exception("Fatal: could not read probables spine: %s", exc)
        sys.exit(1)

    # 4. Schema version guard on spine
    if probables.schema_version < 1:
        logger.warning(
            "[%s] schema_version=%d on mlb/probables.json — expected >= 1; snapshot may be stale",
            msg_id, probables.schema_version,
        )

    # 5. Spine staleness check
    _check_spine_staleness(probables, trigger_path, logger)

    if not probables.games:
        logger.warning("Probables snapshot contains 0 games — writing empty projection")

    # 6. Read all secondaries (never fatal)
    p_snap: MlbPitchersSnapshot | None        = _safe_read(storage, "mlb/pitchers.json", logger)
    t_snap: MlbTeamsSnapshot | None           = _safe_read(storage, "mlb/teams.json", logger)
    b_snap: MlbBullpenSnapshot | None         = _safe_read(storage, "mlb/bullpen.json", logger)
    sc_p_snap: MlbStatcastPitchersSnapshot | None = _safe_read(storage, "mlb/statcast_pitchers.json", logger)
    sc_h_snap: MlbStatcastHittersSnapshot | None  = _safe_read(storage, "mlb/statcast_hitters.json", logger)
    w_snap: MlbWeatherSnapshot | None         = _safe_read(storage, "mlb/weather.json", logger)
    l_snap: MlbLineupsSnapshot | None         = _safe_read(storage, "mlb/lineups.json", logger)
    o_snap: MlbOddsSnapshot | None            = _safe_read(storage, "mlb/odds.json", logger)

    # 7. Pre-season empty detection
    # A snapshot that loaded successfully but contains 0 records is a distinct
    # state from a failed read. Both return empty lookup dicts, but data_quality
    # labelling differs: "preseason_empty" vs "partial".
    pitchers_list  = getattr(p_snap, "pitchers", []) or []
    teams_list     = getattr(t_snap, "teams", []) or []
    bullpen_list   = getattr(b_snap, "bullpens", []) or []
    sc_p_list      = getattr(sc_p_snap, "pitchers", []) or []
    sc_h_list      = getattr(sc_h_snap, "hitters", []) or []
    weather_list   = getattr(w_snap, "games", []) or []
    lineups_list   = getattr(l_snap, "games", []) or []
    odds_list      = getattr(o_snap, "odds", []) or []

    fg_is_preseason = (p_snap is not None and len(pitchers_list) == 0)
    sc_is_preseason = (sc_p_snap is not None and len(sc_p_list) == 0)

    # 8. Build lookup dicts — all secondary lists may be empty, never crash
    # Fangraphs: name-based join (normalize both sides)
    p_lookup    = {normalize_pitcher_name(p.name): p for p in pitchers_list}
    t_lookup    = {(t.team, t.split): t for t in teams_list}
    b_lookup    = {b.team: b for b in bullpen_list}
    # Statcast: ID-based join — cast probables pitcher_id (int) to str at join time
    sc_p_lookup = {str(p.player_id): p for p in sc_p_list}
    sc_h_lookup = {str(h.player_id): h for h in sc_h_list}
    w_lookup    = {w.game_id: w for w in weather_list}
    l_lookup    = {lg.game_id: lg for lg in lineups_list}
    # Odds: raw Action Network team names — no canonical crosswalk yet
    o_lookup    = {(o.away_team, o.home_team): o for o in odds_list}

    # Track envelope-level source availability
    source_map = {
        "probables":          len(probables.games) > 0,
        "pitchers":           len(pitchers_list) > 0,
        "teams":              len(teams_list) > 0,
        "bullpen":            len(bullpen_list) > 0,
        "statcast_pitchers":  len(sc_p_list) > 0,
        "statcast_hitters":   len(sc_h_list) > 0,
        "weather":            len(weather_list) > 0,
        "lineups":            len(lineups_list) > 0,
        "odds":               len(odds_list) > 0,
    }

    # 9. Per-game projection loop
    games_output: list[dict] = []
    matched_odds_count = 0
    total_games = len(probables.games)

    for game in probables.games:
        try:
            _project_game(
                game=game,
                p_lookup=p_lookup,
                t_lookup=t_lookup,
                b_lookup=b_lookup,
                sc_p_lookup=sc_p_lookup,
                sc_h_lookup=sc_h_lookup,
                w_lookup=w_lookup,
                l_lookup=l_lookup,
                o_lookup=o_lookup,
                fg_is_preseason=fg_is_preseason,
                sc_is_preseason=sc_is_preseason,
                games_output=games_output,
                logger=logger,
            )
            if games_output and games_output[-1].get("odds") is not None:
                matched_odds_count += 1

        except Exception as exc:
            logger.error(
                "Failed to process game %s (%s @ %s): %s",
                game.game_id, game.away_team, game.home_team, exc,
                exc_info=True,
            )
            games_output.append({
                "game_id": game.game_id,
                "date": game.date,
                "away_team": game.away_team,
                "home_team": game.home_team,
                "data_quality": "error",
                "missing_sources": ["processing_error"],
                "error": str(exc),
            })

    # 10. Envelope
    odds_match_rate = round(matched_odds_count / total_games, 3) if total_games > 0 else 0.0

    output = {
        "updated": datetime.now(tz=timezone.utc).isoformat(),
        "date": probables.games[0].date if probables.games else None,
        "spine_generated_at": probables.generated_at,
        "game_count": len(games_output),
        "odds_match_rate": odds_match_rate,
        "data_sources_available": [k for k, v in source_map.items() if v],
        "data_sources_missing": [k for k, v in source_map.items() if not v],
        "games": games_output,
    }

    # 11. Odds match rate alert — log before write so operator sees it before output is consumed
    if total_games > 0 and odds_match_rate < 0.5:
        logger.error(
            "Odds match rate %.2f is critically low (%d/%d games matched). "
            "Likely systematic Action Network team name mismatch. "
            "Check raw odds team names against canonical MLB team names.",
            odds_match_rate, matched_odds_count, total_games,
        )

    # 12. Write
    try:
        storage.write_processed("mlb/projections.json", output)
        storage.write_processed_archive("mlb/projections.json", output)
    except Exception as exc:
        logger.exception("Fatal: could not write output: %s", exc)
        sys.exit(1)

    # 13. Summary log
    dq_distribution = {}
    for g in games_output:
        dq = g.get("data_quality", "unknown")
        dq_distribution[dq] = dq_distribution.get(dq, 0) + 1

    logger.info(
        "MLB projector complete: %d games projected. "
        "odds_match_rate=%.2f. data_quality=%s",
        len(games_output),
        odds_match_rate,
        dq_distribution,
    )


# ---------------------------------------------------------------------------
# _project_game — per-game logic extracted for testability and loop safety
# ---------------------------------------------------------------------------

def _project_game(
    game,
    p_lookup: dict,
    t_lookup: dict,
    b_lookup: dict,
    sc_p_lookup: dict,
    sc_h_lookup: dict,
    w_lookup: dict,
    l_lookup: dict,
    o_lookup: dict,
    fg_is_preseason: bool,
    sc_is_preseason: bool,
    games_output: list,
    logger: logging.Logger,
) -> None:
    """Build and append a single game projection record. Raises on unrecoverable error."""

    # --- Pitcher joins (Fangraphs: name, Statcast: ID) ---

    away_fg = p_lookup.get(normalize_pitcher_name(game.away_pitcher))
    away_pitcher_miss_reason: str | None = None
    if game.away_pitcher and not away_fg:
        away_pitcher_miss_reason = "preseason" if fg_is_preseason else "name_mismatch"
        if away_pitcher_miss_reason == "name_mismatch":
            logger.warning(
                "Fangraphs join miss: pitcher '%s' not found (game %s %s @ %s)",
                game.away_pitcher, game.game_id, game.away_team, game.home_team,
            )

    home_fg = p_lookup.get(normalize_pitcher_name(game.home_pitcher))
    home_pitcher_miss_reason: str | None = None
    if game.home_pitcher and not home_fg:
        home_pitcher_miss_reason = "preseason" if fg_is_preseason else "name_mismatch"
        if home_pitcher_miss_reason == "name_mismatch":
            logger.warning(
                "Fangraphs join miss: pitcher '%s' not found (game %s %s @ %s)",
                game.home_pitcher, game.game_id, game.away_team, game.home_team,
            )

    # Statcast: probables stores pitcher_id as int — cast to str for lookup
    away_sc = sc_p_lookup.get(str(game.away_pitcher_id)) if game.away_pitcher_id is not None else None
    home_sc = sc_p_lookup.get(str(game.home_pitcher_id)) if game.home_pitcher_id is not None else None

    # --- Offensive splits ---
    # Away team bats against home pitcher; home team bats against away pitcher.
    # Only use handedness splits when pitcher hand is confirmed in probables.
    away_off, away_split, away_split_fallback = get_team_split(
        game.away_team, game.home_hand, t_lookup
    )
    home_off, home_split, home_split_fallback = get_team_split(
        game.home_team, game.away_hand, t_lookup
    )

    # --- Bullpen ---
    away_bp = b_lookup.get(game.away_team)
    home_bp = b_lookup.get(game.home_team)

    # --- Weather and park factor ---
    weather = w_lookup.get(game.game_id)
    # home_team is always in PARK_METADATA after startup integrity check
    meta = PARK_METADATA[game.home_team]

    w_factor = 1.0
    wind_status = "no_weather"
    confidence = 1.0

    if weather is not None and weather.is_dome:
        wind_status = "dome"
        # Dome: no weather effect, full confidence
    elif weather is not None and not weather.is_dome:
        t_eff = calculate_temp_effect(weather.temperature_f)
        w_eff, wind_status = calculate_wind_effect(
            weather.wind_mph, weather.wind_direction, meta["cf_deg"]
        )
        combined = t_eff * w_eff
        dampen = 0.5 if weather.is_retractable else 1.0
        w_factor = 1.0 + ((combined - 1.0) * dampen)
        w_factor = round(w_factor, 4)
        if weather.is_retractable:
            confidence -= 0.2  # Roof state is a last-minute decision

    run_env_score = round(meta["base_pf"] * w_factor, 3)
    if run_env_score > 1.10:
        run_env_label = "High Scoring"
    elif run_env_score < 0.90:
        run_env_label = "Pitcher's Duel"
    else:
        run_env_label = "Neutral"

    # --- Lineup confirmation ---
    lineup_game = l_lookup.get(game.game_id)
    lineups_confirmed = (
        lineup_game.away_confirmed and lineup_game.home_confirmed
    ) if lineup_game is not None else False

    # --- Odds ---
    odds = o_lookup.get((game.away_team, game.home_team))
    if odds is None:
        logger.warning(
            "Odds miss: no match for (%s, %s) game %s",
            game.away_team, game.home_team, game.game_id,
        )

    # --- Missing sources and data quality ---
    missing: list[str] = []

    if not away_fg and not home_fg:
        missing.append("preseason_empty" if fg_is_preseason else "fangraphs")

    sc_pitcher_miss = not away_sc and not home_sc
    if sc_pitcher_miss:
        missing.append("statcast_pitchers" if not sc_is_preseason else "statcast_preseason")

    if not away_off and not home_off:
        missing.append("teams")
    if not away_bp and not home_bp:
        missing.append("bullpen")
    if weather is None:
        missing.append("weather")
    if lineup_game is None:
        missing.append("lineups")
    if odds is None:
        missing.append("odds")

    data_quality = _build_data_quality(missing, fg_is_preseason, sc_is_preseason)

    # --- Stat coverage (source-join rate, not field-level) ---
    stat_coverage_pct = compute_source_join_rate([
        away_fg, home_fg, away_off, home_off, away_sc, home_sc, away_bp, home_bp
    ])

    # --- Non-MLB team names (AAA / exhibition) ---
    # Sugar Land Space Cowboys, Sultanes de Monterrey etc. will miss teams/bullpen.
    # Already handled gracefully above — they land in missing_sources.

    # --- Assemble output record ---
    games_output.append({
        "game_id": game.game_id,
        "date": game.date,
        "commence_time": game.commence_time,
        "away_team": game.away_team,
        "home_team": game.home_team,
        "away_pitcher": game.away_pitcher,
        "home_pitcher": game.home_pitcher,
        "away_pitcher_hand": game.away_hand,
        "home_pitcher_hand": game.home_hand,
        "pitchers_confirmed": bool(game.away_confirmed and game.home_confirmed),
        "lineups_confirmed": lineups_confirmed,
        "weather": _build_weather_block(weather),
        "run_environment": {
            "score": run_env_score,
            "label": run_env_label,
            "components": {
                "base_park_factor": meta["base_pf"],
                "base_hr_factor": meta["base_hr"],
                "weather_adjustment": round(w_factor, 3),
                "wind_vector": wind_status,   # Never hardcoded — always from calculate_wind_effect
            },
            "confidence": round(confidence, 2),
        },
        # Away side
        "away_pitcher_stats": away_fg.model_dump(mode="json") if away_fg is not None else None,
        "away_pitcher_miss_reason": away_pitcher_miss_reason,
        "away_pitcher_statcast": away_sc.model_dump(mode="json") if away_sc is not None else None,
        "away_team_offense": _build_offense_block(away_off, away_split, away_split_fallback),
        "away_bullpen": away_bp.model_dump(mode="json") if away_bp is not None else None,
        # Home side
        "home_pitcher_stats": home_fg.model_dump(mode="json") if home_fg is not None else None,
        "home_pitcher_miss_reason": home_pitcher_miss_reason,
        "home_pitcher_statcast": home_sc.model_dump(mode="json") if home_sc is not None else None,
        "home_team_offense": _build_offense_block(home_off, home_split, home_split_fallback),
        "home_bullpen": home_bp.model_dump(mode="json") if home_bp is not None else None,
        # Odds
        "odds": _build_odds_block(odds),
        # Quality metadata
        "data_quality": data_quality,
        "missing_sources": missing,
        "stat_coverage_pct": stat_coverage_pct,
    })


def _build_weather_block(weather) -> dict:
    """Build the weather sub-dict for a game record. Safe against None input."""
    if weather is None:
        return {
            "temperature_f": None, "wind_mph": None, "wind_direction": None,
            "precip_pct": None, "conditions": None,
            "is_dome": None, "is_retractable": None, "weather_applied": False,
        }
    return {
        "temperature_f": weather.temperature_f,
        "wind_mph": weather.wind_mph,
        "wind_direction": weather.wind_direction,
        "precip_pct": weather.precip_pct,
        "conditions": weather.conditions,
        "is_dome": weather.is_dome,
        "is_retractable": weather.is_retractable,
        "weather_applied": not weather.is_dome,
    }


def _build_offense_block(offense, split: str, fallback_reason: str | None) -> dict | None:
    """Build the team offense sub-dict. Returns None if no record available."""
    if offense is None:
        return None
    return {
        "split": split,
        "split_fallback_reason": fallback_reason,
        # wOBA used for park-factor calculations (park-neutral raw production)
        "woba": offense.woba,
        "iso": offense.iso,
        "barrel_pct": offense.barrel_pct,
        "hard_hit_pct": offense.hard_hit_pct,
        "k_pct": offense.k_pct,
        "bb_pct": offense.bb_pct,
        # wrc_plus present for display/passthrough ONLY.
        # It is already park-adjusted — do NOT use in run environment calculations.
        "wrc_plus_display_only": offense.wrc_plus,
    }


def _build_odds_block(odds) -> dict | None:
    """Build the odds sub-dict. Returns None if no odds record matched."""
    if odds is None:
        return None
    return {
        "away_ml": odds.away_ml,
        "home_ml": odds.home_ml,
        "away_spread": odds.away_spread,
        "away_spread_odds": odds.away_spread_odds,
        "home_spread": odds.home_spread,
        "home_spread_odds": odds.home_spread_odds,
        "total": odds.total,
        "over_odds": odds.over_odds,
        "under_odds": odds.under_odds,
    }


def _build_data_quality(
    missing: list[str],
    fg_is_preseason: bool,
    sc_is_preseason: bool,
) -> str:
    """Derive data_quality label. Defined at module level for testability."""
    if not missing:
        return "full"
    missing_set = set(missing)
    if fg_is_preseason or sc_is_preseason or "preseason_empty" in missing_set or "statcast_preseason" in missing_set:
        return "preseason_empty"
    if missing_set <= {"statcast_pitchers", "statcast_hitters"}:
        return "no_statcast"
    if missing_set == {"fangraphs"}:
        return "no_fangraphs"
    if missing_set == {"odds"}:
        return "no_odds"
    all_secondaries = {
        "fangraphs", "statcast_pitchers", "teams",
        "bullpen", "weather", "lineups", "odds",
    }
    if missing_set >= all_secondaries:
        return "probables_only"
    return "partial"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Local test defaults — override with env vars as needed
    os.environ.setdefault("TRIGGER_GCS_BUCKET", "sports-data-scraper-491116")
    os.environ.setdefault("TRIGGER_GCS_PATH", "mlb/probables.json")
    os.environ.setdefault("TRIGGER_GCS_GEN", "0")
    os.environ.setdefault("TRIGGER_MESSAGE_ID", "local-mlb-test")
    main()
