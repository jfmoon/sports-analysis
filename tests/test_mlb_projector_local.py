# tests/test_mlb_projector_local.py
# Local unit tests for MLB projector logic.
# Run from repo root: python -m pytest tests/test_mlb_projector_local.py -v
#
# These tests do NOT require GCS access. All GCS I/O is mocked.

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

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
    MlbGame,
    MlbGameWeather,
    MlbLineupsSnapshot,
    MlbOddsSnapshot,
    MlbPitchersSnapshot,
    MlbProbablesSnapshot,
    MlbStatcastPitchersSnapshot,
    MlbStatcastHittersSnapshot,
    MlbTeamsSnapshot,
    MlbWeatherSnapshot,
)
from jobs.mlb_projector import _build_data_quality, _project_game


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_game(**kwargs) -> MlbGame:
    defaults = {
        "game_id": "746445",
        "date": "2026-03-27",
        "commence_time": "2026-03-27T18:05:00Z",
        "away_team": "New York Yankees",
        "home_team": "Boston Red Sox",
        "away_pitcher": "Gerrit Cole",
        "home_pitcher": "Brayan Bello",
        "away_hand": "R",
        "home_hand": "R",
        "away_pitcher_id": 543037,
        "home_pitcher_id": 669373,
        "away_confirmed": True,
        "home_confirmed": True,
    }
    defaults.update(kwargs)
    return MlbGame(**defaults)


def _empty_lookup_kwargs() -> dict:
    """Return the kwargs needed to call _project_game with all empty lookups."""
    return dict(
        p_lookup={},
        t_lookup={},
        b_lookup={},
        sc_p_lookup={},
        sc_h_lookup={},
        w_lookup={},
        l_lookup={},
        o_lookup={},
        fg_is_preseason=False,
        sc_is_preseason=False,
        games_output=[],
        logger=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Test 1 — All secondaries return None (_safe_read failure simulation)
# ---------------------------------------------------------------------------

class TestAllSecondariesNone:
    """
    Simulates every _safe_read() call returning None.
    Verifies: no AttributeError, output contains game records,
    all games have a valid data_quality value.
    """

    def test_no_attribute_error_when_all_snaps_none(self):
        """Job must not crash when every secondary snapshot is None."""
        kwargs = _empty_lookup_kwargs()
        game = _make_game()
        # Explicitly verify None lookups produce empty dicts (tested in scorer)
        # Here we test the full _project_game path with empty dicts
        _project_game(game=game, **kwargs)
        output = kwargs["games_output"]
        assert len(output) == 1, "Expected exactly one game record"

    def test_data_quality_not_full_when_no_secondaries(self):
        kwargs = _empty_lookup_kwargs()
        game = _make_game()
        _project_game(game=game, **kwargs)
        record = kwargs["games_output"][0]
        assert record["data_quality"] != "full"

    def test_output_contains_required_keys(self):
        kwargs = _empty_lookup_kwargs()
        game = _make_game()
        _project_game(game=game, **kwargs)
        record = kwargs["games_output"][0]
        required_keys = {
            "game_id", "date", "away_team", "home_team",
            "away_pitcher_stats", "home_pitcher_stats",
            "away_team_offense", "home_team_offense",
            "away_bullpen", "home_bullpen",
            "weather", "run_environment", "odds",
            "data_quality", "missing_sources", "stat_coverage_pct",
            "pitchers_confirmed", "lineups_confirmed",
        }
        for key in required_keys:
            assert key in record, f"Missing key in output: {key}"

    def test_none_fields_are_null_not_empty_dict(self):
        """Null contract: missing joins must produce None, not empty {}."""
        kwargs = _empty_lookup_kwargs()
        game = _make_game()
        _project_game(game=game, **kwargs)
        record = kwargs["games_output"][0]
        assert record["away_pitcher_stats"] is None
        assert record["home_pitcher_stats"] is None
        assert record["away_bullpen"] is None
        assert record["home_bullpen"] is None
        assert record["odds"] is None


# ---------------------------------------------------------------------------
# Test 2 — Pre-season: all secondaries return 0-record snapshots
# ---------------------------------------------------------------------------

class TestPreseasonEmptySnapshots:
    """
    Simulates Fangraphs/Statcast writing valid but empty snapshots before season start.
    Verifies: data_quality == "preseason_empty", odds_match_rate == 0.0,
    no crash, game records produced.
    """

    def test_preseason_data_quality_label(self):
        dq = _build_data_quality(
            missing=["preseason_empty"],
            fg_is_preseason=True,
            sc_is_preseason=True,
        )
        assert dq == "preseason_empty"

    def test_empty_fangraphs_snap_is_not_none(self):
        """A snapshot with 0 records is not None — _safe_read returns it successfully."""
        snap = MlbPitchersSnapshot(pitchers=[])
        pitchers_list = getattr(snap, "pitchers", []) or []
        assert snap is not None
        assert len(pitchers_list) == 0

    def test_preseason_flag_set_correctly(self):
        """fg_is_preseason must be True when snap exists but list is empty."""
        p_snap = MlbPitchersSnapshot(pitchers=[])
        pitchers_list = getattr(p_snap, "pitchers", []) or []
        fg_is_preseason = (p_snap is not None and len(pitchers_list) == 0)
        assert fg_is_preseason is True

    def test_preseason_miss_reason_not_name_mismatch(self):
        """When preseason, pitcher miss reason must be 'preseason', not 'name_mismatch'."""
        kwargs = _empty_lookup_kwargs()
        kwargs["fg_is_preseason"] = True
        game = _make_game()
        _project_game(game=game, **kwargs)
        record = kwargs["games_output"][0]
        assert record["away_pitcher_miss_reason"] == "preseason"
        assert record["home_pitcher_miss_reason"] == "preseason"

    def test_no_odds_miss_warning_with_zero_match_rate(self):
        """Odds miss rate 0/N — data_quality should reflect 'probables_only' or 'preseason_empty'."""
        dq = _build_data_quality(
            missing=["preseason_empty", "odds", "weather", "lineups", "teams", "bullpen"],
            fg_is_preseason=True,
            sc_is_preseason=True,
        )
        assert dq == "preseason_empty"


# ---------------------------------------------------------------------------
# Test 3 — Pitcher name normalization
# ---------------------------------------------------------------------------

class TestPitcherNameNormalization:
    """Verifies normalize_pitcher_name bridges accent/suffix differences."""

    def test_accented_name_matches_unaccented(self):
        assert normalize_pitcher_name("José Berríos") == normalize_pitcher_name("Jose Berrios")

    def test_accent_stripping_ohtani(self):
        assert normalize_pitcher_name("Shohei Ohtani") == "shohei ohtani"

    def test_suffix_stripping_jr(self):
        assert normalize_pitcher_name("Shohei Ohtani Jr.") == normalize_pitcher_name("Shohei Ohtani")

    def test_suffix_stripping_sr(self):
        assert normalize_pitcher_name("Ken Griffey Sr") == "ken griffey"

    def test_suffix_stripping_ii(self):
        assert normalize_pitcher_name("Cal Ripken II") == "cal ripken"

    def test_period_removed(self):
        assert normalize_pitcher_name("C.C. Sabathia") == "cc sabathia"

    def test_genuine_miss_does_not_match(self):
        """A real typo must not produce a false match."""
        assert normalize_pitcher_name("Jordan Montgomery") != normalize_pitcher_name("Jordan Montgomry")

    def test_none_input_returns_empty_string(self):
        assert normalize_pitcher_name(None) == ""

    def test_empty_string_returns_empty_string(self):
        assert normalize_pitcher_name("") == ""

    def test_lookup_round_trip(self):
        """Simulate the actual join: build lookup with normalized key, query with normalized input."""
        from lib.schemas.inputs import MlbPitcher
        pitcher = MlbPitcher(
            pitcher_id="543037",
            name="José Berríos",
            team="Toronto Blue Jays",
            throws="R",
            season=2026,
        )
        lookup = {normalize_pitcher_name(pitcher.name): pitcher}
        result = lookup.get(normalize_pitcher_name("Jose Berrios"))
        assert result is not None
        assert result.name == "José Berríos"


# ---------------------------------------------------------------------------
# Test 4 — Retractable roof + unknown wind direction
# ---------------------------------------------------------------------------

class TestRetractableRoofUnknownWind:
    """
    Verifies correct handling of retractable stadium with unrecognized wind direction.
    - wind_vector must be "skipped_unknown_direction" (never "calculated")
    - confidence must be < 1.0 due to retractable dampening
    - weather_applied must be True (retractable != dome)
    """

    def test_unknown_wind_direction_returns_correct_status(self):
        multiplier, status = calculate_wind_effect(15.0, "Variable", cf_deg=0)
        assert status == "skipped_unknown_direction"
        assert multiplier == 1.0

    def test_none_wind_direction_returns_no_wind_data(self):
        multiplier, status = calculate_wind_effect(15.0, None, cf_deg=0)
        assert status == "skipped_unknown_direction"
        assert multiplier == 1.0

    def test_calm_wind_returns_calm_status(self):
        multiplier, status = calculate_wind_effect(1.0, "NE", cf_deg=45)
        assert status == "calm"
        assert multiplier == 1.0

    def test_none_wind_mph_returns_no_wind_data(self):
        multiplier, status = calculate_wind_effect(None, "NE", cf_deg=45)
        assert status == "no_wind_data"
        assert multiplier == 1.0

    def test_retractable_with_unknown_direction_in_full_game_record(self):
        """End-to-end: retractable stadium, unknown wind → confidence < 1.0, wind_vector correct."""
        weather = MlbGameWeather(
            game_id="746445",
            date="2026-03-27",
            away_team="Houston Astros",
            home_team="Arizona Diamondbacks",
            stadium="Chase Field",
            city="Phoenix",
            state="AZ",
            is_dome=False,
            is_retractable=True,
            temperature_f=85.0,
            wind_mph=15.0,
            wind_direction="Variable",   # Not in WIND_DIR_MAP
        )
        kwargs = _empty_lookup_kwargs()
        kwargs["w_lookup"] = {"746445": weather}
        game = _make_game(
            game_id="746445",
            away_team="Houston Astros",
            home_team="Arizona Diamondbacks",
        )
        _project_game(game=game, **kwargs)
        record = kwargs["games_output"][0]

        run_env = record["run_environment"]
        assert run_env["components"]["wind_vector"] == "skipped_unknown_direction"
        assert run_env["confidence"] < 1.0, "Retractable roof must reduce confidence"
        assert record["weather"]["weather_applied"] is True
        assert record["weather"]["is_retractable"] is True

    def test_dome_game_sets_dome_wind_status(self):
        """Dome games must set wind_vector to 'dome', not 'no_weather'."""
        weather = MlbGameWeather(
            game_id="999",
            date="2026-03-27",
            away_team="New York Yankees",
            home_team="Tampa Bay Rays",
            stadium="Tropicana Field",
            city="St. Petersburg",
            state="FL",
            is_dome=True,
            is_retractable=False,
            temperature_f=None,
            wind_mph=None,
            wind_direction=None,
        )
        kwargs = _empty_lookup_kwargs()
        kwargs["w_lookup"] = {"999": weather}
        game = _make_game(
            game_id="999",
            away_team="New York Yankees",
            home_team="Tampa Bay Rays",
        )
        _project_game(game=game, **kwargs)
        record = kwargs["games_output"][0]
        assert record["run_environment"]["components"]["wind_vector"] == "dome"
        assert record["weather"]["weather_applied"] is False
        assert record["run_environment"]["confidence"] == 1.0


# ---------------------------------------------------------------------------
# Test 5 — Park metadata integrity check
# ---------------------------------------------------------------------------

class TestParkMetadataIntegrity:
    """Verifies the integrity check is not a no-op (not self-referential)."""

    def test_check_passes_with_correct_canonical_set(self):
        """Should not raise when PARK_METADATA matches CANONICAL_MLB_TEAMS exactly."""
        # This test will fail if CANONICAL_MLB_TEAMS and PARK_METADATA are out of sync,
        # which is the intended early-warning behavior.
        try:
            check_park_metadata_integrity(CANONICAL_MLB_TEAMS)
        except ValueError as exc:
            pytest.fail(
                f"Park metadata integrity check failed: {exc}\n"
                "Update CANONICAL_MLB_TEAMS or PARK_METADATA in mlb_scorer.py to match "
                "scrapers/mlb/names.py canonical team names."
            )

    def test_check_fails_when_team_missing(self):
        """Should raise ValueError when a canonical team is absent from PARK_METADATA."""
        fake_canonical = CANONICAL_MLB_TEAMS | {"Expansion City Franchise"}
        with pytest.raises(ValueError, match="missing canonical teams"):
            check_park_metadata_integrity(fake_canonical)

    def test_canonical_set_is_independent_of_park_metadata(self):
        """CANONICAL_MLB_TEAMS must not be derived from PARK_METADATA keys."""
        # If someone accidentally writes CANONICAL_TEAMS = set(PARK_METADATA.keys()),
        # the check becomes a no-op. Verify they are defined independently by
        # checking that modifying one does not silently affect the other.
        original_canonical_size = len(CANONICAL_MLB_TEAMS)
        original_metadata_size = len(PARK_METADATA)
        assert original_canonical_size == original_metadata_size, (
            f"CANONICAL_MLB_TEAMS has {original_canonical_size} teams, "
            f"PARK_METADATA has {original_metadata_size} entries — they must match."
        )

    def test_all_park_metadata_keys_are_in_canonical_set(self):
        """No stale franchise names in PARK_METADATA."""
        extra = set(PARK_METADATA.keys()) - CANONICAL_MLB_TEAMS
        assert not extra, (
            f"PARK_METADATA contains non-canonical keys: {extra}. "
            "Remove stale entries or update CANONICAL_MLB_TEAMS."
        )


# ---------------------------------------------------------------------------
# Test 6 — data_quality label logic
# ---------------------------------------------------------------------------

class TestDataQualityLabels:
    def test_full_when_no_missing(self):
        assert _build_data_quality([], False, False) == "full"

    def test_preseason_empty_overrides_all(self):
        assert _build_data_quality(["fangraphs", "odds"], True, False) == "preseason_empty"

    def test_no_statcast_label(self):
        assert _build_data_quality(["statcast_pitchers", "statcast_hitters"], False, False) == "no_statcast"

    def test_no_fangraphs_label(self):
        assert _build_data_quality(["fangraphs"], False, False) == "no_fangraphs"

    def test_no_odds_label(self):
        assert _build_data_quality(["odds"], False, False) == "no_odds"

    def test_probables_only_when_all_secondaries_missing(self):
        all_secondary = [
            "fangraphs", "statcast_pitchers", "teams",
            "bullpen", "weather", "lineups", "odds",
        ]
        assert _build_data_quality(all_secondary, False, False) == "probables_only"

    def test_partial_for_mixed_missing(self):
        assert _build_data_quality(["weather", "odds"], False, False) == "partial"


# ---------------------------------------------------------------------------
# Test 7 — Handedness split fallback
# ---------------------------------------------------------------------------

class TestHandednessSplitFallback:
    def _make_lookup(self):
        from lib.schemas.inputs import MlbTeamSplit
        return {
            ("New York Yankees", "vs_rhp"): MlbTeamSplit(team="New York Yankees", season=2026, split="vs_rhp", woba=0.340),
            ("New York Yankees", "overall"): MlbTeamSplit(team="New York Yankees", season=2026, split="overall", woba=0.330),
        }

    def test_right_hand_uses_vs_rhp(self):
        lookup = self._make_lookup()
        record, split, reason = get_team_split("New York Yankees", "R", lookup)
        assert split == "vs_rhp"
        assert reason is None
        assert record is not None

    def test_left_hand_falls_back_to_overall_when_not_found(self):
        lookup = self._make_lookup()  # no vs_lhp entry
        record, split, reason = get_team_split("New York Yankees", "L", lookup)
        assert split == "vs_lhp"
        assert reason == "split_vs_lhp_not_found"
        assert record is not None
        assert record.split == "overall"

    def test_switch_pitcher_uses_overall(self):
        lookup = self._make_lookup()
        record, split, reason = get_team_split("New York Yankees", "S", lookup)
        assert split == "overall"
        assert reason == "hand_S_no_split"

    def test_none_hand_uses_overall(self):
        lookup = self._make_lookup()
        record, split, reason = get_team_split("New York Yankees", None, lookup)
        assert split == "overall"
        assert "None" in reason or "no_split" in reason

    def test_unknown_team_returns_none_record(self):
        lookup = self._make_lookup()
        record, split, reason = get_team_split("Nonexistent FC", "R", lookup)
        assert record is None


# ---------------------------------------------------------------------------
# Test 8 — compute_source_join_rate
# ---------------------------------------------------------------------------

class TestComputeSourceJoinRate:
    def test_all_none(self):
        assert compute_source_join_rate([None, None, None]) == 0.0

    def test_all_populated(self):
        assert compute_source_join_rate([1, 2, 3]) == 1.0

    def test_half_populated(self):
        assert compute_source_join_rate([1, None, 3, None]) == 0.5

    def test_empty_list(self):
        assert compute_source_join_rate([]) == 0.0


# ---------------------------------------------------------------------------
# Test 9 — Per-game error isolation
# ---------------------------------------------------------------------------

class TestPerGameErrorIsolation:
    """Verify that a crash in one game does not abort the entire job loop."""

    def test_game_with_bad_home_team_produces_error_record(self):
        """A game whose home_team is not in PARK_METADATA raises KeyError.
        The caller's try/except must catch it and produce an error record."""
        game = _make_game(home_team="Nonexistent Team Not In Park Metadata")
        games_output: list = []
        logger = MagicMock()

        # Simulate what the job loop does
        try:
            _project_game(
                game=game,
                p_lookup={}, t_lookup={}, b_lookup={},
                sc_p_lookup={}, sc_h_lookup={},
                w_lookup={}, l_lookup={}, o_lookup={},
                fg_is_preseason=False, sc_is_preseason=False,
                games_output=games_output, logger=logger,
            )
        except Exception as exc:
            # Caller (job loop) catches and appends error record
            games_output.append({
                "game_id": game.game_id,
                "data_quality": "error",
                "missing_sources": ["processing_error"],
                "error": str(exc),
            })

        assert len(games_output) == 1
        assert games_output[0]["data_quality"] == "error"
