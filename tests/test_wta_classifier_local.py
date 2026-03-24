"""
Local smoke test for the WTA archetype classifier.

Run from the repo root:
    python tests/test_wta_classifier_local.py

Does NOT require GCS access. Exercises:
  1. compute_ratings_from_raw() in wta_mapper.py directly
  2. The full classifier pipeline (mapper → WTARatings → classify_player)
  3. Backward compatibility: legacy pre-scored ratings dicts still work
  4. Edge cases: empty raw_stats, missing keys, zero values
"""

import sys
from datetime import datetime, timezone

sys.path.insert(0, ".")

from lib.logic.wta_classifier import classify_player, ARCHETYPE_DEFINITIONS
from lib.logic.wta_mapper import compute_ratings_from_raw
from lib.schemas.wta import WTARatings, WTAArchetypeSnapshot


# ---------------------------------------------------------------------------
# Test fixtures — raw_stats format (new scraper output)
# Values are plausible WTA career floats matching tennisabstract_scraper.py keys.
# ---------------------------------------------------------------------------

TEST_PLAYERS_RAW = [
    {
        # Sabalenka-like: elite serve + aggression
        "name": "Big Serving Aggressor",
        "emoji": "🇧🇾",
        "raw_stats": {
            "ace_pct": 6.2, "df_pct": 4.1, "hld_pct": 78.0,
            "first_in": 64.0, "first_w": 72.0, "second_w": 53.0,
            "brk_pct": 36.0, "rpw": 42.0,
            "wnr_pt": 21.0, "ufe_pt": 18.0,
            "fh_wnr_pt": 13.0, "bh_wnr_pt": 8.0, "vs_ufe_pt": 17.0,
            "bp_saved": 62.0, "gp_conv": 66.0,
            "unret_pct": 28.0, "lt3_w": 56.0, "rip_w_serve": None,
            "rip_pct": 64.0, "rip_w": 52.0, "ret_wnr_pct": 5.0,
            "slice_ret_pct": 8.0, "fhbh_ratio": 1.4,
            "rally_len": 3.8, "s13_w": 56.0, "s10p_w": 54.0,
            "bh_slice_pct": 12.0, "fhp100": 11.0, "bhp100": 7.0,
            "snv_freq": 1.5, "net_freq": 8.0, "net_w": 62.0,
            "drop_freq": 0.8, "rally_agg": 65.0, "return_agg": 55.0,
        },
    },
    {
        # Swiatek-like: topspin + movement + return dominance
        "name": "Topspin Baseline Dominator",
        "emoji": "🇵🇱",
        "raw_stats": {
            "ace_pct": 2.8, "df_pct": 2.9, "hld_pct": 75.0,
            "first_in": 61.0, "first_w": 68.0, "second_w": 52.0,
            "brk_pct": 46.0, "rpw": 50.0,
            "wnr_pt": 18.0, "ufe_pt": 14.0,
            "fh_wnr_pt": 12.0, "bh_wnr_pt": 5.0, "vs_ufe_pt": 20.0,
            "bp_saved": 58.0, "gp_conv": 68.0,
            "unret_pct": 19.0, "lt3_w": 51.0, "rip_w_serve": None,
            "rip_pct": 77.0, "rip_w": 59.0, "ret_wnr_pct": 8.0,
            "slice_ret_pct": 5.0, "fhbh_ratio": 1.8,
            "rally_len": 5.1, "s13_w": 51.0, "s10p_w": 61.0,
            "bh_slice_pct": 5.0, "fhp100": 13.0, "bhp100": 9.0,
            "snv_freq": 0.5, "net_freq": 5.0, "net_w": 58.0,
            "drop_freq": 1.2, "rally_agg": 52.0, "return_agg": 68.0,
        },
    },
    {
        # Jabeur-like: variety + net play
        "name": "Variety and Touch Tactician",
        "emoji": "🇹🇳",
        "raw_stats": {
            "ace_pct": 2.1, "df_pct": 3.5, "hld_pct": 68.0,
            "first_in": 60.0, "first_w": 66.0, "second_w": 50.0,
            "brk_pct": 38.0, "rpw": 44.0,
            "wnr_pt": 16.0, "ufe_pt": 17.0,
            "fh_wnr_pt": 9.0, "bh_wnr_pt": 7.0, "vs_ufe_pt": 16.0,
            "bp_saved": 57.0, "gp_conv": 62.0,
            "unret_pct": 18.0, "lt3_w": 52.0, "rip_w_serve": None,
            "rip_pct": 71.0, "rip_w": 53.0, "ret_wnr_pct": 6.0,
            "slice_ret_pct": 18.0, "fhbh_ratio": 1.1,
            "rally_len": 4.2, "s13_w": 52.0, "s10p_w": 56.0,
            "bh_slice_pct": 22.0, "fhp100": 8.0, "bhp100": 6.0,
            "snv_freq": 4.5, "net_freq": 17.0, "net_w": 73.0,
            "drop_freq": 3.2, "rally_agg": 42.0, "return_agg": 50.0,
        },
    },
    {
        # Defensive grinder
        "name": "Defensive Counterpuncher",
        "emoji": "🎾",
        "raw_stats": {
            "ace_pct": 1.8, "df_pct": 2.2, "hld_pct": 65.0,
            "first_in": 59.0, "first_w": 63.0, "second_w": 47.0,
            "brk_pct": 42.0, "rpw": 48.0,
            "wnr_pt": 13.0, "ufe_pt": 14.0,
            "fh_wnr_pt": 7.0, "bh_wnr_pt": 5.0, "vs_ufe_pt": 22.0,
            "bp_saved": 60.0, "gp_conv": 64.0,
            "unret_pct": 17.0, "lt3_w": 48.0, "rip_w_serve": None,
            "rip_pct": 78.0, "rip_w": 58.0, "ret_wnr_pct": 5.0,
            "slice_ret_pct": 12.0, "fhbh_ratio": 1.0,
            "rally_len": 5.5, "s13_w": 48.0, "s10p_w": 62.0,
            "bh_slice_pct": 18.0, "fhp100": 7.0, "bhp100": 5.0,
            "snv_freq": 0.3, "net_freq": 3.0, "net_w": 55.0,
            "drop_freq": 0.5, "rally_agg": 28.0, "return_agg": 62.0,
        },
    },
    {
        # Rybakina-like: flat ball + elite serve
        "name": "Flat Ball Serve Machine",
        "emoji": "🇰🇿",
        "raw_stats": {
            "ace_pct": 6.8, "df_pct": 3.2, "hld_pct": 80.0,
            "first_in": 66.0, "first_w": 73.0, "second_w": 54.0,
            "brk_pct": 32.0, "rpw": 39.0,
            "wnr_pt": 22.0, "ufe_pt": 16.0,
            "fh_wnr_pt": 13.0, "bh_wnr_pt": 10.0, "vs_ufe_pt": 15.0,
            "bp_saved": 63.0, "gp_conv": 67.0,
            "unret_pct": 32.0, "lt3_w": 58.0, "rip_w_serve": None,
            "rip_pct": 62.0, "rip_w": 51.0, "ret_wnr_pct": 4.0,
            "slice_ret_pct": 7.0, "fhbh_ratio": 1.2,
            "rally_len": 3.5, "s13_w": 57.0, "s10p_w": 52.0,
            "bh_slice_pct": 4.0, "fhp100": 10.0, "bhp100": 8.0,
            "snv_freq": 2.0, "net_freq": 9.0, "net_w": 66.0,
            "drop_freq": 0.4, "rally_agg": 70.0, "return_agg": 45.0,
        },
    },
    {
        # Edge case: empty raw_stats — all dimensions should default to 5
        "name": "No Charting Data Player",
        "emoji": "❓",
        "raw_stats": {},
    },
    {
        # Edge case: zero values — must not be treated as None (is not None check)
        "name": "Zero Value Edge Case",
        "emoji": "🔢",
        "raw_stats": {
            "ace_pct": 0.0, "df_pct": 0.0, "wnr_pt": 0.0, "ufe_pt": 0.0,
        },
    },
    {
        # Edge case: missing name — should be skipped
        "name": "",
        "emoji": "⚠️",
        "raw_stats": {"ace_pct": 5.0},
    },
]

# ---------------------------------------------------------------------------
# Legacy fixtures — pre-scored ratings (backward compat check)
# These simulate GCS files written before the raw_stats migration.
# ---------------------------------------------------------------------------

TEST_PLAYERS_LEGACY = [
    {
        "name": "Legacy Ratings Player",
        "emoji": "📼",
        "ratings": {
            "forehand": 8, "backhand": 7, "serve": 9,
            "netPlay": 4, "movement": 6, "spinHeavy": 5,
            "consistency": 5, "aggression": 9, "mentalGame": 7,
            "returnGame": 6, "variety": 4, "riskTaking": 8,
        },
    },
]


def print_player_result(player) -> None:
    print(f"\n{'─' * 62}")
    print(f"  {player.emoji}  {player.name}")
    print(f"{'─' * 62}")
    r = player.ratings
    print(
        f"  serve={r.serve}  aggression={r.aggression}  movement={r.movement}"
        f"  spinHeavy={r.spinHeavy}  consistency={r.consistency}  variety={r.variety}"
    )
    print(f"  Primary:   {player.primary_archetype}")
    print(f"  Secondary: {player.secondary_archetype}")
    print("  Top 5 archetype scores:")
    for s in player.archetype_scores[:5]:
        bar = "█" * int(s.score * 20)
        print(f"    {s.name:<34} {s.score:.4f}  {bar}")


def test_mapper_directly() -> list[str]:
    """
    Unit tests for compute_ratings_from_raw().
    Returns list of failure messages (empty = all passed).
    """
    failures = []

    # All 12 dimensions must be present and in 1-10 range
    sample = TEST_PLAYERS_RAW[0]["raw_stats"]
    result = compute_ratings_from_raw(sample)
    expected_keys = {
        "forehand", "backhand", "serve", "netPlay", "movement", "spinHeavy",
        "consistency", "aggression", "mentalGame", "returnGame", "variety", "riskTaking",
    }
    missing = expected_keys - set(result.keys())
    if missing:
        failures.append(f"compute_ratings_from_raw: missing dimensions {missing}")
    for k, v in result.items():
        if not isinstance(v, int):
            failures.append(f"compute_ratings_from_raw: {k}={v!r} is not int")
        if not (1 <= v <= 10):
            failures.append(f"compute_ratings_from_raw: {k}={v} out of 1-10 range")

    # Empty dict — all dimensions must default to 5
    empty_result = compute_ratings_from_raw({})
    for k, v in empty_result.items():
        if v != 5:
            failures.append(f"Empty raw_stats: {k}={v}, expected 5")

    # Zero values — must not be treated as None (zero-safe check)
    zero_result = compute_ratings_from_raw({"ace_pct": 0.0, "wnr_pt": 0.0, "ufe_pt": 0.0})
    # ace_pct=0.0 is below NORM lo (1.5) → should clamp to 1, not default to 5
    if zero_result["serve"] == 5:
        failures.append(
            f"Zero-safety: serve={zero_result['serve']} when ace_pct=0.0 — "
            f"zero was treated as None (should clamp to 1)"
        )

    return failures


def run_smoke_test() -> None:
    print("\n══════════════════════════════════════════════════════════")
    print("  WTA Archetype Classifier — Local Smoke Test")
    print(f"  Archetypes defined: {len(ARCHETYPE_DEFINITIONS)}")
    print("══════════════════════════════════════════════════════════")

    # ── Part 1: Direct mapper unit tests ─────────────────────────────────
    print("\n── Part 1: wta_mapper.compute_ratings_from_raw() ─────────────")
    mapper_failures = test_mapper_directly()
    if mapper_failures:
        for f in mapper_failures:
            print(f"  ✗ FAIL: {f}")
    else:
        print("  ✓ All mapper unit tests passed (range, completeness, zero-safety, empty)")

    # ── Part 2: Full pipeline with raw_stats ─────────────────────────────
    print("\n── Part 2: Full pipeline (raw_stats → classify_player) ───────")
    processed = []
    skipped = 0
    errors = []

    for p in TEST_PLAYERS_RAW:
        name = p.get("name")
        if not name:
            skipped += 1
            print(f"\n  ⚠️  Skipped record with missing name (expected)")
            continue
        try:
            computed = compute_ratings_from_raw(p.get("raw_stats", {}))
            ratings = WTARatings(**computed)
            result = classify_player(name=name, ratings=ratings, emoji=p.get("emoji", "🎾"))
            processed.append(result)
            print_player_result(result)
        except Exception as e:
            errors.append((name, str(e)))
            print(f"\n  ✗ ERROR processing '{name}': {e}")

    # ── Part 3: Legacy backward compatibility ────────────────────────────
    print("\n── Part 3: Legacy ratings backward compatibility ─────────────")
    legacy_processed = []
    for p in TEST_PLAYERS_LEGACY:
        try:
            ratings = WTARatings(**p.get("ratings", {}))
            result = classify_player(name=p["name"], ratings=ratings, emoji=p.get("emoji", "🎾"))
            legacy_processed.append(result)
            print(f"  ✓ Legacy player '{p['name']}' → {result.primary_archetype}")
        except Exception as e:
            errors.append((p["name"], str(e)))
            print(f"  ✗ ERROR on legacy player '{p['name']}': {e}")

    # ── Snapshot validation ───────────────────────────────────────────────
    all_processed = processed + legacy_processed
    snapshot = WTAArchetypeSnapshot(
        updated=datetime.now(timezone.utc),
        player_count=len(all_processed),
        players=all_processed,
    )
    dumped = snapshot.model_dump(mode="json")

    print(f"\n\n══════════════════════════════════════════════════════════")
    print(f"  Mapper unit tests:      {'PASSED' if not mapper_failures else 'FAILED'}")
    print(f"  Snapshot validation:    PASSED")
    print(f"  Raw stats players:      {len(processed)}/{len(TEST_PLAYERS_RAW)}")
    print(f"  Legacy players:         {len(legacy_processed)}/{len(TEST_PLAYERS_LEGACY)}")
    print(f"  Skipped (no name):      {skipped}  ✓ expected")
    print(f"  Errors:                 {len(errors)}")

    assert isinstance(dumped["updated"], str), "datetime not serialized to string"
    print(f"  Datetime serialization: OK  ({dumped['updated']})")

    for p in dumped["players"]:
        assert len(p["archetype_scores"]) == len(ARCHETYPE_DEFINITIONS), \
            f"{p['name']}: wrong score count"
    print(f"  Archetype score count:  OK  ({len(ARCHETYPE_DEFINITIONS)} per player)")

    print(f"\n  Archetype assignments:")
    for p in all_processed:
        source = "raw" if p.name != "Legacy Ratings Player" else "legacy"
        print(f"    [{source}] {p.name:<36} → {p.primary_archetype}")

    print("══════════════════════════════════════════════════════════\n")

    if errors or mapper_failures:
        sys.exit(1)


if __name__ == "__main__":
    run_smoke_test()
