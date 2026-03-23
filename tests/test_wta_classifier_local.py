"""
Local smoke test for the WTA archetype classifier.

Run from the repo root:
    python tests/test_wta_classifier_local.py

Does NOT require GCS access. Exercises the mapper and classifier
directly with synthetic player data covering edge cases.
"""

import sys
import json
from datetime import datetime, timezone

# Adjust path if running from repo root without install
sys.path.insert(0, ".")

from lib.logic.wta_mapper import map_stats_to_ratings
from lib.logic.wta_classifier import classify_player, ARCHETYPE_DEFINITIONS
from lib.schemas.wta import WTAArchetypeSnapshot


# ------------------------------------------------------------------ #
# Synthetic test players                                               #
# ------------------------------------------------------------------ #
TEST_PLAYERS = [
    {
        "name": "Serena-like Baseline Bomber",
        "emoji": "💪",
        "stats": {
            "ace_pct": 8.0,          # strong serve
            "ret_pts_won_pct": 52.0,  # elite return
            "df_pct": 3.5,           # decent consistency
            "fsv_pts_won_pct": 72.0, # aggressive first serve
        },
    },
    {
        "name": "Defensive Grinder",
        "emoji": "🧱",
        "stats": {
            "ace_pct": 2.0,           # low serve power
            "ret_pts_won_pct": 50.0,  # good return
            "df_pct": 1.8,            # very consistent
            "fsv_pts_won_pct": 58.0,  # conservative
        },
    },
    {
        "name": "Big Server Specialist",
        "emoji": "🚀",
        "stats": {
            "ace_pct": 13.0,          # near elite ace rate
            "ret_pts_won_pct": 38.0,  # weaker return
            "df_pct": 5.0,            # average consistency
            "fsv_pts_won_pct": 74.0,  # dominant first serve
        },
    },
    {
        "name": "Average Tour Player",
        "emoji": "🎾",
        "stats": {
            "ace_pct": 4.0,
            "ret_pts_won_pct": 44.0,
            "df_pct": 4.5,
            "fsv_pts_won_pct": 63.0,
        },
    },
    {
        # Edge case: all stats missing — should fall back to all 5.0 placeholders
        "name": "Missing Stats Player",
        "emoji": "❓",
        "stats": {},
    },
    {
        # Edge case: out-of-range stats — clamping should contain them to [1, 10]
        "name": "Out-of-Range Stats Player",
        "emoji": "⚠️",
        "stats": {
            "ace_pct": 99.0,      # way above 15.0 ceiling
            "df_pct": -5.0,       # below 0 floor
            "fsv_pts_won_pct": 0, # below 55.0 floor
        },
    },
]


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #
def print_player_result(player) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {player.emoji}  {player.name}")
    print(f"{'─' * 60}")

    r = player.ratings
    print(
        f"  Ratings (live):  serve={r.serve}  return={r.returnGame}  "
        f"consistency={r.consistency}  aggression={r.aggression}"
    )
    print(f"  Primary:   {player.primary_archetype}")
    print(f"  Secondary: {player.secondary_archetype}")

    print("  Top 5 archetype scores:")
    for s in player.archetype_scores[:5]:
        bar = "█" * int(s.score * 20)
        print(f"    {s.name:<30} {s.score:.4f}  {bar}")


# ------------------------------------------------------------------ #
# Main test                                                            #
# ------------------------------------------------------------------ #
def run_smoke_test() -> None:
    print("\n══════════════════════════════════════════════════════════")
    print("  WTA Archetype Classifier — Local Smoke Test")
    print(f"  Archetypes defined: {len(ARCHETYPE_DEFINITIONS)}")
    print("══════════════════════════════════════════════════════════")

    processed = []
    errors = []

    for p in TEST_PLAYERS:
        try:
            ratings = map_stats_to_ratings(p.get("stats", {}))
            result = classify_player(
                name=p["name"],
                ratings=ratings,
                emoji=p.get("emoji", "🎾"),
            )
            processed.append(result)
            print_player_result(result)
        except Exception as e:
            errors.append((p["name"], str(e)))
            print(f"\n  ✗ ERROR processing '{p['name']}': {e}")

    # Build and validate snapshot (exercises full Pydantic chain)
    snapshot = WTAArchetypeSnapshot(
        updated=datetime.now(timezone.utc),
        player_count=len(processed),
        players=processed,
    )

    dumped = snapshot.model_dump(mode="json")

    print(f"\n\n══════════════════════════════════════════════════════════")
    print(f"  Snapshot validation: PASSED")
    print(f"  Players processed:   {len(processed)}/{len(TEST_PLAYERS)}")
    print(f"  Errors:              {len(errors)}")
    if errors:
        for name, err in errors:
            print(f"    - {name}: {err}")

    # Confirm datetime serialized to string (not datetime object)
    assert isinstance(dumped["updated"], str), "updated should be ISO string after model_dump(mode='json')"
    print(f"  Datetime serialization: OK  ({dumped['updated']})")

    # Confirm all players have 13 archetype scores
    for p in dumped["players"]:
        assert len(p["archetype_scores"]) == len(ARCHETYPE_DEFINITIONS), \
            f"{p['name']}: expected {len(ARCHETYPE_DEFINITIONS)} scores, got {len(p['archetype_scores'])}"
    print(f"  Archetype score count:  OK  ({len(ARCHETYPE_DEFINITIONS)} per player)")

    print("══════════════════════════════════════════════════════════\n")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    run_smoke_test()
