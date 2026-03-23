"""
Local smoke test for the WTA archetype classifier.

Run from the repo root:
    python tests/test_wta_classifier_local.py

Does NOT require GCS access. Exercises the classifier directly with
synthetic player records that match actual scraper output shape —
i.e. pre-computed 1-10 integer ratings, not raw stat percentages.
"""

import sys
from datetime import datetime, timezone

sys.path.insert(0, ".")

from lib.logic.wta_classifier import classify_player, ARCHETYPE_DEFINITIONS
from lib.schemas.wta import WTARatings, WTAArchetypeSnapshot


TEST_PLAYERS = [
    {
        # Sabalenka-like: elite serve + aggression
        "name": "Big Serving Aggressor",
        "emoji": "🇧🇾",
        "ratings": {
            "forehand": 8, "backhand": 7, "serve": 9,
            "netPlay": 4, "movement": 6, "spinHeavy": 5,
            "consistency": 5, "aggression": 9, "mentalGame": 7,
            "returnGame": 6, "variety": 4, "riskTaking": 8,
        },
    },
    {
        # Swiatek-like: topspin + movement + return
        "name": "Topspin Baseline Dominator",
        "emoji": "🇵🇱",
        "ratings": {
            "forehand": 9, "backhand": 7, "serve": 6,
            "netPlay": 4, "movement": 9, "spinHeavy": 9,
            "consistency": 8, "aggression": 7, "mentalGame": 9,
            "returnGame": 9, "variety": 5, "riskTaking": 5,
        },
    },
    {
        # Jabeur-like: variety + net play + touch
        "name": "Variety and Touch Tactician",
        "emoji": "🇹🇳",
        "ratings": {
            "forehand": 6, "backhand": 7, "serve": 6,
            "netPlay": 8, "movement": 7, "spinHeavy": 4,
            "consistency": 6, "aggression": 5, "mentalGame": 7,
            "returnGame": 7, "variety": 9, "riskTaking": 6,
        },
    },
    {
        # Defensive grinder: high consistency + movement, low aggression
        "name": "Defensive Counterpuncher",
        "emoji": "🎾",
        "ratings": {
            "forehand": 5, "backhand": 6, "serve": 4,
            "netPlay": 3, "movement": 9, "spinHeavy": 6,
            "consistency": 9, "aggression": 2, "mentalGame": 8,
            "returnGame": 8, "variety": 4, "riskTaking": 2,
        },
    },
    {
        # Rybakina-like: elite serve, flat ball, low spin
        "name": "Flat Ball Serve Machine",
        "emoji": "🇰🇿",
        "ratings": {
            "forehand": 8, "backhand": 8, "serve": 10,
            "netPlay": 5, "movement": 5, "spinHeavy": 2,
            "consistency": 6, "aggression": 8, "mentalGame": 7,
            "returnGame": 5, "variety": 4, "riskTaking": 6,
        },
    },
    {
        # Edge case: empty ratings dict — all fields default to 5
        "name": "No Charting Data Player",
        "emoji": "❓",
        "ratings": {},
    },
    {
        # Edge case: missing name — should be skipped
        "name": "",
        "emoji": "⚠️",
        "ratings": {"forehand": 7, "aggression": 8},
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


def run_smoke_test() -> None:
    print("\n══════════════════════════════════════════════════════════")
    print("  WTA Archetype Classifier — Local Smoke Test")
    print(f"  Archetypes defined: {len(ARCHETYPE_DEFINITIONS)}")
    print("══════════════════════════════════════════════════════════")

    processed = []
    skipped = 0
    errors = []

    for p in TEST_PLAYERS:
        name = p.get("name") or p.get("player_name")
        if not name:
            skipped += 1
            print(f"\n  ⚠️  Skipped record with missing name (expected)")
            continue
        try:
            ratings = WTARatings(**p.get("ratings", {}))
            result = classify_player(name=name, ratings=ratings, emoji=p.get("emoji", "🎾"))
            processed.append(result)
            print_player_result(result)
        except Exception as e:
            errors.append((name, str(e)))
            print(f"\n  ✗ ERROR processing '{name}': {e}")

    snapshot = WTAArchetypeSnapshot(
        updated=datetime.now(timezone.utc),
        player_count=len(processed),
        players=processed,
    )
    dumped = snapshot.model_dump(mode="json")

    print(f"\n\n══════════════════════════════════════════════════════════")
    print(f"  Snapshot validation:    PASSED")
    print(f"  Players processed:      {len(processed)}/{len(TEST_PLAYERS)}")
    print(f"  Skipped (no name):      {skipped}  ✓ expected")
    print(f"  Errors:                 {len(errors)}")

    assert isinstance(dumped["updated"], str), "datetime not serialized to string"
    print(f"  Datetime serialization: OK  ({dumped['updated']})")

    for p in dumped["players"]:
        assert len(p["archetype_scores"]) == len(ARCHETYPE_DEFINITIONS), \
            f"{p['name']}: wrong score count"
    print(f"  Archetype score count:  OK  ({len(ARCHETYPE_DEFINITIONS)} per player)")

    print(f"\n  Archetype assignments:")
    for p in processed:
        print(f"    {p.name:<36} → {p.primary_archetype}")

    print("══════════════════════════════════════════════════════════\n")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    run_smoke_test()
