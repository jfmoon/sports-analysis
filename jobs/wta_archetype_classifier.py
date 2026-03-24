import os
import logging
import sys
from datetime import datetime, timezone

from lib.storage import AnalysisStorage
from lib.logic.wta_classifier import classify_player
from lib.logic.wta_mapper import compute_ratings_from_raw
from lib.schemas.wta import WTARatings, WTAArchetypeSnapshot


def main() -> None:
    # ------------------------------------------------------------------ #
    # Local test env overrides                                             #
    # ------------------------------------------------------------------ #
    if not os.getenv("TRIGGER_GCS_BUCKET"):
        os.environ.setdefault("TRIGGER_GCS_BUCKET", "sports-data-scraper-491116")
        os.environ.setdefault("TRIGGER_GCS_PATH", "tennis/players.json")
        os.environ.setdefault("TRIGGER_GCS_GEN", "0")
        os.environ.setdefault("TRIGGER_MESSAGE_ID", "manual-local-smoke-test")

    msg_id = os.getenv("TRIGGER_MESSAGE_ID", "local-test-run")
    logging.basicConfig(
        level=logging.INFO,
        format=f"[{msg_id}] %(levelname)s: %(message)s",
        force=True,
    )
    logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------ #
    # 1. Read input snapshot                                               #
    # ------------------------------------------------------------------ #
    storage = AnalysisStorage.from_env()

    try:
        trigger_model = storage.read_trigger_snapshot()
        input_data = trigger_model.model_dump()
    except Exception as e:
        logger.error(f"Failed to read trigger snapshot: {e}")
        sys.exit(1)

    raw_players = input_data.get("players")
    if raw_players is None:
        logger.error(
            f"STRICT_SCHEMA_VIOLATION: 'players' key missing. "
            f"Available keys: {list(input_data.keys())}"
        )
        sys.exit(1)

    if not raw_players:
        logger.error(
            "'players' list is empty — possible scraper failure. "
            "Exiting to avoid writing a zero-player snapshot."
        )
        sys.exit(1)

    logger.info(f"Loaded {len(raw_players)} players from scraper output.")

    # ------------------------------------------------------------------ #
    # 2. Process each player                                               #
    # ------------------------------------------------------------------ #
    processed_players = []
    legacy_count = 0
    raw_stats_count = 0

    for p in raw_players:
        try:
            name = p.get("name") or p.get("player_name")
            if not name:
                logger.warning(
                    f"Skipping record missing 'name'/'player_name'. "
                    f"Keys present: {list(p.keys())}"
                )
                continue

            # ── Rating resolution: raw_stats (new) → ratings (legacy) ──
            # New scraper output contains raw_stats (float percentages).
            # Legacy GCS files contain pre-scored ratings (1-10 ints).
            # Use raw_stats when present; fall back to ratings for any
            # files written before the 2026-03-23 schema migration.
            raw_stats = p.get("raw_stats")
            legacy_ratings = p.get("ratings")

            if raw_stats:
                # New path: compute ratings from raw floats
                computed = compute_ratings_from_raw(raw_stats)
                ratings = WTARatings(**computed)
                raw_stats_count += 1
            elif legacy_ratings:
                # Legacy path: ratings were pre-computed by the scraper
                ratings = WTARatings(**legacy_ratings)
                legacy_count += 1
                logger.debug(f"[legacy] Using pre-scored ratings for '{name}'")
            else:
                # No data at all — default all dimensions to 5
                ratings = WTARatings()
                logger.warning(f"No ratings or raw_stats for '{name}' — defaulting all to 5")

            player_result = classify_player(
                name=name,
                ratings=ratings,
                emoji=p.get("emoji", "🎾"),
            )
            processed_players.append(player_result)

        except Exception as e:
            logger.warning(f"Error processing player '{p.get('name', '?')}': {e}")

    if legacy_count > 0:
        logger.warning(
            f"{legacy_count} player(s) used legacy pre-scored ratings. "
            f"These are from GCS files written before the raw_stats migration. "
            f"They will resolve naturally as the scraper re-runs."
        )

    logger.info(
        f"Classification complete: {len(processed_players)}/{len(raw_players)} players processed "
        f"({raw_stats_count} via raw_stats, {legacy_count} via legacy ratings)."
    )

    # ------------------------------------------------------------------ #
    # 3. Build and write snapshot                                          #
    # ------------------------------------------------------------------ #
    snapshot = WTAArchetypeSnapshot(
        updated=datetime.now(timezone.utc),
        player_count=len(processed_players),
        players=processed_players,
    )

    storage.write_processed("wta/archetypes.json", snapshot.model_dump(mode="json"))
    logger.info("Successfully exported WTA archetypes to processed bucket.")


if __name__ == "__main__":
    main()
