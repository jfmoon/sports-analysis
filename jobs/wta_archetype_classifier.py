import os
import logging
import sys
from datetime import datetime, timezone

from lib.storage import AnalysisStorage
from lib.logic.wta_mapper import map_stats_to_ratings
from lib.logic.wta_classifier import classify_player
from lib.schemas.wta import WTAArchetypeSnapshot


def main() -> None:
    # ------------------------------------------------------------------ #
    # Local test env overrides — must come before logging init so that    #
    # TRIGGER_MESSAGE_ID is set before msg_id is read.                    #
    # ------------------------------------------------------------------ #
    if not os.getenv("TRIGGER_GCS_BUCKET"):
        os.environ.setdefault("TRIGGER_GCS_BUCKET", "sports-data-scraper-491116")
        os.environ.setdefault("TRIGGER_GCS_PATH", "wta/raw_stats.json")
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

    # Strict schema check — we expect a 'players' key from the WTA scraper.
    # If the key is missing, log available keys to aid debugging.
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

    for p in raw_players:
        try:
            name = p.get("name") or p.get("player_name")
            if not name:
                logger.warning(
                    f"Skipping record missing 'name'/'player_name'. "
                    f"Keys present: {list(p.keys())}"
                )
                continue

            ratings = map_stats_to_ratings(p.get("stats", {}))
            player_result = classify_player(
                name=name,
                ratings=ratings,
                emoji=p.get("emoji", "🎾"),
            )
            processed_players.append(player_result)

        except Exception as e:
            logger.warning(f"Error processing player '{p.get('name', '?')}': {e}")

    logger.info(
        f"Classification complete: {len(processed_players)}/{len(raw_players)} players processed."
    )

    # ------------------------------------------------------------------ #
    # 3. Build and write snapshot                                          #
    # ------------------------------------------------------------------ #
    snapshot = WTAArchetypeSnapshot(
        updated=datetime.now(timezone.utc),
        player_count=len(processed_players),
        players=processed_players,
    )

    # mode="json" ensures datetime -> ISO string throughout the object graph.
    storage.write_processed("wta/archetypes.json", snapshot.model_dump(mode="json"))
    logger.info("Successfully exported WTA archetypes to processed bucket.")


if __name__ == "__main__":
    main()
