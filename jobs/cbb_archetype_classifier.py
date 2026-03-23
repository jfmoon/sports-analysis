import os
import sys
import logging
from datetime import datetime, timezone
from lib.storage import AnalysisStorage


def setup_logger(name):
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


ARCHETYPES = [
    {"id": "two_way_machine", "name": "Two-Way Machine", "description": "Elite on both ends. No glaring weakness.", "weights": {"off_efficiency": 2.5, "def_efficiency": 2.5, "ball_security": 0.5, "rim_protection": 0.5}, "thresholds": {"off_efficiency": 7.0, "def_efficiency": 7.0}, "anti_thresholds": {}},
    {"id": "offensive_juggernaut", "name": "Offensive Juggernaut", "description": "Historically elite offense scoring from everywhere. Defense is secondary.", "weights": {"off_efficiency": 4.0, "three_pt_prowess": 1.0, "free_throw_gen": 1.0, "ball_security": 0.5}, "thresholds": {"off_efficiency": 8.0}, "anti_thresholds": {"def_efficiency": 7.0}},
    {"id": "defensive_fortress", "name": "Defensive Fortress", "description": "Suffocating defense with rim protection and arc lockdown.", "weights": {"def_efficiency": 3.0, "rim_protection": 1.5, "opp_3pt_allowed": 1.0, "pressure_defense": 1.0, "off_efficiency": 0.5}, "thresholds": {"def_efficiency": 6.5}, "anti_thresholds": {"off_efficiency": 7.0}},
    {"id": "gunslinger", "name": "Gunslinger", "description": "High 3PT volume AND gets to the line relentlessly.", "weights": {"three_pt_prowess": 2.5, "free_throw_gen": 2.5, "off_efficiency": 1.0, "ball_security": 0.5}, "thresholds": {"three_pt_prowess": 6.0, "free_throw_gen": 7.0}, "anti_thresholds": {}},
    {"id": "sniper_system", "name": "Sniper System", "description": "Lives and dies by the 3. High volume and efficiency from deep.", "weights": {"three_pt_prowess": 4.0, "off_efficiency": 1.0, "ball_security": 0.5}, "thresholds": {"three_pt_prowess": 6.5}, "anti_thresholds": {"free_throw_gen": 7.0}},
    {"id": "offensive_engine", "name": "Offensive Engine", "description": "Efficient offense without a dominant identity. Defense is a question mark.", "weights": {"off_efficiency": 3.0, "three_pt_prowess": 1.0, "ball_security": 1.0, "free_throw_gen": 0.5}, "thresholds": {"off_efficiency": 7.0}, "anti_thresholds": {"def_efficiency": 7.0}},
    {"id": "foul_line_bully", "name": "Foul-Line Bully", "description": "Gets to the line relentlessly. Physical, paint-dominant.", "weights": {"free_throw_gen": 3.5, "off_rebounding": 1.5, "off_efficiency": 1.0, "rim_protection": 0.5}, "thresholds": {"free_throw_gen": 7.5}, "anti_thresholds": {"def_efficiency": 7.0, "three_pt_prowess": 6.5}},
    {"id": "system_operator", "name": "System Operator", "description": "Wins through execution. Catchall for disciplined, hard-to-categorize teams.", "weights": {"ball_security": 2.0, "off_efficiency": 1.5, "def_efficiency": 1.0, "three_pt_prowess": 0.5}, "thresholds": {}, "anti_thresholds": {}},
]


def main():
    logger = setup_logger("cbb_classifier")
    trigger_path = os.environ.get("TRIGGER_GCS_PATH", "")

    if trigger_path != "cbb/kenpom.json":
        logger.error(f"Classifier received invalid trigger: {trigger_path}")
        sys.exit(1)

    try:
        storage = AnalysisStorage.from_env()
        kenpom = storage.read_trigger_snapshot()
        teams = kenpom.teams

        attr_map = {
            "three_pt_prowess": ("three_p_pct", "higher"),
            "free_throw_gen": ("ftr", "higher"),
            "off_efficiency": ("adj_o", "higher"),
            "ball_security": ("to_pct", "lower"),
            "off_rebounding": ("orb_pct", "higher"),
            "def_efficiency": ("adj_d", "lower"),
            "opp_3pt_allowed": ("opp_3p_pct", "lower"),
            "rim_protection": ("block_pct", "higher"),
            "pressure_defense": ("steal_pct", "higher"),
        }

        # Safe attribute bounding
        bounds = {
            attr: (min(vals), max(vals))
            for attr, (f, _) in attr_map.items()
            if (vals := [getattr(t, f, None) for t in teams if getattr(t, f, None) is not None])
        }

        # Validate all required attributes are present
        for attr, (field, _) in attr_map.items():
            if attr not in bounds:
                logger.error(f"Field '{field}' is missing across entire dataset")
                sys.exit(1)

        processed = []
        for team in teams:
            normalized = {}
            for attr, (f, direction) in attr_map.items():
                raw = getattr(team, f, None)
                if raw is None:
                    normalized[attr] = 5.5
                    continue
                f_min, f_max = bounds[attr]
                if f_max == f_min:
                    normalized[attr] = 5.5
                elif direction == "higher":
                    normalized[attr] = round(max(1.0, min(10.0, 1 + 9 * (raw - f_min) / (f_max - f_min))), 2)
                else:
                    normalized[attr] = round(max(1.0, min(10.0, 1 + 9 * (1 - (raw - f_min) / (f_max - f_min)))), 2)

            weighted_scores = {}
            eligible_ids = []
            for arch in ARCHETYPES:
                meets_thresholds = all(normalized[k] >= v for k, v in arch["thresholds"].items())
                meets_antis = all(normalized[k] < v for k, v in arch["anti_thresholds"].items())
                score = sum(normalized.get(k, 1.0) * w for k, w in arch["weights"].items())
                weighted_scores[arch["id"]] = round(score, 2)
                if arch["id"] != "system_operator" and meets_thresholds and meets_antis:
                    eligible_ids.append(arch["id"])

            final_id = max(eligible_ids, key=lambda x: weighted_scores[x]) if eligible_ids else "system_operator"
            meta = next(a for a in ARCHETYPES if a["id"] == final_id)

            processed.append({
                "name": team.name,
                "kenpom_rank": team.kenpom_rank,
                "archetype": final_id,
                "archetype_name": meta["name"],
                "is_veteran": (team.experience or 0) >= 2.0,
                "is_length": normalized["rim_protection"] >= 7.0,
                "attributes": normalized,
                "weighted_scores": weighted_scores,
            })

        output = {
            "_provenance": {
                "job_run_at": datetime.now(timezone.utc).isoformat(),
                "message_id": os.environ.get("TRIGGER_MESSAGE_ID"),
                "trigger_path": trigger_path,
                "source_updated": getattr(kenpom, "updated", None),
            },
            "data": {
                "updated": kenpom.updated,
                "team_count": len(processed),
                "teams": sorted(processed, key=lambda x: (x["kenpom_rank"] is None, x["kenpom_rank"])),
            },
        }
        storage.write_processed("cbb/archetypes.json", output)
        logger.info(f"Classified {len(processed)} teams")

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    os.environ.setdefault("TRIGGER_GCS_BUCKET", "sports-data-scraper-491116")
    os.environ.setdefault("TRIGGER_GCS_PATH", "cbb/kenpom.json")
    os.environ.setdefault("TRIGGER_GCS_GEN", "0")
    os.environ.setdefault("TRIGGER_MESSAGE_ID", "local-test-classifier")
    main()
