import os
import sys
import logging
from datetime import datetime, timezone
from lib.storage import AnalysisStorage
from lib.team_names import TeamNameResolver


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


def get_norm_val(val, b_min, b_max, inverted=False):
    """Standard 0-1 normalization. Returns 0.5 for constant fields, None if data missing."""
    if val is None or b_min is None or b_max is None:
        return None
    if b_max == b_min:
        return 0.5
    res = (val - b_min) / (b_max - b_min)
    res = max(0.0, min(1.0, res))
    return 1.0 - res if inverted else res


def compute_weighted_score(weights, norms_dict):
    """Availability-based weight renormalization — missing fields are dropped and weights rescaled."""
    active_weights = {k: v for k, v in weights.items() if k in norms_dict and norms_dict[k] is not None}
    total_w = sum(active_weights.values())
    if total_w == 0:
        return None
    return sum((w / total_w) * norms_dict[k] for k, w in active_weights.items())


def main():
    logger = setup_logger("cbb_projector")
    storage = AnalysisStorage.from_env()
    trigger_path = os.environ.get("TRIGGER_GCS_PATH", "")

    if trigger_path not in ["cbb/kenpom.json", "cbb/odds.json"]:
        logger.error(f"Invalid trigger: {trigger_path}")
        sys.exit(1)

    try:
        if trigger_path == "cbb/kenpom.json":
            kenpom = storage.read_trigger_snapshot()
            odds_snap = storage.read_snapshot("cbb/odds.json")
        else:
            odds_snap = storage.read_trigger_snapshot()
            kenpom = storage.read_snapshot("cbb/kenpom.json")

        teams = kenpom.teams

        # 1. Bounds Calculation
        fields = [
            "adj_o", "adj_d", "three_p_pct", "three_par", "ftr", "to_pct",
            "steal_pct", "barthag", "rr", "kenpom_rank", "experience", "orb_pct", "opp_3p_pct",
        ]
        bounds = {
            f: (min(vals), max(vals))
            for f in fields
            if (vals := [getattr(t, f, None) for t in teams if getattr(t, f, None) is not None])
        }

        # Safe NetRTG list & population-based EM Ranks
        rtg_list = [
            {"name": t.name, "val": (t.adj_o - t.adj_d) if (t.adj_o is not None and t.adj_d is not None) else None}
            for t in teams
        ]
        rtg_sorted = sorted([r for r in rtg_list if r["val"] is not None], key=lambda x: x["val"], reverse=True)
        em_ranks = {item["name"]: i + 1 for i, item in enumerate(rtg_sorted)}
        rtg_min, rtg_max = (min(v), max(v)) if (v := [r["val"] for r in rtg_sorted]) else (0.0, 0.0)

        # 2. Team Strength Calculation
        team_metrics = {}
        for t in teams:
            norms = {f: get_norm_val(getattr(t, f, None), *bounds.get(f, (None, None))) for f in fields}

            # Derived metrics (strict population normalization)
            norms["net_rtg"] = get_norm_val(
                t.adj_o - t.adj_d if (t.adj_o is not None and t.adj_d is not None) else None,
                rtg_min, rtg_max,
            )
            norms["em_rank_inv"] = (
                get_norm_val(em_ranks.get(t.name), 1, len(em_ranks), inverted=True)
                if t.name in em_ranks else None
            )
            norms["kenpom_rank_inv"] = get_norm_val(
                t.kenpom_rank, *bounds.get("kenpom_rank", (None, None)), inverted=True
            )

            # Inverse stat keys used by weight dicts
            if norms.get("adj_d") is not None:
                norms["adj_d_inv"] = 1.0 - norms["adj_d"]
            if norms.get("to_pct") is not None:
                norms["to_pct_inv"] = 1.0 - norms["to_pct"]
            if norms.get("opp_3p_pct") is not None:
                norms["opp_3p_pct_inv"] = 1.0 - norms["opp_3p_pct"]

            # JBScore
            jb_weights = {
                "adj_o": 0.15, "adj_d_inv": 0.10, "net_rtg": 0.10, "three_p_pct": 0.10,
                "three_par": 0.07, "ftr": 0.07, "to_pct_inv": 0.07, "steal_pct": 0.05,
                "barthag": 0.08, "rr": 0.07, "em_rank_inv": 0.08, "kenpom_rank_inv": 0.06,
            }

            is_fallback = False
            if t.adj_o is None or t.adj_d is None:
                is_fallback = True
                jb_score = (t.barthag or 0) * 45
            else:
                jb_raw = compute_weighted_score(jb_weights, norms)
                if jb_raw is not None:
                    jb_score = jb_raw * 100
                else:
                    is_fallback = True
                    jb_score = (t.barthag or 0) * 45

            if is_fallback:
                logger.warning(f"Fallback JBScore used for {t.name}")

            # Stability Score
            stab_weights = {"adj_d_inv": 0.40, "to_pct_inv": 0.25, "ftr": 0.20, "experience": 0.15}
            stab_raw = compute_weighted_score(stab_weights, norms)
            stability = round(stab_raw * 100, 1) if stab_raw is not None else 50.0

            team_metrics[t.name] = {
                "name": t.name,
                "kenpom_rank": t.kenpom_rank,
                "jb_score": round(jb_score, 1),
                "jb_score_is_fallback": is_fallback,
                "stability_score": stability,
                "net_rtg": round(t.adj_o - t.adj_d, 1) if (t.adj_o is not None and t.adj_d is not None) else None,
                "_norms": norms,
            }

        # 3. Game Edge Calculation
        resolver = TeamNameResolver(list(team_metrics.keys()), logger)
        games = []
        odds_list = getattr(odds_snap, "odds", [])
        if not isinstance(odds_list, list):
            odds_list = []

        for o in odds_list:
            h_name = resolver.resolve(o.home_team)
            a_name = resolver.resolve(o.away_team)

            if not h_name or not a_name:
                logger.warning(f"Resolution failed for {o.home_team} vs {o.away_team}")
                continue

            h, a = team_metrics[h_name], team_metrics[a_name]
            jb_delta = h["jb_score"] - a["jb_score"]
            model_spread = -(jb_delta / 2.0)
            edge = o.spread - model_spread

            side = "home" if edge > 0 else "away" if edge < 0 else "none"
            if side == "home":
                label = f"{h['name']} {model_spread:+.1f} (market {o.spread:.1f})"
            elif side == "away":
                label = f"{a['name']} {-model_spread:+.1f} (market {o.spread:.1f})"
            else:
                label = f"No edge (market {o.spread:.1f}, model {model_spread:+.1f})"

            # Upset score — signed, range -100 to +100. Negative means dog has no advantage.
            upset_score = None
            if abs(jb_delta) > 5:
                dog, fav = (a, h) if jb_delta > 0 else (h, a)
                u_weights = {
                    "to_pct_inv": 0.30, "three_p_pct": 0.25,
                    "orb_pct": 0.20, "ftr": 0.15, "opp_3p_pct_inv": 0.10,
                }
                dn, fn = dog["_norms"], fav["_norms"]
                u_edges = {k: dn[k] - fn[k] for k in u_weights if k in dn and k in fn}
                active_u = {k: w for k, w in u_weights.items() if k in u_edges}
                total_u = sum(active_u.values())
                if total_u > 0:
                    upset_score = round(
                        sum((w / total_u) * u_edges[k] for k, w in active_u.items()) * 100, 1
                    )

            games.append({
                "home_team": h["name"],
                "away_team": a["name"],
                "market_spread": o.spread,
                "model_spread": round(model_spread, 1),
                "spread_edge": round(edge, 2),
                "jb_home": h["jb_score"],
                "jb_away": a["jb_score"],
                "jb_delta": round(jb_delta, 1),
                "upset_score": upset_score,
                "value_side": side,
                "value_label": label,
                "home_ml": o.home_ml,
                "away_ml": o.away_ml,
            })

        # 4. Output Construction
        sorted_games = sorted(games, key=lambda x: abs(x["spread_edge"]), reverse=True)
        output = {
            "_provenance": {
                "job_run_at": datetime.now(timezone.utc).isoformat(),
                "message_id": os.environ.get("TRIGGER_MESSAGE_ID"),
                "trigger_path": trigger_path,
                "trigger_bucket": os.environ.get("TRIGGER_GCS_BUCKET"),
                "trigger_gen": os.environ.get("TRIGGER_GCS_GEN"),
                "source_updated": getattr(kenpom, "updated", None),
                "odds_updated": getattr(odds_snap, "updated", None),
                "odds_games_found": len(sorted_games),
            },
            "data": {
                "updated": kenpom.updated,
                "team_scores": sorted(
                    [{k: v for k, v in tm.items() if k != "_norms"} for tm in team_metrics.values()],
                    key=lambda x: (x["kenpom_rank"] is None, x["kenpom_rank"]),
                ),
                "games": sorted_games,
                "ranked_edges": [
                    {
                        "rank": i + 1,
                        "game": f"{g['home_team']} vs {g['away_team']}",
                        "value_side": g["value_side"],
                        "spread_edge": g["spread_edge"],
                        "upset_score": g["upset_score"],
                    }
                    for i, g in enumerate(sorted_games)
                ],
            },
        }
        storage.write_processed("cbb/projections.json", output)
        logger.info(f"Projected {len(sorted_games)} games")

    except Exception as e:
        logger.exception(f"Fatal projector error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    os.environ.setdefault("TRIGGER_GCS_BUCKET", "sports-data-scraper-491116")
    os.environ.setdefault("TRIGGER_GCS_PATH", "cbb/kenpom.json")
    os.environ.setdefault("TRIGGER_GCS_GEN", "0")
    os.environ.setdefault("TRIGGER_MESSAGE_ID", "local-test-projector")
    main()
