"""
lib/logic/wta_mapper.py

Translates raw WTA player stats (as scraped by tennisabstract_scraper.py)
into 1-10 attribute ratings for use by the WTA archetype classifier.

Raw stat keys correspond exactly to the `raw_stats` dict written by
tennisabstract_scraper.py. Do not rename keys without updating the scraper.

Replaces the previous partial implementation (map_stats_to_ratings) which
had only 4 of 12 dimensions mapped and 8 hardcoded placeholders.
"""

from typing import Optional, Mapping
import logging

logger = logging.getLogger(__name__)

# ── Normalization ranges derived from WTA career stat distributions ───────────
# Ranges represent realistic min/max across WTA top 100.
# Sources: Tennis Abstract career aggregates across known players.
# invert=True means lower raw value = higher rating (e.g. fewer errors = better).

NORM: dict[str, dict] = {
    # ── Serve Effectiveness ───────────────────────────────────────────────────
    "ace_pct":    dict(lo=1.5,  hi=7.0,  invert=False),   # Ace %
    "df_pct":     dict(lo=2.0,  hi=6.5,  invert=True),    # DF % (lower = better)
    "hld_pct":    dict(lo=60.0, hi=85.0, invert=False),   # Hold %
    "first_in":   dict(lo=57.0, hi=68.0, invert=False),   # 1st serve in %
    "first_w":    dict(lo=61.0, hi=74.0, invert=False),   # 1st serve win %
    "second_w":   dict(lo=44.0, hi=57.0, invert=False),   # 2nd serve win %
    "unret_pct":  dict(lo=16.0, hi=34.0, invert=False),   # Unreturnable serve %

    # ── Return / Receiving ────────────────────────────────────────────────────
    "brk_pct":    dict(lo=28.0, hi=50.0, invert=False),   # Break %
    "rip_w":      dict(lo=47.0, hi=63.0, invert=False),   # Return in-play win %
    "rpw":        dict(lo=36.0, hi=52.0, invert=False),   # Return points won %

    # ── Aggression / Power ────────────────────────────────────────────────────
    "wnr_pt":     dict(lo=12.0, hi=24.0, invert=False),   # Winners per point %
    "fh_wnr_pt":  dict(lo=6.0,  hi=15.0, invert=False),   # FH winners per point %
    "bh_wnr_pt":  dict(lo=3.0,  hi=11.0, invert=False),   # BH winners per point %
    "rally_agg":  dict(lo=25.0, hi=80.0, invert=False),   # RallyAgg (0-100 scale)
    "s13_w":      dict(lo=47.0, hi=60.0, invert=False),   # 1-3 shot rally win %

    # ── Consistency ───────────────────────────────────────────────────────────
    "ufe_pt":     dict(lo=13.0, hi=26.0, invert=True),    # UFEs per point % (lower = better)
    "vs_ufe_pt":  dict(lo=13.0, hi=26.0, invert=True),    # Opponent UFEs (grinder effect)

    # ── Movement / Defense ────────────────────────────────────────────────────
    "s10p_w":     dict(lo=48.0, hi=64.0, invert=False),   # 10+ shot rally win %
    "rally_len":  dict(lo=3.2,  hi=5.8,  invert=False),   # Avg rally length

    # ── Net Play ──────────────────────────────────────────────────────────────
    "snv_freq":   dict(lo=0.0,  hi=6.0,  invert=False),   # S&V frequency %
    "net_freq":   dict(lo=2.0,  hi=20.0, invert=False),   # Net approach frequency %
    "net_w":      dict(lo=52.0, hi=80.0, invert=False),   # Net win %

    # ── Spin / Topspin ────────────────────────────────────────────────────────
    "bh_slice_pct": dict(lo=3.0, hi=30.0, invert=True),  # BH slice % (lower = more topspin)
    "slice_ret_pct": dict(lo=3.0, hi=22.0, invert=False), # Return slice % (higher = slice tendency)
    "fhp100":     dict(lo=5.0,  hi=15.0, invert=False),   # FH patterns per 100 pts

    # ── Return game skill ─────────────────────────────────────────────────────
    "rip_pct":    dict(lo=58.0, hi=80.0, invert=False),   # Return in-play %
    "ret_wnr_pct": dict(lo=2.0, hi=12.0, invert=False),   # Return winner %

    # ── Mental / Clutch ───────────────────────────────────────────────────────
    "bp_saved":   dict(lo=50.0, hi=66.0, invert=False),   # BP Save %
    "gp_conv":    dict(lo=56.0, hi=70.0, invert=False),   # Game point conversion %

    # ── Variety / Drop shot ───────────────────────────────────────────────────
    "drop_freq":  dict(lo=0.0,  hi=4.0,  invert=False),   # Drop shot frequency %
    "return_agg": dict(lo=25.0, hi=75.0, invert=False),   # ReturnAgg (0-100 scale)
}


def _clamp(v: float, lo: float = 1.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, v))


def _normalize(raw: Optional[float], lo: float, hi: float,
               invert: bool = False, scale: float = 10.0) -> Optional[float]:
    """Linear map raw ∈ [lo, hi] → [1, scale], optionally inverted."""
    if raw is None:
        return None
    frac = (raw - lo) / (hi - lo) if hi != lo else 0.5
    frac = max(0.0, min(1.0, frac))
    if invert:
        frac = 1.0 - frac
    return _clamp(round(1 + frac * (scale - 1), 1))


def normalize_stat(key: str, raw_val: Optional[float]) -> Optional[float]:
    """Normalize a single raw stat to a 1-10 score using the NORM table."""
    if raw_val is None or key not in NORM:
        return None
    p = NORM[key]
    return _normalize(raw_val, p["lo"], p["hi"], p.get("invert", False))


def _safe_avg(*scores: Optional[float]) -> Optional[float]:
    """Average of all non-None scores. Returns None if all inputs are None."""
    valid = [s for s in scores if s is not None]
    return round(sum(valid) / len(valid), 1) if valid else None


def _score_or_default(score: Optional[float], default: float = 5.0) -> float:
    return score if score is not None else default


def compute_ratings_from_raw(raw_stats: Mapping[str, Optional[float]]) -> dict:
    """
    Compute 12 WTA style dimension ratings (1-10 int each) from raw scraped stats.

    Args:
        raw_stats: Dict of raw float stats keyed as output by tennisabstract_scraper.py.
                   Missing keys and None values are handled gracefully — dimensions
                   with no usable data default to 5.

    Returns:
        Dict of {dimension_name: int} for all 12 dimensions. Suitable for
        WTARatings(**result) instantiation.
    """

    # ── Pull raw values ───────────────────────────────────────────────────────
    hld       = raw_stats.get("hld_pct")
    brk       = raw_stats.get("brk_pct")
    ace       = raw_stats.get("ace_pct")
    df        = raw_stats.get("df_pct")
    f_in      = raw_stats.get("first_in")
    f_w       = raw_stats.get("first_w")
    s_w       = raw_stats.get("second_w")
    rpw       = raw_stats.get("rpw")
    wnr_pt    = raw_stats.get("wnr_pt")
    ufe_pt    = raw_stats.get("ufe_pt")
    fh_wnr    = raw_stats.get("fh_wnr_pt")
    bh_wnr    = raw_stats.get("bh_wnr_pt")
    vs_ufe    = raw_stats.get("vs_ufe_pt")
    bp_saved  = raw_stats.get("bp_saved")
    gp_conv   = raw_stats.get("gp_conv")
    unret     = raw_stats.get("unret_pct")
    lt3_w     = raw_stats.get("lt3_w")       # short point win % serving
    rip_w_s   = raw_stats.get("rip_w_serve") # return-in-play win % (serve perspective)
    rip       = raw_stats.get("rip_pct")
    rip_w     = raw_stats.get("rip_w")
    ret_wnr   = raw_stats.get("ret_wnr_pct")
    slice_r   = raw_stats.get("slice_ret_pct")
    fhbh_r    = raw_stats.get("fhbh_ratio")
    rlen      = raw_stats.get("rally_len")
    s13       = raw_stats.get("s13_w")
    s10p      = raw_stats.get("s10p_w")
    bh_sl     = raw_stats.get("bh_slice_pct")
    fhp100    = raw_stats.get("fhp100")
    bhp100    = raw_stats.get("bhp100")
    snv_f     = raw_stats.get("snv_freq")
    net_f     = raw_stats.get("net_freq")
    net_w     = raw_stats.get("net_w")
    drop_f    = raw_stats.get("drop_freq")
    r_agg     = raw_stats.get("rally_agg")
    ret_agg   = raw_stats.get("return_agg")

    # ── Dimension computations ────────────────────────────────────────────────

    # 1. FOREHAND POWER: FH winners, short rally win %, RallyAgg, overall winners
    forehand = _safe_avg(
        normalize_stat("fh_wnr_pt", fh_wnr),
        normalize_stat("s13_w",     s13),
        normalize_stat("rally_agg", r_agg),
        normalize_stat("wnr_pt",    wnr_pt),
    )

    # 2. BACKHAND QUALITY: BH winners, BH topspin (inverted BH slice %)
    bh_topspin = normalize_stat("bh_slice_pct", bh_sl)  # low slice → high topspin
    backhand = _safe_avg(
        normalize_stat("bh_wnr_pt", bh_wnr),
        bh_topspin,
    )

    # 3. SERVE EFFECTIVENESS: Ace%, Hold%, 1st serve %, 1st/2nd win %, Unret%
    serve = _safe_avg(
        normalize_stat("ace_pct",   ace),
        normalize_stat("df_pct",    df),
        normalize_stat("hld_pct",   hld),
        normalize_stat("first_in",  f_in),
        normalize_stat("first_w",   f_w),
        normalize_stat("second_w",  s_w),
        normalize_stat("unret_pct", unret),
    )

    # 4. NET PLAY: S&V freq, net approach freq, net win %, drop shot freq
    net_play = _safe_avg(
        normalize_stat("snv_freq",  snv_f),
        normalize_stat("net_freq",  net_f),
        normalize_stat("net_w",     net_w),
        normalize_stat("drop_freq", drop_f),
    )

    # 5. MOVEMENT / DEFENSE: Long rally win %, RPW, return RiP%, break %
    # Note: rally_len is intentionally excluded — short rallies indicate aggression,
    # not poor movement. Long rally win % and RPW are the real movement signals.
    movement = _safe_avg(
        normalize_stat("s10p_w",   s10p),
        normalize_stat("rpw",      rpw),
        normalize_stat("rip_pct",  rip),
        normalize_stat("brk_pct",  brk),
    )

    # 6. SPIN HEAVINESS: BH topspin (inverted BH slice), FH patterns per 100
    spin_heavy = _safe_avg(
        bh_topspin,
        normalize_stat("fhp100", fhp100),
    )

    # 7. CONSISTENCY: low UFE/Pt, low DF%, opponent UFE rate (grinder effect)
    consistency = _safe_avg(
        normalize_stat("ufe_pt",   ufe_pt),
        normalize_stat("df_pct",   df),
        normalize_stat("vs_ufe_pt", vs_ufe),
    )

    # 8. AGGRESSION: Winners per point, RallyAgg, short rally %, ReturnAgg
    aggression = _safe_avg(
        normalize_stat("wnr_pt",    wnr_pt),
        normalize_stat("rally_agg", r_agg),
        normalize_stat("s13_w",     s13),
        normalize_stat("return_agg", ret_agg),
    )

    # 9. MENTAL GAME: BP Save%, Game point conversion %
    mental_game = _safe_avg(
        normalize_stat("bp_saved", bp_saved),
        normalize_stat("gp_conv",  gp_conv),
    )

    # 10. RETURN GAME: Break%, RPW, RiP%, RiP win%, return winners
    return_game = _safe_avg(
        normalize_stat("brk_pct",    brk),
        normalize_stat("rpw",        rpw),
        normalize_stat("rip_pct",    rip),
        normalize_stat("rip_w",      rip_w),
        normalize_stat("ret_wnr_pct", ret_wnr),
    )

    # 11. VARIETY: net freq, drop freq, return slice %, FH/BH ratio diversity
    # High FH/BH ratio = FH-dominant (less variety). Optimal variety ≈ 1.0 ratio.
    fhbh_variety: Optional[float] = None
    if fhbh_r is not None:
        deviation = abs(fhbh_r - 1.0)
        fhbh_variety = _clamp(round(10 - deviation * 3, 1))

    variety = _safe_avg(
        normalize_stat("net_freq",     net_f),
        normalize_stat("drop_freq",    drop_f),
        normalize_stat("slice_ret_pct", slice_r),
        fhbh_variety,
    )

    # 12. RISK TAKING: winner/UFE ratio combined with absolute winner rate.
    # ratio < 1.0 = more errors than winners (risky/aggressive), > 1.0 = safer.
    # Zero-safe: use `is not None` checks, not truthiness, to handle 0.0 correctly.
    risk: Optional[float] = None
    if wnr_pt is not None and ufe_pt is not None:
        ratio = wnr_pt / ufe_pt if ufe_pt > 0 else 1.0
        risk_raw = wnr_pt * (1.0 / max(ratio, 0.5))
        risk = _normalize(risk_raw, lo=10.0, hi=28.0, invert=False)

    risk_taking = _safe_avg(
        risk,
        normalize_stat("wnr_pt", wnr_pt),
    )

    # ── Finalize to integers, defaulting missing dimensions to 5 ─────────────
    def finalize(score: Optional[float]) -> int:
        return int(round(_score_or_default(score, 5.0)))

    return {
        "forehand":    finalize(forehand),
        "backhand":    finalize(backhand),
        "serve":       finalize(serve),
        "netPlay":     finalize(net_play),
        "movement":    finalize(movement),
        "spinHeavy":   finalize(spin_heavy),
        "consistency": finalize(consistency),
        "aggression":  finalize(aggression),
        "mentalGame":  finalize(mental_game),
        "returnGame":  finalize(return_game),
        "variety":     finalize(variety),
        "riskTaking":  finalize(risk_taking),
    }
