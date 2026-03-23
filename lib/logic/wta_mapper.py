from typing import Optional
import logging
from lib.schemas.wta import WTARatings

logger = logging.getLogger(__name__)


def map_stats_to_ratings(raw_stats: dict) -> WTARatings:
    """
    Translates raw scraper percentages into 1-10 ratings.

    Currently maps 4 metrics from live data; the remaining 8 dimensions
    use placeholder values (5.0 / 6.0) until the scraper is expanded.
    This means archetypes that weight forehand, backhand, netPlay, spinHeavy,
    variety, and riskTaking heavily will cluster and produce low-signal output
    until those fields are populated.

    Scaling ranges are based on WTA Tour 3-year averages.
    """

    def scale(
        val: Optional[float],
        min_val: float,
        max_val: float,
        invert: bool = False,
    ) -> float:
        if val is None:
            return 5.0
        norm = (val - min_val) / (max_val - min_val)
        if invert:
            norm = 1.0 - norm
        result = 1 + (9 * norm)
        return round(max(1.0, min(10.0, result)), 1)

    ratings = {
        # Serve: ace_pct range [1.0%, 15.0%]
        "serve": scale(raw_stats.get("ace_pct"), 1.0, 15.0),

        # Return: ret_pts_won_pct range [35.0%, 55.0%]
        "returnGame": scale(raw_stats.get("ret_pts_won_pct"), 35.0, 55.0),

        # Consistency: df_pct range [1.0%, 10.0%] — inverted (low DF = high consistency)
        "consistency": scale(raw_stats.get("df_pct"), 1.0, 10.0, invert=True),

        # Aggression: fsv_pts_won_pct range [55.0%, 75.0%]
        "aggression": scale(raw_stats.get("fsv_pts_won_pct"), 55.0, 75.0),

        # --- Placeholders (pending scraper expansion) ---
        "forehand": 5.0,
        "backhand": 5.0,
        "netPlay": 5.0,
        "movement": 6.0,
        "spinHeavy": 5.0,
        "mentalGame": 5.0,
        "variety": 5.0,
        "riskTaking": 5.0,
    }

    return WTARatings(**ratings)
