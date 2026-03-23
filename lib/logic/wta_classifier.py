from lib.schemas.wta import WTARatings, ArchetypeScore, WTAPlayerArchetype

ARCHETYPE_DEFINITIONS = [
    {
        "id": "flat-hitter",
        "name": "Flat Ball Striker",
        "weights": {"spinHeavy": -2, "aggression": 2, "forehand": 1.5, "backhand": 1.5, "serve": 1, "riskTaking": -0.5},
    },
    {
        "id": "weapon-backhand",
        "name": "Backhand Weapon",
        "weights": {"backhand": 3, "aggression": 1, "consistency": 0.5},
    },
    {
        "id": "forehand-dominant",
        "name": "Forehand Dominant",
        "weights": {"forehand": 3, "aggression": 1.5, "spinHeavy": 0.5},
    },
    {
        "id": "big-server",
        "name": "Big Server",
        "weights": {"serve": 3.5, "aggression": 1.5, "forehand": 0.5},
    },
    {
        "id": "topspin-grinder",
        "name": "Topspin Grinder",
        "weights": {"spinHeavy": 2.5, "consistency": 2, "movement": 1.5, "aggression": 0.5},
    },
    {
        "id": "counterpuncher",
        "name": "Counterpuncher",
        "weights": {"movement": 2.5, "consistency": 2.5, "aggression": -1.5, "mentalGame": 1},
    },
    {
        "id": "all-court",
        "name": "All-Court Player",
        "weights": {"netPlay": 1, "movement": 1, "forehand": 1, "backhand": 1, "serve": 1, "consistency": 1},
    },
    {
        "id": "aggressive-baseliner",
        "name": "Aggressive Baseliner",
        "weights": {"aggression": 2.5, "forehand": 1.5, "backhand": 1.5, "consistency": 0.5, "riskTaking": 0.5},
    },
    {
        "id": "net-rusher",
        "name": "Serve & Net Rusher",
        "weights": {"netPlay": 3, "serve": 2, "aggression": 1.5},
    },
    {
        "id": "tactician",
        "name": "Tactician / Variety",
        "weights": {"variety": 3.5, "netPlay": 1.5, "consistency": 1, "aggression": -0.5, "spinHeavy": -0.5},
    },
    {
        "id": "touch-constructor",
        "name": "Touch & Construction",
        "weights": {"netPlay": 3, "variety": 2, "backhand": 1, "movement": 1, "serve": -0.5},
    },
    {
        "id": "streaky-bomber",
        "name": "Streaky / High-Risk Bomber",
        "weights": {"riskTaking": 3.5, "aggression": 2.5, "consistency": -2, "forehand": 1, "backhand": 1},
    },
    {
        "id": "return-specialist",
        "name": "Return Specialist",
        "weights": {"returnGame": 3.5, "movement": 1.5, "mentalGame": 1.5, "aggression": 1},
    },
]


def classify_player(name: str, ratings: WTARatings, emoji: str = "🎾") -> WTAPlayerArchetype:
    """
    Computes weighted archetype scores for a player and returns a validated
    WTAPlayerArchetype with primary and secondary archetype assignments.

    Scoring formula per archetype:
      - Normalize each rating: val = rating / 10.0  (range: 0.1 – 1.0)
      - weighted_sum = sum(weight * val for each metric in archetype)
      - denom = sum(abs(weight) for each metric in archetype)
      - score = weighted_sum / denom

    Negative weights (e.g. spinHeavy: -2 for Flat Ball Striker) reduce the
    score when that dimension is high, encoding style incompatibilities.
    Note: because ratings floor at 1.0 (val = 0.1), negative weights never
    fully zero out — a spinHeavy rating of 1.0 still contributes -0.2.
    """
    rating_dict = ratings.model_dump()
    scores = []

    for arch in ARCHETYPE_DEFINITIONS:
        weighted_sum = 0.0
        denom = 0.0

        for metric, weight in arch["weights"].items():
            val = rating_dict.get(metric, 5.0) / 10.0
            weighted_sum += weight * val
            denom += abs(weight)

        final_score = round(weighted_sum / denom if denom > 0 else 0.0, 4)
        scores.append(ArchetypeScore(name=arch["name"], score=final_score))

    ranked = sorted(scores, key=lambda x: x.score, reverse=True)

    return WTAPlayerArchetype(
        name=name,
        emoji=emoji,
        ratings=ratings,
        primary_archetype=ranked[0].name,
        secondary_archetype=ranked[1].name,
        archetype_scores=ranked,
    )
