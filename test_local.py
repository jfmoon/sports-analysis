import json
from google.cloud import storage
from lib.schemas.inputs import (
    KenPomSnapshot,
    ESPNSnapshot,
    ActionNetworkSnapshot,
    TennisOddsSnapshot,
    TennisAbstractSnapshot,
    GCS_PATH_REGISTRY,
)

RAW_BUCKET = "sports-data-scraper-491116"
client = storage.Client()

paths_to_test = [
    "cbb/kenpom.json",
    "cbb/scores.json",
    "cbb/odds.json",
    "tennis/odds.json",
    "tennis/players.json",
]

print("\n=== Schema Validation Test ===\n")
for path in paths_to_test:
    model_cls = GCS_PATH_REGISTRY[path]
    try:
        raw = client.bucket(RAW_BUCKET).blob(path).download_as_text()
        data = json.loads(raw)
        snapshot = model_cls.model_validate(data)
        print(f"✓ {path} → {model_cls.__name__}")

        # Spot check record counts
        if hasattr(snapshot, "teams"):
            print(f"  teams: {len(snapshot.teams)}")
        if hasattr(snapshot, "games"):
            print(f"  games: {len(snapshot.games)}")
        if hasattr(snapshot, "odds"):
            print(f"  matches: {len(snapshot.odds)}")
        if hasattr(snapshot, "players"):
            print(f"  players: {len(snapshot.players)}")

    except Exception as e:
        print(f"✗ {path} → FAILED: {e}")

print("\n=== Storage.py from_env() Test ===\n")
import os
os.environ["TRIGGER_GCS_BUCKET"] = "sports-data-scraper-491116"
os.environ["TRIGGER_GCS_PATH"]   = "cbb/kenpom.json"
os.environ["TRIGGER_GCS_GEN"]    = "0"  # will use latest
os.environ["TRIGGER_MESSAGE_ID"] = "test-local-001"

from lib.storage import AnalysisStorage
storage = AnalysisStorage.from_env()
print("✓ AnalysisStorage.from_env() constructed successfully")

# Test pinned read falls back gracefully on gen=0
try:
    raw = client.bucket(RAW_BUCKET).blob("cbb/kenpom.json").download_as_text()
    snapshot = KenPomSnapshot.model_validate(json.loads(raw))
    top = snapshot.teams[0]
    print(f"✓ KenPom top team: {top.name} | AdjEM: {top.adj_em} | Rank: {top.kenpom_rank}")
except Exception as e:
    print(f"✗ KenPom read failed: {e}")

print("\n=== Tennis computed properties test ===\n")
try:
    raw = client.bucket(RAW_BUCKET).blob("tennis/odds.json").download_as_text()
    snap = TennisOddsSnapshot.model_validate(json.loads(raw))
    m = snap.odds[0]
    print(f"✓ Match: {m.p1_name} vs {m.p2_name}")
    print(f"  p1_ml={m.p1_ml} → implied={m.p1_implied_prob} no_vig={m.no_vig_p1}")
    print(f"  p2_ml={m.p2_ml} → implied={m.p2_implied_prob} no_vig={m.no_vig_p2}")
    print(f"  vig={m.vig}")
except Exception as e:
    print(f"✗ Tennis odds test failed: {e}")

print("\n=== All done ===\n")
