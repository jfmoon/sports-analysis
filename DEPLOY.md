# Deployment Instructions
# CBB Analysis Jobs — sports-analysis repo
# 2026-03-23

## Files to add/modify

Three files are being added. Nothing in lib/schemas/inputs.py, lib/storage.py,
or requirements.txt needs to change.

  lib/team_names.py                  ← NEW
  jobs/cbb_archetype_classifier.py   ← NEW
  jobs/cbb_projector.py              ← NEW


## Step 1 — Copy files into the repo

From the root of your sports-analysis checkout:

  cp lib/team_names.py                ../sports-analysis/lib/team_names.py
  cp jobs/cbb_archetype_classifier.py ../sports-analysis/jobs/cbb_archetype_classifier.py
  cp jobs/cbb_projector.py            ../sports-analysis/jobs/cbb_projector.py

Or just paste them directly into the repo — no other files are touched.


## Step 2 — Verify imports resolve

The two jobs import from lib, which must be on the Python path.
Cloud Run Jobs already handle this if the repo root is the working directory.

For local testing, run from the repo root:

  cd /path/to/sports-analysis
  python -m jobs.cbb_archetype_classifier
  python -m jobs.cbb_projector

Do NOT run as `python jobs/cbb_archetype_classifier.py` from the repo root —
relative imports from lib/ will fail. Use -m.


## Step 3 — Local smoke test

Set these env vars before running (the __main__ blocks set defaults,
but you can override to point at real GCS data):

  export TRIGGER_GCS_BUCKET=sports-data-scraper-491116
  export TRIGGER_GCS_PATH=cbb/kenpom.json
  export TRIGGER_GCS_GEN=0
  export TRIGGER_MESSAGE_ID=local-smoke-001

  python -m jobs.cbb_archetype_classifier
  # Expect: "[local-smoke-001] INFO: Classified N teams"
  # Expect: cbb/archetypes.json written to processed bucket

  export TRIGGER_GCS_PATH=cbb/kenpom.json
  python -m jobs.cbb_projector
  # Expect: "[local-smoke-001] INFO: Projected N games"
  # Expect: cbb/projections.json written to processed bucket
  # If no active odds lines: "Projected 0 games" is correct, not an error.


## Step 4 — Things to watch on first live run

1. Name resolution misses
   Both jobs log a WARNING for any team that fails to match between KenPom
   and Action Network. Check Cloud Run logs after first run:

     grep "Resolution failed" <log output>
     grep "Name Resolution Collision" <log output>

   If you see consistent misses for the same team names, update the
   resolve_name_key() function in lib/team_names.py. Common cases:
   - Abbreviations (YSU, UConn, VCU) — need a manual alias dict
   - Disambiguation suffixes (St. Francis PA vs St. Francis NY)

2. Fallback JBScore
   If any team triggers the barthag*45 fallback you will see:

     WARNING: Fallback JBScore used for <team name>

   This means adj_o or adj_d was null for that team in the KenPom snapshot.
   Check the scraper output for that team.

3. Upset score range
   upset_score is signed (-100 to +100). Negative values mean the underdog
   is statistically worse than the favorite on every upset metric — this is
   valid signal, not an error.


## Step 5 — Git

  git checkout -b feature/cbb-analysis-jobs
  git add lib/team_names.py jobs/cbb_archetype_classifier.py jobs/cbb_projector.py
  git commit -m "Add CBB archetype classifier and projector jobs

  - lib/team_names.py: collision-safe team name resolver, shared by both jobs
  - jobs/cbb_archetype_classifier.py: KenPom style classification, 8 archetypes
  - jobs/cbb_projector.py: JBScore, StabilityScore, jbGap edge calculation
  "
  git push origin feature/cbb-analysis-jobs

Then open a PR against main.


## Step 6 — Cloud Run Job config (if not already set)

Each job needs these env vars injected by the triggering Cloud Function:

  TRIGGER_GCS_BUCKET   = sports-data-scraper-491116
  TRIGGER_GCS_PATH     = cbb/kenpom.json  (or cbb/odds.json for projector)
  TRIGGER_GCS_GEN      = <generation ID from GCS event>
  TRIGGER_MESSAGE_ID   = <Pub/Sub message ID for idempotency>

The classifier should only be triggered by writes to cbb/kenpom.json.
The projector should be triggered by writes to either cbb/kenpom.json or cbb/odds.json.
