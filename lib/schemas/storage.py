"""
storage.py — GCS read/write utility for the Sports Analysis layer.

Reads from:  sports-data-scraper-491116  (raw scraper snapshots)
Writes to:   sports-processed-data-491116 (analysis outputs)

Key design: all reads are pinned to the GCS generation ID passed by the
Cloud Function trigger. This guarantees the analysis job processes the
exact file version that fired the event — not a newer write that may have
arrived between trigger and execution.

Usage in a Cloud Run Job:
    from lib.storage import AnalysisStorage
    storage = AnalysisStorage.from_env()
    snapshot = storage.read_trigger_snapshot()   # type-validated via schemas
    storage.write_processed("cbb/projections.json", output_dict)
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from google.cloud import storage as gcs

from lib.schemas.inputs import GCS_PATH_REGISTRY

RAW_BUCKET      = "sports-data-scraper-491116"
PROCESSED_BUCKET = "sports-processed-data-491116"

logger = logging.getLogger(__name__)


class AnalysisStorage:
    def __init__(
        self,
        gcs_bucket: str,
        gcs_path: str,
        gcs_generation: int,
        message_id: str,
        raw_bucket: str = RAW_BUCKET,
        processed_bucket: str = PROCESSED_BUCKET,
    ):
        self.gcs_bucket     = gcs_bucket
        self.gcs_path       = gcs_path
        self.gcs_generation = gcs_generation
        self.message_id     = message_id
        self.raw_bucket     = raw_bucket
        self.processed_bucket = processed_bucket
        self._client: Optional[gcs.Client] = None

    @classmethod
    def from_env(cls) -> "AnalysisStorage":
        """
        Construct from Cloud Run Job environment variables injected by the
        Cloud Function orchestrator. Raises clearly if any are missing.
        """
        required = [
            "TRIGGER_GCS_BUCKET",
            "TRIGGER_GCS_PATH",
            "TRIGGER_GCS_GEN",
            "TRIGGER_MESSAGE_ID",
        ]
        missing = [k for k in required if not os.environ.get(k)]
        if missing:
            raise EnvironmentError(
                f"Missing required env vars: {missing}. "
                "Was this job invoked by the orchestrator Cloud Function?"
            )

        return cls(
            gcs_bucket=os.environ["TRIGGER_GCS_BUCKET"],
            gcs_path=os.environ["TRIGGER_GCS_PATH"],
            gcs_generation=int(os.environ["TRIGGER_GCS_GEN"]),
            message_id=os.environ["TRIGGER_MESSAGE_ID"],
        )

    @property
    def client(self) -> gcs.Client:
        """Lazy GCS client — uses ADC, no key files."""
        if self._client is None:
            self._client = gcs.Client()
        return self._client

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read_raw(self, path: Optional[str] = None, generation: Optional[int] = None) -> dict:
        """
        Read a raw JSON file from the scraper bucket.
        Defaults to the trigger path + generation (pinned read).
        Pass explicit path/generation to read a different file.
        """
        bucket     = self.client.bucket(self.raw_bucket)
        blob_path  = path or self.gcs_path
        blob_gen   = generation or self.gcs_generation

        blob = bucket.blob(blob_path, generation=blob_gen)
        logger.info(
            f"[{self.message_id}] Reading gs://{self.raw_bucket}/{blob_path} "
            f"gen={blob_gen}"
        )
        raw = blob.download_as_text()
        return json.loads(raw)

    def read_trigger_snapshot(self):
        """
        Read and validate the triggering GCS file against its Pydantic schema.
        Returns a typed snapshot object (e.g. KenPomSnapshot, TennisOddsSnapshot).
        Raises KeyError if the GCS path is not in GCS_PATH_REGISTRY.
        Raises ValidationError if the data doesn't match the schema.
        """
        model_cls = GCS_PATH_REGISTRY.get(self.gcs_path)
        if model_cls is None:
            raise KeyError(
                f"No schema registered for GCS path '{self.gcs_path}'. "
                f"Add it to lib/schemas/inputs.py GCS_PATH_REGISTRY."
            )

        data = self.read_raw()
        logger.info(
            f"[{self.message_id}] Validating {self.gcs_path} "
            f"against {model_cls.__name__}"
        )
        return model_cls.model_validate(data)

    def read_snapshot(self, path: str):
        """
        Read and validate any registered GCS snapshot by path (latest version).
        Use this when a job needs to pull a secondary source alongside the trigger.
        Example: CBB projector reads both kenpom.json and odds.json.
        """
        model_cls = GCS_PATH_REGISTRY.get(path)
        if model_cls is None:
            raise KeyError(f"No schema registered for path '{path}'.")

        bucket = self.client.bucket(self.raw_bucket)
        raw = bucket.blob(path).download_as_text()
        logger.info(f"[{self.message_id}] Reading secondary snapshot: {path}")
        return model_cls.model_validate(json.loads(raw))

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write_processed(self, output_path: str, data: dict | list) -> str:
        """
        Write analysis output to the processed bucket.
        Automatically stamps provenance metadata into the payload.

        Args:
            output_path: GCS blob path in processed bucket
                         e.g. "cbb/projections.json"
            data:        Dict or list to serialize as JSON

        Returns:
            Full GCS URI of the written blob.
        """
        payload = {
            "_provenance": {
                "trigger_path":       self.gcs_path,
                "trigger_generation": self.gcs_generation,
                "trigger_message_id": self.message_id,
                "processed_at":       datetime.now(timezone.utc).isoformat(),
                "source_bucket":      self.raw_bucket,
                "output_bucket":      self.processed_bucket,
            },
            "data": data,
        }

        bucket = self.client.bucket(self.processed_bucket)
        blob   = bucket.blob(output_path)
        blob.upload_from_string(
            json.dumps(payload, indent=2, default=str),
            content_type="application/json",
        )

        uri = f"gs://{self.processed_bucket}/{output_path}"
        logger.info(f"[{self.message_id}] Written to {uri}")
        return uri

    def write_processed_archive(self, output_path: str, data: dict | list) -> str:
        """
        Write a timestamped archive copy to processed bucket.
        Use alongside write_processed to keep a history of analysis runs.

        Output path example:
            "cbb/projections/2026-03-23/143022.json"
        """
        ts = datetime.now(timezone.utc)
        date_str = ts.strftime("%Y-%m-%d")
        time_str = ts.strftime("%H%M%S")

        base, ext = output_path.rsplit(".", 1)
        archive_path = f"{base}/{date_str}/{time_str}.{ext}"

        return self.write_processed(archive_path, data)
