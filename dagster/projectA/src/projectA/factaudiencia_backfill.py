from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Dict


DEFAULT_FACTAUDIENCIA_BACKFILL_START_DATE = "2025-10-01"


def parse_utc_datetime(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("UTC datetime value must be a non-empty string")

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)

    return parsed.replace(microsecond=0)


def validate_backfill_window(from_utc: datetime, to_utc: datetime) -> None:
    if from_utc >= to_utc:
        raise ValueError("FactAudiencia backfill requires from_utc to be before to_utc")


def build_backfill_window(from_utc: str, to_utc: str) -> Dict[str, datetime]:
    parsed_from = parse_utc_datetime(from_utc)
    parsed_to = parse_utc_datetime(to_utc)
    validate_backfill_window(parsed_from, parsed_to)
    return {"from_utc": parsed_from, "to_utc": parsed_to}


def build_daily_backfill_window(partition_key: str) -> Dict[str, datetime]:
    start = datetime.strptime(partition_key, "%Y-%m-%d")
    end = start + timedelta(days=1)
    return {"from_utc": start, "to_utc": end}


def get_factaudiencia_backfill_start_datetime() -> datetime:
    start_date = os.getenv(
        "FACTAUDIENCIA_BACKFILL_START_DATE",
        DEFAULT_FACTAUDIENCIA_BACKFILL_START_DATE,
    )
    return datetime.strptime(start_date, "%Y-%m-%d")
