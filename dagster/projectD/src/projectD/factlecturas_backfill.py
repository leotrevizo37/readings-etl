from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Sequence
from uuid import UUID


DEFAULT_FACTLECTURAS_BACKFILL_START_DATE = "2025-10-01"


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
        raise ValueError("FactLecturas backfill requires from_utc to be before to_utc")


def build_backfill_window(from_utc: str, to_utc: str) -> Dict[str, datetime]:
    parsed_from = parse_utc_datetime(from_utc)
    parsed_to = parse_utc_datetime(to_utc)
    validate_backfill_window(parsed_from, parsed_to)
    return {"from_utc": parsed_from, "to_utc": parsed_to}


def build_daily_backfill_window(partition_key: str) -> Dict[str, datetime]:
    start = datetime.strptime(partition_key, "%Y-%m-%d")
    end = start + timedelta(days=1)
    return {"from_utc": start, "to_utc": end}


def normalize_sensor_ids(sensor_ids: Sequence[str]) -> list[str]:
    if isinstance(sensor_ids, (str, bytes)) or not sensor_ids:
        raise ValueError("FactLecturas sensor backfill requires a non-empty sensor_ids list")

    normalized_ids = []
    seen = set()
    for sensor_id in sensor_ids:
        raw_sensor_id = str(sensor_id).strip()
        if not raw_sensor_id:
            raise ValueError("FactLecturas sensor_ids cannot contain empty values")

        normalized_id = str(UUID(raw_sensor_id)).upper()
        if normalized_id not in seen:
            normalized_ids.append(normalized_id)
            seen.add(normalized_id)

    return normalized_ids


def build_sensor_backfill_request(
    from_utc: str,
    to_utc: str,
    sensor_ids: Sequence[str],
) -> Dict[str, Any]:
    window = build_backfill_window(from_utc, to_utc)
    return {
        "from_utc": window["from_utc"],
        "to_utc": window["to_utc"],
        "sensor_ids": normalize_sensor_ids(sensor_ids),
        "delete_before_load": False,
    }


def get_factlecturas_backfill_start_datetime() -> datetime:
    start_date = os.getenv(
        "FACTLECTURAS_BACKFILL_START_DATE",
        DEFAULT_FACTLECTURAS_BACKFILL_START_DATE,
    )
    return datetime.strptime(start_date, "%Y-%m-%d")
