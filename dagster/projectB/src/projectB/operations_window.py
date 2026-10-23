from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo


OPERATIONS_TIMEZONE_NAME = "America/Ciudad_Juarez"
DEFAULT_OPERATIONS_BOOTSTRAP_START = datetime(2025, 10, 1)
DEFAULT_FACTREADINGS_OVERLAP_HOURS = 12


def get_operations_timezone():
    try:
        return ZoneInfo(OPERATIONS_TIMEZONE_NAME)
    except Exception:
        return timezone(timedelta(hours=-6), name=OPERATIONS_TIMEZONE_NAME)


OPERATIONS_TIMEZONE = get_operations_timezone()


def truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def coerce_local_naive(
    dt: datetime,
    timezone: tzinfo = OPERATIONS_TIMEZONE,
) -> datetime:
    if dt.tzinfo is None:
        return truncate_to_seconds(dt)
    return truncate_to_seconds(
        dt.astimezone(timezone).replace(tzinfo=None)
    )


def current_operational_hour_start(
    now: Optional[datetime] = None,
    timezone: ZoneInfo = OPERATIONS_TIMEZONE,
) -> datetime:
    current = now if now is not None else datetime.now(timezone)
    if current.tzinfo is not None:
        current = current.astimezone(timezone).replace(tzinfo=None)
    current = truncate_to_seconds(current)
    return current.replace(minute=0, second=0, microsecond=0)


def factreadings_overlap_delta() -> timedelta:
    overlap_hours = int(
        os.getenv(
            "FACTREADINGS_OVERLAP_HOURS",
            str(DEFAULT_FACTREADINGS_OVERLAP_HOURS),
        )
    )
    if overlap_hours < 1:
        raise ValueError("FACTREADINGS_OVERLAP_HOURS must be greater than or equal to 1")
    return timedelta(hours=overlap_hours - 1)


def current_utc_timestamp(now: Optional[datetime] = None) -> datetime:
    current = now if now is not None else datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return truncate_to_seconds(
        current.astimezone(timezone.utc).replace(tzinfo=None)
    )


def build_operations_window(
    last_loaded_at: Optional[datetime],
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    if last_loaded_at is None:
        last_loaded_at = DEFAULT_OPERATIONS_BOOTSTRAP_START

    from_time_span = coerce_local_naive(last_loaded_at, timezone.utc)
    max_utc_hour = current_utc_timestamp(now).replace(
        minute=0, second=0, microsecond=0
    )
    watermark_in_future = from_time_span > max_utc_hour

    processing_anchor = min(from_time_span, max_utc_hour)
    fetch_from = (
        processing_anchor.replace(minute=0, second=0, microsecond=0)
        - factreadings_overlap_delta()
    )

    return {
        "delete_from": fetch_from,
        "delete_to_exclusive": max(from_time_span, max_utc_hour) + timedelta(hours=1),
        "fetch_from": fetch_from,
        "from_time_span": from_time_span,
        "max_utc_hour": max_utc_hour,
        "read_to_utc": current_utc_timestamp(now),
        "original_last_loaded_at": last_loaded_at,
        "watermark_in_future": watermark_in_future,
    }


def resolve_operations_watermark(
    *,
    succeeded: bool,
    original_last_loaded_at: datetime,
    max_time_span: Optional[datetime],
    max_utc_hour: datetime,
) -> datetime:
    if not succeeded:
        return original_last_loaded_at
    if max_time_span is None:
        return max_utc_hour
    return max_time_span
