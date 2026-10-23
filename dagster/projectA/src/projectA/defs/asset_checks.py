from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check


PROJECT_NAME = "projectA"
MIN_REAL_READINGS_RATIO = 0.6
TARGET_REAL_READINGS_RATIO = 0.66

FACTREADINGS_ASSET_KEY = AssetKey([PROJECT_NAME, "dwh_factReadingsMeasurement"])
OPERATIONS_CONTROL_ASSET_KEY = AssetKey([PROJECT_NAME, "ctl_IngestionControl"])
FACTLECTURAS_ASSET_KEY = AssetKey([PROJECT_NAME, "dwh_factlecturas"])
FACTAUDIENCIA_ASSET_KEY = AssetKey([PROJECT_NAME, "dwh_factaudiencia"])


def _metadata_value(metadata: Mapping[str, Any], key: str) -> Any:
    value = metadata.get(key)
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "text"):
        return value.text
    if hasattr(value, "data"):
        return value.data
    return value


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    return None


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        value = value.strip()
        if not value or value.lower() == "none":
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _latest_materialization_metadata(context: Any, asset_key: AssetKey) -> Mapping[str, Any]:
    materialization_event = context.instance.get_latest_materialization_event(asset_key)
    if materialization_event is None:
        return {}

    materialization = getattr(materialization_event, "asset_materialization", None)
    if materialization is not None:
        return materialization.metadata or {}

    dagster_event = getattr(materialization_event, "dagster_event", None)
    event_data = getattr(dagster_event, "event_specific_data", None)
    materialization = getattr(event_data, "materialization", None)
    if materialization is not None:
        return materialization.metadata or {}

    return getattr(materialization_event, "metadata", {}) or {}


def _build_real_readings_ratio_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    row_count = _coerce_int(_metadata_value(metadata, "row_count")) or 0
    rows_with_readings = (
        _coerce_int(_metadata_value(metadata, "rows_with_readings_count_gt_0")) or 0
    )
    ratio = rows_with_readings / row_count if row_count else 0.0
    passed = ratio >= TARGET_REAL_READINGS_RATIO
    severity = (
        AssetCheckSeverity.ERROR
        if ratio < MIN_REAL_READINGS_RATIO
        else AssetCheckSeverity.WARN
    )

    return AssetCheckResult(
        passed=bool(row_count > 0 and passed),
        severity=severity,
        metadata={
            "row_count": row_count,
            "rows_with_readings_count_gt_0": rows_with_readings,
            "ratio": round(ratio, 4),
            "minimum_ratio": MIN_REAL_READINGS_RATIO,
            "target_ratio": TARGET_REAL_READINGS_RATIO,
        },
    )


def _build_utc_window_order_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    from_utc = _coerce_datetime(_metadata_value(metadata, "from_utc"))
    to_utc = _coerce_datetime(_metadata_value(metadata, "to_utc"))

    return AssetCheckResult(
        passed=bool(from_utc is not None and to_utc is not None
                    and to_utc > from_utc),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "from_utc": str(from_utc),
            "to_utc": str(to_utc),
        },
    )


def _build_operations_succeeded_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    succeeded = _coerce_bool(_metadata_value(metadata, "succeeded"))

    return AssetCheckResult(
        passed=bool(succeeded is True),
        metadata={"succeeded": bool(succeeded is True)},
    )


def _build_to_utc_gte_max_time_span_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    to_utc = _coerce_datetime(_metadata_value(metadata, "to_utc"))
    max_time_span = _coerce_datetime(_metadata_value(metadata, "max_time_span"))
    time_delta = (
        to_utc - max_time_span
        if to_utc is not None and max_time_span is not None
        else None
    )
    passed = to_utc is not None and (
        max_time_span is None or (
            time_delta is not None
            and timedelta(0) <= time_delta < timedelta(hours=1)
        )
    )
    severity = (
        AssetCheckSeverity.ERROR
        if to_utc is None or (
            time_delta is not None
            and (time_delta < timedelta(0) or time_delta > timedelta(hours=2))
        )
        else AssetCheckSeverity.WARN
    )

    return AssetCheckResult(
        passed=bool(passed),
        severity=severity,
        metadata={
            "to_utc": str(to_utc),
            "max_time_span": str(max_time_span),
            "time_delta": str(time_delta),
        },
    )


def _build_original_lte_to_utc_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    original_last_loaded_at = _coerce_datetime(
        _metadata_value(metadata, "original_last_loaded_at")
    )
    to_utc = _coerce_datetime(_metadata_value(metadata, "to_utc"))

    return AssetCheckResult(
        passed=bool(original_last_loaded_at is not None and to_utc is not None
                    and original_last_loaded_at <= to_utc),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "original_last_loaded_at": str(original_last_loaded_at),
            "to_utc": str(to_utc),
        },
    )


def _build_factlecturas_row_count_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    row_count = _coerce_int(_metadata_value(metadata, "row_count")) or 0

    return AssetCheckResult(
        passed=bool(row_count > 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": row_count},
    )


def _build_factlecturas_original_lte_to_utc_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    original_last_loaded_at = _coerce_datetime(
        _metadata_value(metadata, "original_last_loaded_at")
    )
    to_utc = _coerce_datetime(_metadata_value(metadata, "to_utc"))

    return AssetCheckResult(
        passed=bool(original_last_loaded_at is not None and to_utc is not None
                    and original_last_loaded_at <= to_utc),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "original_last_loaded_at": str(original_last_loaded_at),
            "to_utc": str(to_utc),
        },
    )


def _build_factaudiencia_row_count_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    row_count = _coerce_int(_metadata_value(metadata, "row_count")) or 0

    return AssetCheckResult(
        passed=bool(row_count > 0),
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": row_count},
    )


def _build_factaudiencia_original_lte_to_utc_result(
        metadata: Mapping[str, Any]) -> AssetCheckResult:
    original_last_loaded_at = _coerce_datetime(
        _metadata_value(metadata, "original_last_loaded_at")
    )
    to_utc = _coerce_datetime(_metadata_value(metadata, "to_utc"))

    return AssetCheckResult(
        passed=bool(original_last_loaded_at is not None and to_utc is not None
                    and original_last_loaded_at <= to_utc),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "original_last_loaded_at": str(original_last_loaded_at),
            "to_utc": str(to_utc),
        },
    )


@asset_check(
    asset=FACTREADINGS_ASSET_KEY,
    name="real_readings_ratio_gt_0_6",
)
def factreadings_real_readings_ratio_gt_0_6(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTREADINGS_ASSET_KEY)
    return _build_real_readings_ratio_result(metadata)


@asset_check(
    asset=FACTREADINGS_ASSET_KEY,
    name="to_utc_gt_from_utc",
)
def factreadings_to_utc_gt_from_utc(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTREADINGS_ASSET_KEY)
    return _build_utc_window_order_result(metadata)


@asset_check(
    asset=OPERATIONS_CONTROL_ASSET_KEY,
    name="succeeded_is_true",
)
def operations_control_succeeded_is_true(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, OPERATIONS_CONTROL_ASSET_KEY)
    return _build_operations_succeeded_result(metadata)


@asset_check(
    asset=OPERATIONS_CONTROL_ASSET_KEY,
    name="to_utc_gte_max_time_span",
)
def operations_control_to_utc_gte_max_time_span(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, OPERATIONS_CONTROL_ASSET_KEY)
    return _build_to_utc_gte_max_time_span_result(metadata)


@asset_check(
    asset=OPERATIONS_CONTROL_ASSET_KEY,
    name="original_last_loaded_at_lte_to_utc",
)
def operations_control_original_lte_to_utc(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, OPERATIONS_CONTROL_ASSET_KEY)
    return _build_original_lte_to_utc_result(metadata)


@asset_check(
    asset=FACTLECTURAS_ASSET_KEY,
    name="row_count_gt_0",
)
def factlecturas_row_count_gt_0(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTLECTURAS_ASSET_KEY)
    return _build_factlecturas_row_count_result(metadata)


@asset_check(
    asset=FACTLECTURAS_ASSET_KEY,
    name="original_last_loaded_at_lte_to_utc",
)
def factlecturas_original_lte_to_utc(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTLECTURAS_ASSET_KEY)
    return _build_factlecturas_original_lte_to_utc_result(metadata)


@asset_check(
    asset=FACTAUDIENCIA_ASSET_KEY,
    name="row_count_gt_0",
)
def factaudiencia_row_count_gt_0(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTAUDIENCIA_ASSET_KEY)
    return _build_factaudiencia_row_count_result(metadata)


@asset_check(
    asset=FACTAUDIENCIA_ASSET_KEY,
    name="original_last_loaded_at_lte_to_utc",
)
def factaudiencia_original_lte_to_utc(context) -> AssetCheckResult:
    metadata = _latest_materialization_metadata(context, FACTAUDIENCIA_ASSET_KEY)
    return _build_factaudiencia_original_lte_to_utc_result(metadata)


quality_asset_checks = [
    factreadings_real_readings_ratio_gt_0_6,
    factreadings_to_utc_gt_from_utc,
    operations_control_succeeded_is_true,
    operations_control_to_utc_gte_max_time_span,
    operations_control_original_lte_to_utc,
    factlecturas_row_count_gt_0,
    factlecturas_original_lte_to_utc,
    factaudiencia_row_count_gt_0,
    factaudiencia_original_lte_to_utc,
]
