import json
from datetime import datetime, time, timedelta, timezone
from typing import Any

import pyodbc
from dagster import (
    DagsterEventType,
    DefaultSensorStatus,
    EventRecordsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from .operational_alerts_sensor import (
    _USER_CODE,
    _code_location,
    _is_due,
    _load_active_config,
    _mark_checked,
    _record_cleared,
    _record_detected,
    _select_rule,
    _utc,
)


_ALERT_TYPE = "asset_row_count_trend"


def _coerce_row_count(value: Any) -> int | None:
    raw_value = getattr(value, "value", value)
    if raw_value is None or isinstance(raw_value, bool):
        return None
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return None
    if parsed < 0 or (
        isinstance(raw_value, float) and not raw_value.is_integer()
    ):
        return None
    return parsed


def _record_observation(
    connection,
    observation_date,
    asset_key: str,
    metadata_key: str,
    row_count: int,
    event_storage_id: int,
    run_id: str | None,
    materialized_at: datetime,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            DECLARE @recorded BIT = 0;

            UPDATE alerting.asset_row_count_daily WITH (
                UPDLOCK,
                SERIALIZABLE
            )
            SET
                daily_materialization_count =
                    daily_materialization_count + 1,
                daily_row_count_sum = daily_row_count_sum + ?,
                daily_row_count_average = ROUND(
                    CAST(daily_row_count_sum + ? AS NUMERIC(38, 4))
                    / (daily_materialization_count + 1),
                    4
                ),
                daily_row_count_min = CASE
                    WHEN daily_row_count_min > ? THEN ?
                    ELSE daily_row_count_min
                END,
                daily_row_count_max = CASE
                    WHEN daily_row_count_max < ? THEN ?
                    ELSE daily_row_count_max
                END,
                last_result_count = ?,
                latest_event_storage_id = ?,
                latest_run_id = ?,
                latest_materialized_at = ?,
                updated_at = SYSUTCDATETIME()
            WHERE
                observation_date = ?
                AND code_location = ?
                AND asset_key = ?
                AND metadata_key = ?
                AND latest_event_storage_id < ?;

            IF @@ROWCOUNT > 0
                SET @recorded = 1;
            ELSE IF NOT EXISTS (
                SELECT 1
                FROM alerting.asset_row_count_daily
                WHERE
                    observation_date = ?
                    AND code_location = ?
                    AND asset_key = ?
                    AND metadata_key = ?
            )
                BEGIN
                    INSERT INTO alerting.asset_row_count_daily (
                        observation_date,
                        code_location,
                        asset_key,
                        metadata_key,
                        daily_materialization_count,
                        daily_row_count_sum,
                        daily_row_count_average,
                        daily_row_count_min,
                        daily_row_count_max,
                        first_result_count,
                        last_result_count,
                        latest_event_storage_id,
                        latest_run_id,
                        latest_materialized_at
                    )
                    VALUES (
                        ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    );
                    SET @recorded = 1;
                END;

            IF @recorded = 1
                BEGIN
                    UPDATE target
                    SET
                        average_1d = (
                            SELECT AVG(CAST(
                                history.daily_row_count_sum
                                AS NUMERIC(38, 4)
                            ))
                            FROM alerting.asset_row_count_daily AS history
                            WHERE
                                history.code_location = target.code_location
                                AND history.asset_key = target.asset_key
                                AND history.metadata_key = target.metadata_key
                                AND history.observation_date = DATEADD(
                                    DAY,
                                    -1,
                                    target.observation_date
                                )
                        ),
                        average_7d = (
                            SELECT AVG(CAST(
                                history.daily_row_count_sum
                                AS NUMERIC(38, 4)
                            ))
                            FROM alerting.asset_row_count_daily AS history
                            WHERE
                                history.code_location = target.code_location
                                AND history.asset_key = target.asset_key
                                AND history.metadata_key = target.metadata_key
                                AND history.observation_date >= DATEADD(
                                    DAY,
                                    -7,
                                    target.observation_date
                                )
                                AND history.observation_date
                                    < target.observation_date
                        ),
                        average_28d = (
                            SELECT AVG(CAST(
                                history.daily_row_count_sum
                                AS NUMERIC(38, 4)
                            ))
                            FROM alerting.asset_row_count_daily AS history
                            WHERE
                                history.code_location = target.code_location
                                AND history.asset_key = target.asset_key
                                AND history.metadata_key = target.metadata_key
                                AND history.observation_date >= DATEADD(
                                    DAY,
                                    -28,
                                    target.observation_date
                                )
                                AND history.observation_date
                                    < target.observation_date
                        ),
                        observed_days_7d = (
                            SELECT COUNT(*)
                            FROM alerting.asset_row_count_daily AS history
                            WHERE
                                history.code_location = target.code_location
                                AND history.asset_key = target.asset_key
                                AND history.metadata_key = target.metadata_key
                                AND history.observation_date >= DATEADD(
                                    DAY,
                                    -7,
                                    target.observation_date
                                )
                                AND history.observation_date
                                    < target.observation_date
                        ),
                        observed_days_28d = (
                            SELECT COUNT(*)
                            FROM alerting.asset_row_count_daily AS history
                            WHERE
                                history.code_location = target.code_location
                                AND history.asset_key = target.asset_key
                                AND history.metadata_key = target.metadata_key
                                AND history.observation_date >= DATEADD(
                                    DAY,
                                    -28,
                                    target.observation_date
                                )
                                AND history.observation_date
                                    < target.observation_date
                        ),
                        updated_at = SYSUTCDATETIME()
                    FROM alerting.asset_row_count_daily AS target
                    WHERE
                        target.code_location = ?
                        AND target.asset_key = ?
                        AND target.metadata_key = ?
                        AND target.observation_date >= ?;

                    UPDATE alerting.asset_row_count_daily
                    SET
                        change_vs_1d_pct = CASE
                            WHEN average_1d IS NULL OR average_1d = 0
                                THEN NULL
                            ELSE ROUND(
                                (
                                    CAST(
                                        daily_row_count_sum
                                        AS NUMERIC(38, 4)
                                    ) - average_1d
                                ) / average_1d * 100,
                                4
                            )
                        END,
                        change_vs_7d_pct = CASE
                            WHEN average_7d IS NULL OR average_7d = 0
                                THEN NULL
                            ELSE ROUND(
                                (
                                    CAST(
                                        daily_row_count_sum
                                        AS NUMERIC(38, 4)
                                    ) - average_7d
                                ) / average_7d * 100,
                                4
                            )
                        END,
                        change_vs_28d_pct = CASE
                            WHEN average_28d IS NULL OR average_28d = 0
                                THEN NULL
                            ELSE ROUND(
                                (
                                    CAST(
                                        daily_row_count_sum
                                        AS NUMERIC(38, 4)
                                    ) - average_28d
                                ) / average_28d * 100,
                                4
                            )
                        END,
                        updated_at = SYSUTCDATETIME()
                    WHERE
                        code_location = ?
                        AND asset_key = ?
                        AND metadata_key = ?
                        AND observation_date >= ?;
                END;

            COMMIT TRANSACTION;
            SELECT @recorded;
            """,
            row_count,
            row_count,
            row_count,
            row_count,
            row_count,
            row_count,
            row_count,
            event_storage_id,
            run_id,
            materialized_at,
            observation_date,
            _USER_CODE,
            asset_key,
            metadata_key,
            event_storage_id,
            observation_date,
            _USER_CODE,
            asset_key,
            metadata_key,
            observation_date,
            _USER_CODE,
            asset_key,
            metadata_key,
            row_count,
            row_count,
            row_count,
            row_count,
            row_count,
            row_count,
            event_storage_id,
            run_id,
            materialized_at,
            _USER_CODE,
            asset_key,
            metadata_key,
            observation_date,
            _USER_CODE,
            asset_key,
            metadata_key,
            observation_date,
        )
        return bool(cursor.fetchone()[0])


def _latest_trends(connection, metadata_key: str) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH UltimaObservacion AS (
                SELECT
                    observation_date,
                    asset_key,
                    metadata_key,
                    daily_materialization_count,
                    daily_row_count_sum,
                    average_1d,
                    average_7d,
                    average_28d,
                    observed_days_7d,
                    observed_days_28d,
                    change_vs_1d_pct,
                    change_vs_7d_pct,
                    change_vs_28d_pct,
                    latest_run_id,
                    latest_materialized_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY asset_key, metadata_key
                        ORDER BY observation_date DESC
                    ) AS row_number
                FROM alerting.asset_row_count_daily
                WHERE
                    code_location = ?
                    AND metadata_key = ?
                    AND observation_date
                        < CONVERT(DATE, SYSUTCDATETIME())
            )
            SELECT
                observation_date,
                asset_key,
                metadata_key,
                daily_materialization_count,
                daily_row_count_sum,
                average_1d,
                average_7d,
                average_28d,
                observed_days_7d,
                observed_days_28d,
                change_vs_1d_pct,
                change_vs_7d_pct,
                change_vs_28d_pct,
                latest_run_id,
                latest_materialized_at
            FROM UltimaObservacion
            WHERE row_number = 1
            """,
            _USER_CODE,
            metadata_key,
        )
        return _rows(cursor)


def _rows(cursor) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _active_trend_incidents(
    connection,
) -> dict[str, dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                incident.alert_id,
                incident.entity_key,
                incident.detected_at,
                trend.warning_drop_pct,
                trend.critical_drop_pct,
                trend.minimum_observed_days_7d,
                trend.minimum_observed_days_28d,
                trend.critical_severity_id
            FROM alerting.alert_incident AS incident
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_type.alert_type_id = incident.alert_type_id
            LEFT JOIN alerting.alert_rule_row_count_trend AS trend
                ON trend.alert_rule_id = incident.alert_rule_id
            WHERE
                incident.user_code = ?
                AND alert_type.code = N'asset_row_count_trend'
                AND incident.is_point_in_time = 0
                AND incident.cleared_at IS NULL
            """,
            _USER_CODE,
        )
        return {
            row["entity_key"]: row
            for row in _rows(cursor)
            if row["entity_key"] is not None
        }


@sensor(
    name="asset_row_count_trend_sensor",
    minimum_interval_seconds=21600,
    required_resource_keys={"sqlserver"},
    default_status=DefaultSensorStatus.RUNNING,
)
def asset_row_count_trend_sensor(
    context: SensorEvaluationContext,
) -> SkipReason:
    now = datetime.now(timezone.utc)
    try:
        cursor_state = json.loads(context.cursor or "{}")
    except (json.JSONDecodeError, TypeError):
        cursor_state = {}
    checks = dict(cursor_state.get("checks", {}))
    storage_id = cursor_state.get("storage_id")
    capture = dict(cursor_state.get("capture", {}))
    try:
        with context.resources.sqlserver._get_connection() as connection:
            config, reason = _load_active_config(connection)
            if config is None:
                return SkipReason(reason)
            capture_due = (
                capture.get("pending")
                or capture.get("version") != config["version_token"]
                or now.timestamp() - capture.get("at", 0)
                >= config["capture_interval_seconds"]
            )
            scanned = 0
            recorded = 0
            pending = False
            if capture_due:
                records_page = (
                    context.instance.event_log_storage.get_event_records(
                        EventRecordsFilter(
                            event_type=(
                                DagsterEventType.ASSET_MATERIALIZATION
                            ),
                            after_cursor=storage_id,
                            after_timestamp=(
                                None
                                if storage_id is not None
                                else datetime.combine(
                                    now.date()
                                    - timedelta(days=config["backfill_days"]),
                                    time.min,
                                    tzinfo=timezone.utc,
                                ).timestamp()
                            ),
                        ),
                        limit=config["batch_size"] + 1,
                        ascending=True,
                    )
                )
                pending = len(records_page) > config["batch_size"]
                records = records_page[: config["batch_size"]]
                scanned = len(records)
                runs: dict[str, Any] = {}
                for record in records:
                    materialization = record.asset_materialization
                    asset_key = record.asset_key
                    if (
                        materialization is None
                        or asset_key is None
                        or record.run_id is None
                        or record.timestamp is None
                    ):
                        continue
                    row_count = _coerce_row_count(
                        materialization.metadata.get(config["metadata_key"])
                    )
                    if row_count is None:
                        continue
                    if record.run_id not in runs:
                        runs[record.run_id] = context.instance.get_run_by_id(
                            record.run_id
                        )
                    if _code_location(runs[record.run_id]) != _USER_CODE:
                        continue
                    materialized_at = datetime.fromtimestamp(
                        record.timestamp,
                        timezone.utc,
                    )
                    if _record_observation(
                        connection,
                        materialized_at.date(),
                        asset_key.to_user_string(),
                        config["metadata_key"],
                        row_count,
                        record.storage_id,
                        record.run_id,
                        materialized_at,
                    ):
                        recorded += 1
                if records:
                    storage_id = records[-1].storage_id
                capture = {
                    "at": now.timestamp(),
                    "version": config["version_token"],
                    "pending": pending,
                }

            detected = 0
            cleared = 0
            if not pending:
                incidents = _active_trend_incidents(connection)
                for trend in _latest_trends(
                    connection,
                    config["metadata_key"],
                ):
                    entity_key = trend["asset_key"]
                    rule = _select_rule(config, _ALERT_TYPE, entity_key)
                    if not rule["is_enabled"] or not _is_due(
                        config,
                        rule,
                        entity_key,
                        checks,
                        now,
                    ):
                        continue
                    _mark_checked(config, rule, entity_key, checks, now)
                    incident = incidents.get(entity_key)
                    warning_drop = float(
                        incident["warning_drop_pct"]
                        if incident is not None
                        and incident["warning_drop_pct"] is not None
                        else rule["warning_drop_pct"]
                    )
                    critical_drop = float(
                        incident["critical_drop_pct"]
                        if incident is not None
                        and incident["critical_drop_pct"] is not None
                        else rule["critical_drop_pct"]
                    )
                    minimum_7d = int(
                        incident["minimum_observed_days_7d"]
                        if incident is not None
                        and incident["minimum_observed_days_7d"] is not None
                        else rule["minimum_observed_days_7d"]
                    )
                    minimum_28d = int(
                        incident["minimum_observed_days_28d"]
                        if incident is not None
                        and incident["minimum_observed_days_28d"] is not None
                        else rule["minimum_observed_days_28d"]
                    )
                    required = (
                        trend["average_7d"],
                        trend["average_28d"],
                        trend["change_vs_7d_pct"],
                        trend["change_vs_28d_pct"],
                        trend["latest_materialized_at"],
                    )
                    if (
                        any(value is None for value in required)
                        or trend["observed_days_7d"] < minimum_7d
                        or trend["observed_days_28d"] < minimum_28d
                    ):
                        continue
                    change_7d = float(trend["change_vs_7d_pct"])
                    change_28d = float(trend["change_vs_28d_pct"])
                    materialized_at = _utc(
                        trend["latest_materialized_at"]
                    )
                    metadata = {
                        "measurements": {
                            "observation_date": (
                                trend["observation_date"].isoformat()
                            ),
                            "daily_row_count": trend["daily_row_count_sum"],
                            "average_1d": (
                                float(trend["average_1d"])
                                if trend["average_1d"] is not None
                                else None
                            ),
                            "average_7d": float(trend["average_7d"]),
                            "average_28d": float(trend["average_28d"]),
                            "change_vs_1d_pct": (
                                float(trend["change_vs_1d_pct"])
                                if trend["change_vs_1d_pct"] is not None
                                else None
                            ),
                            "change_vs_7d_pct": change_7d,
                            "change_vs_28d_pct": change_28d,
                            "materialization_count": (
                                trend["daily_materialization_count"]
                            ),
                        },
                        "thresholds": {
                            "warning_drop_pct": warning_drop,
                            "critical_drop_pct": critical_drop,
                            "minimum_observed_days_7d": minimum_7d,
                            "minimum_observed_days_28d": minimum_28d,
                        },
                        "evidence": {
                            "latest_run_id": trend["latest_run_id"],
                            "latest_materialized_at": (
                                materialized_at.isoformat()
                            ),
                        },
                    }
                    is_low = (
                        change_7d <= -warning_drop
                        and change_28d <= -warning_drop
                    )
                    if is_low:
                        if incident is None and _record_detected(
                            connection,
                            rule,
                            "dagster",
                            entity_key,
                            entity_key,
                            materialized_at,
                            (
                                "El volumen diario cayó por debajo de los "
                                "umbrales de 7 y 28 días."
                            ),
                            metadata,
                            severity_id=(
                                rule["critical_severity_id"]
                                if change_7d <= -critical_drop
                                and change_28d <= -critical_drop
                                else rule["severity_id"]
                            ),
                        ):
                            detected += 1
                        continue
                    if (
                        incident is not None
                        and materialized_at
                        > _utc(incident["detected_at"])
                        and _record_cleared(
                            connection,
                            incident,
                            materialized_at,
                            (
                                "Una observación posterior confirmó un "
                                "volumen saludable."
                            ),
                            metadata,
                        )
                    ):
                        cleared += 1
    except pyodbc.Error:
        return SkipReason("No fue posible consultar la configuración local.")

    context.update_cursor(
        json.dumps(
            {
                "checks": checks,
                "storage_id": storage_id,
                "capture": capture,
            }
        )
    )
    summary = (
        f"Materializaciones revisadas: {scanned}; observaciones registradas: "
        f"{recorded}; señales detectadas: {detected}; "
        f"señales finalizadas: {cleared}."
    )
    context.log.info(summary)
    return SkipReason(summary)
