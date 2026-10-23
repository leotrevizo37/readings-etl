import json
import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pyodbc
from dagster import (
    AssetKey,
    AssetRecordsFilter,
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)


_USER_CODE = "projectD"
_SIGNAL_TYPE_IDS = {
    "DETECTED": "20000000-0000-0000-0000-000000000001",
    "CLEARED": "20000000-0000-0000-0000-000000000002",
}
_TRIGGER_SOURCE_IDS = {
    "sensor_monitoring": "40000000-0000-0000-0000-000000000001",
    "dagster_etl": "40000000-0000-0000-0000-000000000002",
    "dagster": "40000000-0000-0000-0000-000000000003",
}
_OPERATIONAL_TYPES = (
    "connection_lost",
    "late_readings",
    "pipeline_failure",
    "pipeline_stale",
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _version(value: Any) -> str:
    return bytes(value).hex() if value is not None else ""


def _rows(cursor) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _load_active_config(connection) -> tuple[dict[str, Any] | None, str | None]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                config.alert_config_id,
                config.version_number,
                config.version AS config_version,
                operational.sensor_lookback_hours,
                operational.dagster_failure_lookback_minutes,
                operational.dagster_failure_batch_size,
                trend.capture_interval_seconds,
                trend.metadata_key,
                trend.backfill_days,
                trend.batch_size
            FROM alerting.alert_config AS config
            INNER JOIN alerting.dim_config_status AS status
                ON status.config_status_id = config.config_status_id
            INNER JOIN alerting.alert_detector_operational_config AS operational
                ON operational.alert_config_id = config.alert_config_id
            INNER JOIN alerting.alert_detector_row_count_config AS trend
                ON trend.alert_config_id = config.alert_config_id
            WHERE
                config.user_code = ?
                AND status.code = N'ACTIVE'
            """,
            _USER_CODE,
        )
        rows = _rows(cursor)
        if len(rows) != 1:
            return None, "No existe una configuración activa única."
        config = rows[0]
        cursor.execute(
            """
            SELECT CodigoValidacion
            FROM serving.vwValidacionConfiguracionesAlerta
            WHERE
                ConfiguracionAlertaId = ?
                AND EsBloqueante = 1
            """,
            config["alert_config_id"],
        )
        problems = [row[0] for row in cursor.fetchall()]
        if problems:
            return None, "La configuración activa tiene errores bloqueantes."
        cursor.execute(
            """
            SELECT
                alert_rule.alert_rule_id,
                alert_rule.alert_config_id,
                alert_rule.alert_type_id,
                alert_type.code AS alert_type,
                alert_rule.entity_key,
                alert_rule.is_enabled,
                alert_rule.severity_id,
                severity.code AS severity,
                alert_rule.evaluation_interval_seconds,
                alert_rule.version AS rule_version,
                no_readings.no_readings_minutes,
                no_readings.source_max_age_minutes,
                pipeline_stale.expected_interval_minutes,
                trend.warning_drop_pct,
                trend.critical_drop_pct,
                trend.minimum_observed_days_7d,
                trend.minimum_observed_days_28d,
                trend.critical_severity_id
            FROM alerting.alert_rule AS alert_rule
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_type.alert_type_id = alert_rule.alert_type_id
            INNER JOIN alerting.dim_severity AS severity
                ON severity.severity_id = alert_rule.severity_id
            LEFT JOIN alerting.alert_rule_no_readings AS no_readings
                ON no_readings.alert_rule_id = alert_rule.alert_rule_id
            LEFT JOIN alerting.alert_rule_pipeline_stale AS pipeline_stale
                ON pipeline_stale.alert_rule_id = alert_rule.alert_rule_id
            LEFT JOIN alerting.alert_rule_row_count_trend AS trend
                ON trend.alert_rule_id = alert_rule.alert_rule_id
            WHERE alert_rule.alert_config_id = ?
            """,
            config["alert_config_id"],
        )
        rules = _rows(cursor)
    config["rules"] = {
        (row["alert_type"], row["entity_key"]): row
        for row in rules
    }
    config["version_token"] = _version(config["config_version"])
    return config, None


def _select_rule(
    config: dict[str, Any],
    alert_type: str,
    entity_key: str,
) -> dict[str, Any]:
    rules = config["rules"]
    return rules.get((alert_type, entity_key), rules[(alert_type, None)])


def _is_due(
    config: dict[str, Any],
    rule: dict[str, Any],
    entity_key: str,
    checks: dict[str, Any],
    now: datetime,
) -> bool:
    key = f"{rule['alert_type']}::{entity_key}"
    previous = checks.get(key, {})
    token = f"{config['version_token']}:{_version(rule['rule_version'])}"
    return (
        previous.get("version") != token
        or now.timestamp() - previous.get("at", 0)
        >= rule["evaluation_interval_seconds"]
    )


def _mark_checked(
    config: dict[str, Any],
    rule: dict[str, Any],
    entity_key: str,
    checks: dict[str, Any],
    now: datetime,
) -> None:
    checks[f"{rule['alert_type']}::{entity_key}"] = {
        "version": (
            f"{config['version_token']}:{_version(rule['rule_version'])}"
        ),
        "at": now.timestamp(),
    }


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").lower()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value).strip("-")
    return slug or "entidad"


def _alert_code(
    alert_type: str,
    entity_key: str,
    dimension: dict[str, Any] | None = None,
) -> str:
    if alert_type in {"connection_lost", "late_readings"}:
        dimension = dimension or {}
        sensor_type = dimension.get("sensor_type")
        sublocation = dimension.get("sublocation_name")
        if sensor_type is None and sublocation is None:
            suffix = _slug(entity_key)
        else:
            suffix = (
                f"{_slug(sensor_type or entity_key)}-"
                f"{_slug(sublocation or entity_key)}"
            )
        prefix = (
            "sin-senal"
            if alert_type == "connection_lost"
            else "lecturas-retrasadas"
        )
        return f"{prefix}-{suffix}"
    prefixes = {
        "pipeline_failure": "fallo-ingesta",
        "pipeline_stale": "ingesta-atrasada",
        "dagster_run_failure": "fallo-dagster",
        "asset_row_count_trend": "caida-volumen",
        "telemetry_device_configuration": "configuracion-dispositivo",
        "telemetry_alert_configuration": "configuracion-alerta",
    }
    return f"{prefixes[alert_type]}-{_slug(entity_key)}"


def _load_active_incidents(connection) -> dict[tuple[str, str], dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                incident.alert_id,
                alert_type.code AS alert_type,
                incident.entity_id,
                incident.entity_key,
                incident.detected_at,
                no_readings.no_readings_minutes,
                no_readings.source_max_age_minutes,
                pipeline_stale.expected_interval_minutes
            FROM alerting.alert_incident AS incident
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_type.alert_type_id = incident.alert_type_id
            LEFT JOIN alerting.alert_rule_no_readings AS no_readings
                ON no_readings.alert_rule_id = incident.alert_rule_id
            LEFT JOIN alerting.alert_rule_pipeline_stale AS pipeline_stale
                ON pipeline_stale.alert_rule_id = incident.alert_rule_id
            WHERE
                incident.user_code = ?
                AND incident.is_point_in_time = 0
                AND incident.cleared_at IS NULL
                AND alert_type.code IN (
                    N'connection_lost',
                    N'late_readings',
                    N'pipeline_failure',
                    N'pipeline_stale'
                )
            """,
            _USER_CODE,
        )
        return {
            (row["alert_type"], row["entity_key"]): row
            for row in _rows(cursor)
            if row["entity_key"] is not None
        }


def _record_detected(
    connection,
    rule: dict[str, Any],
    trigger_source: str,
    entity_id: str,
    entity_key: str,
    occurred_at: datetime,
    details: str,
    metadata: dict[str, Any],
    is_point_in_time: bool = False,
    severity_id: Any = None,
) -> bool:
    alert_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())
    is_occurrence = is_point_in_time or rule["alert_type"] == "dagster_run_failure"
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            DECLARE @inserted BIT = 0;

            IF (
                ? = 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                    WHERE
                        user_code = ?
                        AND alert_type_id = ?
                        AND entity_id = ?
                        AND entity_key = ?
                )
            )
            OR (
                ? = 0
                AND NOT EXISTS (
                    SELECT 1
                    FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                    WHERE
                        user_code = ?
                        AND alert_type_id = ?
                        AND entity_key = ?
                        AND is_point_in_time = 0
                        AND cleared_at IS NULL
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                    WHERE
                        user_code = ?
                        AND alert_type_id = ?
                        AND entity_key = ?
                        AND is_point_in_time = 0
                        AND detected_at = ?
                )
            )
                BEGIN
                    INSERT INTO alerting.alert_incident (
                        alert_id,
                        alert_code,
                        user_code,
                        alert_type_id,
                        severity_id,
                        trigger_source_id,
                        entity_id,
                        entity_key,
                        alert_config_id,
                        alert_rule_id,
                        detected_at,
                        details,
                        metadata_json,
                        is_point_in_time,
                        is_legacy
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0);

                    INSERT INTO alerting.alert_signal_event (
                        event_id,
                        alert_id,
                        signal_type_id,
                        occurred_at,
                        details,
                        metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?);

                    SET @inserted = 1;
                END;

            COMMIT TRANSACTION;
            SELECT @inserted;
            """,
            int(is_occurrence),
            _USER_CODE,
            rule["alert_type_id"],
            entity_id,
            entity_key,
            int(is_occurrence),
            _USER_CODE,
            rule["alert_type_id"],
            entity_key,
            _USER_CODE,
            rule["alert_type_id"],
            entity_key,
            occurred_at,
            alert_id,
            _alert_code(rule["alert_type"], entity_key, metadata.get("dimension")),
            _USER_CODE,
            rule["alert_type_id"],
            severity_id or rule["severity_id"],
            _TRIGGER_SOURCE_IDS[trigger_source],
            entity_id,
            entity_key,
            rule["alert_config_id"],
            rule["alert_rule_id"],
            occurred_at,
            details,
            json.dumps(metadata, ensure_ascii=False, default=str),
            int(is_point_in_time),
            event_id,
            alert_id,
            _SIGNAL_TYPE_IDS["DETECTED"],
            occurred_at,
            details,
            json.dumps(metadata, ensure_ascii=False, default=str),
        )
        return bool(cursor.fetchone()[0])


def _record_cleared(
    connection,
    incident: dict[str, Any],
    occurred_at: datetime,
    details: str,
    metadata: dict[str, Any],
) -> bool:
    event_id = str(uuid.uuid4())
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            DECLARE @cleared BIT = 0;

            IF EXISTS (
                SELECT 1
                FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                WHERE
                    alert_id = ?
                    AND cleared_at IS NULL
                    AND detected_at < ?
            )
                BEGIN
                    UPDATE alerting.alert_incident
                    SET cleared_at = ?
                    WHERE alert_id = ?;

                    IF NOT EXISTS (
                        SELECT 1
                        FROM alerting.alert_signal_event WITH (UPDLOCK, HOLDLOCK)
                        WHERE
                            alert_id = ?
                            AND signal_type_id = ?
                    )
                        INSERT INTO alerting.alert_signal_event (
                            event_id,
                            alert_id,
                            signal_type_id,
                            occurred_at,
                            details,
                            metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?);

                    SET @cleared = 1;
                END;

            COMMIT TRANSACTION;
            SELECT @cleared;
            """,
            incident["alert_id"],
            occurred_at,
            occurred_at,
            incident["alert_id"],
            incident["alert_id"],
            _SIGNAL_TYPE_IDS["CLEARED"],
            event_id,
            incident["alert_id"],
            _SIGNAL_TYPE_IDS["CLEARED"],
            occurred_at,
            details,
            json.dumps(metadata, ensure_ascii=False, default=str),
        )
        return bool(cursor.fetchone()[0])


def _sensor_statuses(connection, lookback_hours: int) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH UltimoEstado AS (
                SELECT
                    LOWER(CONVERT(VARCHAR(36), SensorId)) AS sensor_id,
                    TimeSpan AS observed_at,
                    LastReadingAt AS last_reading_at,
                    MinutesWithoutReadings AS minutes_without_readings,
                    ModifiedAt AS modified_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY SensorId
                        ORDER BY TimeSpan DESC
                    ) AS row_number
                FROM observability.factRedingsAudits
                WHERE TimeSpan >= DATEADD(HOUR, ?, SYSUTCDATETIME())
            )
            SELECT
                sensor_id,
                observed_at,
                last_reading_at,
                minutes_without_readings,
                modified_at
            FROM UltimoEstado
            WHERE row_number = 1
            """,
            -lookback_hours,
        )
        return _rows(cursor)


def _sensor_dimensions(
    connection,
    sensor_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not sensor_ids:
        return {}
    placeholders = ",".join("?" for _ in sensor_ids)
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                LOWER(CONVERT(VARCHAR(36), SensorId)) AS sensor_id,
                location_name,
                sublocation_name,
                sublocation_type,
                device_name,
                device_type,
                sensor_name,
                sensor_type
            FROM dwh.dimTelemetryProdDimensions
            WHERE LOWER(CONVERT(VARCHAR(36), SensorId)) IN ({placeholders})
            """,
            sensor_ids,
        )
        return {row["sensor_id"]: row for row in _rows(cursor)}


def _ingestions(connection) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                IngestionName AS ingestion_name,
                LastRunStatus AS last_run_status,
                LastRunStartedAt AS last_run_started_at,
                LastRunFinishedAt AS last_run_finished_at,
                LastRunError AS last_run_error
            FROM ctl.IngestionControl
            WHERE IsActive = 1
            """
        )
        return _rows(cursor)


def _code_location(run: Any) -> str | None:
    origin = getattr(run, "remote_job_origin", None)
    if origin is None:
        return None
    repository_origin = getattr(origin, "repository_origin", None)
    code_location_origin = getattr(
        repository_origin,
        "code_location_origin",
        None,
    )
    return getattr(code_location_origin, "location_name", None)


def _failed_run_assets(
    context: SensorEvaluationContext,
    lookback_minutes: int,
    batch_size: int,
    now: datetime,
) -> list[dict[str, Any]]:
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.FAILURE],
            updated_after=now - timedelta(minutes=lookback_minutes),
        ),
        limit=batch_size,
    )
    failures: dict[tuple[str, str], dict[str, Any]] = {}
    for run_record in run_records:
        run = run_record.dagster_run
        if _code_location(run) != _USER_CODE:
            continue
        snapshot_id = getattr(run, "execution_plan_snapshot_id", None)
        if snapshot_id is None:
            continue
        snapshot = context.instance.get_execution_plan_snapshot(snapshot_id)
        if snapshot is None:
            continue
        step_keys = set(snapshot.step_keys_to_execute)
        steps = {
            step.key: step for step in snapshot.steps if step.key in step_keys
        }
        downstream = {step_key: set() for step_key in steps}
        for step_key, upstream_steps in snapshot.step_deps.items():
            if step_key not in steps:
                continue
            for upstream_step in upstream_steps:
                if upstream_step in downstream:
                    downstream[upstream_step].add(step_key)
        materialized = {
            record.asset_key
            for record in context.instance.get_records_for_run(
                run.run_id,
                of_type=DagsterEventType.ASSET_MATERIALIZATION,
            ).records
        }
        records = context.instance.get_records_for_run(
            run.run_id,
            of_type=DagsterEventType.STEP_FAILURE,
        ).records
        for record in records:
            entry = record.event_log_entry
            step = steps.get(entry.step_key)
            if step is None or entry.timestamp is None:
                continue
            occurred_at = datetime.fromtimestamp(entry.timestamp, timezone.utc)
            pending = [step.key]
            visited = set()
            while pending:
                step_key = pending.pop()
                if step_key in visited:
                    continue
                visited.add(step_key)
                pending.extend(sorted(downstream[step_key] - visited))
                for output in steps[step_key].outputs:
                    entity_key = output.entity_key
                    if (
                        not isinstance(entity_key, AssetKey)
                        or entity_key in materialized
                    ):
                        continue
                    asset_key = entity_key.to_user_string()
                    failures[(run.run_id, asset_key)] = {
                        "run_id": run.run_id,
                        "job_name": run.job_name,
                        "step_key": entry.step_key,
                        "asset_key": asset_key,
                        "occurred_at": occurred_at,
                    }
    return list(failures.values())


def _clear_dagster_failures(
    context: SensorEvaluationContext,
    connection,
    config: dict[str, Any],
    checks: dict[str, Any],
    now: datetime,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                incident.alert_id,
                incident.entity_id,
                incident.entity_key,
                incident.detected_at
            FROM alerting.alert_incident AS incident
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_type.alert_type_id = incident.alert_type_id
            WHERE
                incident.user_code = ?
                AND alert_type.code = N'dagster_run_failure'
                AND incident.cleared_at IS NULL
                AND incident.entity_key IS NOT NULL
            """,
            _USER_CODE,
        )
        incidents = _rows(cursor)
    cleared = 0
    for incident in incidents:
        entity_key = incident["entity_key"]
        rule = _select_rule(config, "dagster_run_failure", entity_key)
        recovery_key = f"recovery::{incident['alert_id']}"
        if not rule["is_enabled"] or not _is_due(
            config, rule, recovery_key, checks, now
        ):
            continue
        detected_at = _utc(incident["detected_at"])
        records_filter = AssetRecordsFilter(
            asset_key=AssetKey.from_user_string(entity_key),
            after_timestamp=detected_at.timestamp(),
            before_timestamp=now.timestamp(),
        )
        cursor = None
        recovered = False
        while not recovered:
            page = context.instance.fetch_materializations(
                records_filter,
                limit=config["dagster_failure_batch_size"],
                cursor=cursor,
                ascending=True,
            )
            for record in page.records:
                occurred_at = datetime.fromtimestamp(record.timestamp, timezone.utc)
                if (
                    record.run_id == incident["entity_id"]
                    or occurred_at <= detected_at
                    or occurred_at > now
                ):
                    continue
                run = context.instance.get_run_by_id(record.run_id)
                if _code_location(run) != _USER_CODE:
                    continue
                metadata = {
                    "evidence": {
                        "failed_run_id": incident["entity_id"],
                        "recovery_run_id": record.run_id,
                        "asset_key": entity_key,
                        "materialization_storage_id": record.storage_id,
                    }
                }
                if _record_cleared(
                    connection,
                    incident,
                    occurred_at,
                    (
                        "Una materialización exitosa posterior confirmó "
                        f"la recuperación de {entity_key}."
                    ),
                    metadata,
                ):
                    cleared += 1
                recovered = True
                break
            if not page.has_more:
                break
            cursor = page.cursor
        _mark_checked(config, rule, recovery_key, checks, now)
    return cleared


@sensor(
    name="operational_alerts_sensor",
    minimum_interval_seconds=30,
    required_resource_keys={"sqlserver"},
    default_status=DefaultSensorStatus.RUNNING,
)
def operational_alerts_sensor(
    context: SensorEvaluationContext,
) -> SkipReason:
    now = datetime.now(timezone.utc)
    try:
        cursor_state = json.loads(context.cursor or "{}")
    except (json.JSONDecodeError, TypeError):
        cursor_state = {}
    checks = dict(cursor_state.get("checks", {}))
    try:
        with context.resources.sqlserver._get_connection() as connection:
            config, reason = _load_active_config(connection)
            if config is None:
                return SkipReason(reason)
            incidents = _load_active_incidents(connection)
            detected = 0
            cleared = 0
            statuses = _sensor_statuses(
                connection,
                config["sensor_lookback_hours"],
            )
            dimensions = _sensor_dimensions(
                connection,
                [status["sensor_id"] for status in statuses],
            )
            for status in statuses:
                entity_key = status["sensor_id"]
                for alert_type in ("connection_lost", "late_readings"):
                    rule = _select_rule(config, alert_type, entity_key)
                    if not rule["is_enabled"] or not _is_due(
                        config,
                        rule,
                        entity_key,
                        checks,
                        now,
                    ):
                        continue
                    _mark_checked(config, rule, entity_key, checks, now)
                    incident = incidents.get((alert_type, entity_key))
                    threshold = (
                        incident["no_readings_minutes"]
                        if incident is not None
                        and incident["no_readings_minutes"] is not None
                        else rule["no_readings_minutes"]
                    )
                    source_age = (
                        incident["source_max_age_minutes"]
                        if incident is not None
                        and incident["source_max_age_minutes"] is not None
                        else rule["source_max_age_minutes"]
                    )
                    values = (
                        status["observed_at"],
                        status["last_reading_at"],
                        status["minutes_without_readings"],
                        status["modified_at"],
                    )
                    if any(value is None for value in values):
                        continue
                    observed_at = _utc(status["observed_at"])
                    last_reading_at = _utc(status["last_reading_at"])
                    modified_at = _utc(status["modified_at"])
                    if (
                        observed_at > now
                        or last_reading_at > now
                        or modified_at > now
                        or now - modified_at
                        > timedelta(minutes=source_age)
                    ):
                        continue
                    metadata = {
                        "dimension": dimensions.get(entity_key),
                        "measurements": {
                            "observed_at": observed_at.isoformat(),
                            "last_reading_at": last_reading_at.isoformat(),
                            "minutes_without_readings": (
                                status["minutes_without_readings"]
                            ),
                            "source_modified_at": modified_at.isoformat(),
                        },
                        "thresholds": {
                            "no_readings_minutes": threshold,
                            "source_max_age_minutes": source_age,
                        },
                    }
                    if status["minutes_without_readings"] >= threshold:
                        if incident is None:
                            occurred_at = last_reading_at + timedelta(
                                minutes=threshold
                            )
                            if occurred_at <= now and _record_detected(
                                connection,
                                rule,
                                "sensor_monitoring",
                                entity_key,
                                entity_key,
                                occurred_at,
                                (
                                    f"El sensor excedió {threshold} minutos "
                                    "sin lecturas."
                                ),
                                metadata,
                            ):
                                detected += 1
                        continue
                    if (
                        incident is not None
                        and last_reading_at > _utc(incident["detected_at"])
                        and _record_cleared(
                            connection,
                            incident,
                            last_reading_at,
                            "Una lectura posterior confirmó la recuperación.",
                            metadata,
                        )
                    ):
                        cleared += 1

            for ingestion in _ingestions(connection):
                entity_key = ingestion["ingestion_name"]
                status_code = (ingestion["last_run_status"] or "").upper()
                finished = ingestion["last_run_finished_at"]
                for alert_type in ("pipeline_failure", "pipeline_stale"):
                    rule = _select_rule(config, alert_type, entity_key)
                    if not rule["is_enabled"] or not _is_due(
                        config,
                        rule,
                        entity_key,
                        checks,
                        now,
                    ):
                        continue
                    _mark_checked(config, rule, entity_key, checks, now)
                    incident = incidents.get((alert_type, entity_key))
                    metadata = {
                        "evidence": {
                            "last_run_status": status_code,
                            "last_run_started_at": (
                                _utc(ingestion["last_run_started_at"]).isoformat()
                                if ingestion["last_run_started_at"] is not None
                                else None
                            ),
                            "last_run_finished_at": (
                                _utc(finished).isoformat()
                                if finished is not None
                                else None
                            ),
                            "last_run_error": ingestion["last_run_error"],
                        }
                    }
                    if alert_type == "pipeline_failure":
                        if (
                            incident is not None
                            and status_code == "SUCCESS"
                            and finished is not None
                            and _utc(finished) > _utc(incident["detected_at"])
                            and _record_cleared(
                                connection,
                                incident,
                                _utc(finished),
                                (
                                    "Una ejecución exitosa posterior confirmó "
                                    "la recuperación."
                                ),
                                metadata,
                            )
                        ):
                            cleared += 1
                        elif (
                            incident is None
                            and status_code == "FAILED"
                            and finished is not None
                            and _utc(finished) <= now
                            and _record_detected(
                                connection,
                                rule,
                                "dagster_etl",
                                entity_key,
                                entity_key,
                                _utc(finished),
                                "La última ejecución de la ingesta falló.",
                                metadata,
                            )
                        ):
                            detected += 1
                        continue

                    if status_code != "SUCCESS" or finished is None:
                        continue
                    finished_at = _utc(finished)
                    if (
                        incident is not None
                        and finished_at > _utc(incident["detected_at"])
                    ):
                        if _record_cleared(
                            connection,
                            incident,
                            finished_at,
                            (
                                "Una ejecución exitosa posterior confirmó "
                                "la recuperación."
                            ),
                            metadata,
                        ):
                            cleared += 1
                        incident = None
                    if incident is None:
                        current_interval = rule["expected_interval_minutes"]
                        occurred_at = finished_at + timedelta(
                            minutes=current_interval
                        )
                        if occurred_at <= now:
                            metadata["thresholds"] = {
                                "expected_interval_minutes": current_interval
                            }
                            if _record_detected(
                                connection,
                                rule,
                                "dagster_etl",
                                entity_key,
                                entity_key,
                                occurred_at,
                                (
                                    "La ingesta excedió su intervalo esperado "
                                    "sin una nueva ejecución exitosa."
                                ),
                                metadata,
                            ):
                                detected += 1

            if any(
                rule["alert_type"] == "dagster_run_failure"
                and rule["is_enabled"]
                for rule in config["rules"].values()
            ):
                for failure in _failed_run_assets(
                    context,
                    config["dagster_failure_lookback_minutes"],
                    config["dagster_failure_batch_size"],
                    now,
                ):
                    entity_key = failure["asset_key"]
                    failure_key = f"{failure['run_id']}::{entity_key}"
                    rule = _select_rule(
                        config,
                        "dagster_run_failure",
                        entity_key,
                    )
                    if not rule["is_enabled"] or not _is_due(
                        config,
                        rule,
                        failure_key,
                        checks,
                        now,
                    ):
                        continue
                    _mark_checked(config, rule, failure_key, checks, now)
                    if failure["occurred_at"] > now:
                        continue
                    metadata = {
                        "evidence": {
                            "run_id": failure["run_id"],
                            "job_name": failure["job_name"],
                            "step_key": failure["step_key"],
                            "asset_key": entity_key,
                        }
                    }
                    if _record_detected(
                        connection,
                        rule,
                        "dagster",
                        failure["run_id"],
                        entity_key,
                        failure["occurred_at"],
                        (
                            f"El fallo del paso {failure['step_key']} impidió "
                            f"materializar {entity_key}."
                        ),
                        metadata,
                    ):
                        detected += 1
                cleared += _clear_dagster_failures(
                    context, connection, config, checks, now
                )
    except pyodbc.Error:
        return SkipReason("No fue posible consultar la configuración local.")

    context.update_cursor(json.dumps({"checks": checks}))
    summary = (
        f"Señales detectadas: {detected}; señales finalizadas: {cleared}."
    )
    context.log.info(summary)
    return SkipReason(summary)
