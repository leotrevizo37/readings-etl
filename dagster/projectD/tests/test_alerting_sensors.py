import importlib
import re
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
REPOSITORY_ROOT = PROJECT_SRC.parents[2]
original_sys_path = list(sys.path)
sys.path[:] = [
    str(PROJECT_SRC),
    *[
        path
        for path in original_sys_path
        if Path(path or ".").resolve() != REPOSITORY_ROOT
    ],
]
original_dagster_module = sys.modules.pop("dagster", None)
try:
    from dagster import AssetKey, DefaultSensorStatus

    trend_module = importlib.import_module(
        "projectD.defs.sensors.asset_row_count_trend_sensor"
    )
    sensor_module = importlib.import_module(
        "projectD.defs.sensors.operational_alerts_sensor"
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


class _RecordingCursor:
    def __init__(self):
        self.statement = ""
        self.parameters = ()
        self.description = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, statement, *parameters):
        assert statement.count("?") == len(parameters)
        self.statement = statement
        self.parameters = parameters
        return self

    def fetchone(self):
        return (1,)


class _RecordingConnection:
    def __init__(self):
        self.cursor_value = _RecordingCursor()

    def cursor(self):
        return self.cursor_value


class _ConfigCursor:
    def __init__(self, mode):
        self.mode = mode
        self.call = 0
        self.description = []
        self.values = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, _statement, *_parameters):
        self.call += 1
        if self.call == 1:
            self.description = [
                ("alert_config_id",),
                ("version_number",),
                ("config_version",),
                ("sensor_lookback_hours",),
                ("dagster_failure_lookback_minutes",),
                ("dagster_failure_batch_size",),
                ("capture_interval_seconds",),
                ("metadata_key",),
                ("backfill_days",),
                ("batch_size",),
            ]
            self.values = (
                []
                if self.mode == "missing"
                else [
                    (
                        uuid.uuid4(),
                        1,
                        b"1",
                        1,
                        120,
                        200,
                        21600,
                        "row_count",
                        28,
                        500,
                    )
                ]
            )
        else:
            self.description = [("CodigoValidacion",)]
            self.values = [("REGLA_DEFAULT_FALTANTE",)]
        return self

    def fetchall(self):
        return self.values


class _ConfigConnection:
    def __init__(self, mode):
        self.cursor_value = _ConfigCursor(mode)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def cursor(self):
        return self.cursor_value


class _SqlResource:
    def __init__(self, mode):
        self.connection = _ConfigConnection(mode)

    def _get_connection(self):
        return self.connection


def _rule(alert_type="connection_lost"):
    return {
        "alert_rule_id": uuid.uuid4(),
        "alert_config_id": uuid.uuid4(),
        "alert_type_id": uuid.uuid4(),
        "alert_type": alert_type,
        "severity_id": uuid.uuid4(),
        "severity": "CRITICAL",
        "evaluation_interval_seconds": 30,
        "rule_version": b"1",
        "is_enabled": True,
        "no_readings_minutes": 20,
        "source_max_age_minutes": 60,
        "expected_interval_minutes": 200,
    }


def test_sensors_are_running_and_use_only_local_sqlserver():
    for sensor in (
        sensor_module.operational_alerts_sensor,
        trend_module.asset_row_count_trend_sensor,
    ):
        assert sensor.default_status == DefaultSensorStatus.RUNNING
        assert sensor.required_resource_keys == {"sqlserver"}


def test_exact_entity_override_wins_over_default():
    default = {**_rule(), "no_readings_minutes": 200}
    exact = {**_rule(), "no_readings_minutes": 20}
    config = {
        "rules": {
            ("connection_lost", None): default,
            ("connection_lost", "sensor-20"): exact,
        }
    }
    assert sensor_module._select_rule(
        config,
        "connection_lost",
        "sensor-20",
    )["no_readings_minutes"] == 20
    assert sensor_module._select_rule(
        config,
        "connection_lost",
        "sensor-200",
    )["no_readings_minutes"] == 200


def test_alert_codes_are_ascii_lowercase_kebab_case():
    code = sensor_module._alert_code(
        "connection_lost",
        "sensor-1",
        {
            "sensor_type": "Temperatúra",
            "sublocation_name": "Cámara fría",
        },
    )
    assert code == "sin-senal-temperatura-camara-fria"
    assert re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", code)


@pytest.mark.parametrize("mode", ["missing", "invalid"])
def test_missing_or_invalid_configuration_skips_without_signals(
    mode,
    monkeypatch,
):
    record = Mock(side_effect=AssertionError("unexpected signal"))
    monkeypatch.setattr(sensor_module, "_record_detected", record)
    context = SimpleNamespace(
        resources=SimpleNamespace(sqlserver=_SqlResource(mode)),
        cursor=None,
        update_cursor=Mock(),
        log=Mock(),
    )
    result = sensor_module.operational_alerts_sensor._raw_fn(context)
    assert result.skip_message
    record.assert_not_called()


def test_dagster_failures_require_origin_and_provable_assets():
    local_origin = SimpleNamespace(
        repository_origin=SimpleNamespace(
            code_location_origin=SimpleNamespace(
                location_name=sensor_module._USER_CODE
            )
        )
    )
    foreign_origin = SimpleNamespace(
        repository_origin=SimpleNamespace(
            code_location_origin=SimpleNamespace(
                location_name="another_location"
            )
        )
    )
    local_run = SimpleNamespace(
        run_id="local-run",
        job_name="local-job",
        remote_job_origin=local_origin,
        execution_plan_snapshot_id="local-snapshot",
    )
    foreign_run = SimpleNamespace(
        run_id="foreign-run",
        job_name="foreign-job",
        remote_job_origin=foreign_origin,
        execution_plan_snapshot_id="foreign-snapshot",
    )
    local_snapshot = SimpleNamespace(
        step_keys_to_execute=["multi_asset_step", "unmapped_step"],
        step_deps={"multi_asset_step": set(), "unmapped_step": set()},
        steps=[
            SimpleNamespace(
                key="multi_asset_step",
                outputs=[
                    SimpleNamespace(entity_key=AssetKey(["asset_one"])),
                    SimpleNamespace(entity_key=AssetKey(["asset_two"])),
                ],
            ),
            SimpleNamespace(
                key="unmapped_step",
                outputs=[SimpleNamespace(entity_key=None)],
            ),
        ]
    )
    foreign_snapshot = SimpleNamespace(
        steps=[
            SimpleNamespace(
                key="foreign_step",
                outputs=[
                    SimpleNamespace(entity_key=AssetKey(["foreign_asset"]))
                ],
            )
        ]
    )
    entries = {
        "local-run": SimpleNamespace(
            records=[
                SimpleNamespace(
                    event_log_entry=SimpleNamespace(
                        step_key="multi_asset_step",
                        timestamp=100.0,
                    )
                ),
                SimpleNamespace(
                    event_log_entry=SimpleNamespace(
                        step_key="multi_asset_step",
                        timestamp=101.0,
                    )
                ),
                SimpleNamespace(
                    event_log_entry=SimpleNamespace(
                        step_key="unmapped_step",
                        timestamp=102.0,
                    )
                ),
                SimpleNamespace(
                    event_log_entry=SimpleNamespace(
                        step_key="multi_asset_step",
                        timestamp=None,
                    )
                ),
            ]
        ),
        "foreign-run": SimpleNamespace(
            records=[
                SimpleNamespace(
                    event_log_entry=SimpleNamespace(
                        step_key="foreign_step",
                        timestamp=103.0,
                    )
                )
            ]
        ),
    }
    instance = SimpleNamespace(
        get_run_records=lambda **_: [
            SimpleNamespace(dagster_run=local_run),
            SimpleNamespace(dagster_run=foreign_run),
        ],
        get_execution_plan_snapshot=lambda snapshot_id: {
            "local-snapshot": local_snapshot,
            "foreign-snapshot": foreign_snapshot,
        }[snapshot_id],
        get_records_for_run=lambda run_id, of_type: (
            entries[run_id]
            if of_type == sensor_module.DagsterEventType.STEP_FAILURE
            else SimpleNamespace(records=[])
        ),
    )
    failures = sensor_module._failed_run_assets(
        SimpleNamespace(instance=instance),
        120,
        200,
        datetime.now(timezone.utc),
    )
    assert {
        (failure["run_id"], failure["asset_key"])
        for failure in failures
    } == {
        ("local-run", "asset_one"),
        ("local-run", "asset_two"),
    }
    assert {failure["occurred_at"].timestamp() for failure in failures} == {
        101.0
    }


def test_detected_and_cleared_events_keep_unique_guid_correlation():
    connection = _RecordingConnection()
    rule = _rule("dagster_run_failure")
    occurred_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert sensor_module._record_detected(
        connection,
        rule,
        "dagster",
        "run-id",
        "asset_key",
        occurred_at,
        "detalle",
        {},
        is_point_in_time=True,
    )
    parameters = connection.cursor_value.parameters
    alert_id = parameters[13]
    event_id = parameters[27]
    assert uuid.UUID(alert_id)
    assert uuid.UUID(event_id)
    assert alert_id != event_id
    assert parameters[28] == alert_id
    assert parameters[19] == "run-id"
    assert parameters[20] == "asset_key"
    assert "WITH (UPDLOCK, HOLDLOCK)" in connection.cursor_value.statement

    first_alert_id = alert_id
    assert sensor_module._record_detected(
        connection,
        rule,
        "dagster",
        "second-run-id",
        "asset_key",
        occurred_at,
        "detalle",
        {},
        is_point_in_time=True,
    )
    assert connection.cursor_value.parameters[13] != first_alert_id

    incident = {
        "alert_id": first_alert_id,
        "detected_at": occurred_at,
    }
    assert sensor_module._record_cleared(
        connection,
        incident,
        datetime(2026, 1, 3, tzinfo=timezone.utc),
        "recuperada",
        {},
    )
    parameters = connection.cursor_value.parameters
    assert uuid.UUID(parameters[6])
    assert parameters[7] == first_alert_id
    assert "detected_at < ?" in connection.cursor_value.statement


def test_row_count_capture_preserves_daily_history_contract():
    connection = _RecordingConnection()
    assert trend_module._record_observation(
        connection,
        date(2026, 1, 2),
        "asset_key",
        "row_count",
        42,
        10,
        "run-id",
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    statement = connection.cursor_value.statement
    assert "alerting.asset_row_count_daily" in statement
    assert "latest_event_storage_id < ?" in statement
    assert "average_7d" in statement
    assert "average_28d" in statement

