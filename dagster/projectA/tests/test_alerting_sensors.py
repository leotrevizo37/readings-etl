import importlib
import re
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

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
    from dagster import (
        AssetKey,
        AssetOut,
        DagsterInstance,
        DefaultSensorStatus,
        Failure,
        Out,
        Output,
        asset,
        graph_multi_asset,
        materialize,
        op,
    )

    trend_module = importlib.import_module(
        "projectA.defs.sensors.asset_row_count_trend_sensor"
    )
    sensor_module = importlib.import_module(
        "projectA.defs.sensors.operational_alerts_sensor"
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


def test_rule_interval_and_version_control_evaluation():
    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    config = {"version_token": "config-1"}
    rule = _rule()
    checks = {}
    assert sensor_module._is_due(config, rule, "sensor", checks, now)
    sensor_module._mark_checked(config, rule, "sensor", checks, now)
    assert not sensor_module._is_due(
        config,
        rule,
        "sensor",
        checks,
        now + timedelta(seconds=29),
    )
    assert sensor_module._is_due(
        config,
        rule,
        "sensor",
        checks,
        now + timedelta(seconds=30),
    )
    assert sensor_module._is_due(
        {"version_token": "config-2"},
        rule,
        "sensor",
        checks,
        now + timedelta(seconds=1),
    )
    assert sensor_module._is_due(
        config,
        {**rule, "rule_version": b"2"},
        "sensor",
        checks,
        now + timedelta(seconds=1),
    )


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
    assert sensor_module._alert_code(
        "dagster_run_failure",
        "!!!",
    ) == "fallo-dagster-entidad"


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
    missing_snapshot_run = SimpleNamespace(
        run_id="missing-snapshot-run",
        job_name="missing-snapshot-job",
        remote_job_origin=local_origin,
        execution_plan_snapshot_id="missing-snapshot",
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
            SimpleNamespace(dagster_run=missing_snapshot_run),
        ],
        get_execution_plan_snapshot=lambda snapshot_id: {
            "local-snapshot": local_snapshot,
            "foreign-snapshot": foreign_snapshot,
            "missing-snapshot": None,
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


@pytest.mark.parametrize(
    "user_code",
    ("projectD", "projectA", "projectB"),
)
@pytest.mark.parametrize("failure_point", ("direct", "intermediate"))
def test_dagster_failures_follow_real_asset_dependencies(
    user_code, failure_point, monkeypatch
):
    module = importlib.import_module(
        f"{user_code}.defs.sensors.operational_alerts_sensor"
    )

    @op(out={"asset": Out(), "result": Out()})
    def start():
        yield Output(1, "asset")
        if failure_point == "direct":
            raise Failure("Direct failure after partial materialization")
        yield Output(1, "result")

    @op
    def intermediate(value):
        raise Failure("Intermediate failure")

    @op
    def finish(value):
        return value

    @op
    def finalize(left, right):
        return left + right

    @graph_multi_asset(
        outs={name: AssetOut() for name in ("completed", "target", "control")}
    )
    def grouped_assets():
        initial = start()
        blocked = intermediate(initial.result)
        target = finish(blocked)
        control = finalize(target, blocked)
        return {"completed": initial.asset, "target": target, "control": control}

    @asset
    def dependent(target):
        return target

    @asset
    def independent():
        return 1

    monkeypatch.setattr(module, "_code_location", lambda _run: user_code)
    with DagsterInstance.ephemeral() as instance:
        result = materialize(
            [grouped_assets, dependent, independent],
            instance=instance,
            raise_on_error=False,
        )
        assert not result.success
        assert {
            event.asset_key.to_user_string()
            for event in result.get_asset_materialization_events()
        } == {"completed", "independent"}
        failures = module._failed_run_assets(
            SimpleNamespace(instance=instance),
            120,
            200,
            datetime.now(timezone.utc),
        )

    assert len(failures) == 3
    assert {failure["asset_key"] for failure in failures} == {
        "target", "control", "dependent"
    }
    assert {failure["run_id"] for failure in failures} == {result.run_id}
    expected_step = "start" if failure_point == "direct" else "intermediate"
    assert {failure["step_key"] for failure in failures} == {
        f"grouped_assets.{expected_step}"
    }


def test_sensor_applies_exact_threshold_and_default(monkeypatch):
    now = datetime.now(timezone.utc)
    sensor_20 = "00000000-0000-0000-0000-000000000020"
    sensor_200 = "00000000-0000-0000-0000-000000000200"
    default_rule = {**_rule(), "no_readings_minutes": 200}
    exact_rule = {**_rule(), "no_readings_minutes": 20}
    disabled_rules = {
        alert_type: {**_rule(alert_type), "is_enabled": False}
        for alert_type in (
            "late_readings",
            "pipeline_failure",
            "pipeline_stale",
            "dagster_run_failure",
        )
    }
    config = {
        "version_token": "config-1",
        "sensor_lookback_hours": 1,
        "dagster_failure_lookback_minutes": 120,
        "dagster_failure_batch_size": 200,
        "rules": {
            ("connection_lost", None): default_rule,
            ("connection_lost", sensor_20): exact_rule,
            **{
                (alert_type, None): rule
                for alert_type, rule in disabled_rules.items()
            },
        },
    }
    statuses = [
        {
            "sensor_id": sensor_id,
            "observed_at": now - timedelta(minutes=1),
            "last_reading_at": now - timedelta(minutes=25),
            "minutes_without_readings": 25,
            "modified_at": now - timedelta(minutes=1),
        }
        for sensor_id in (sensor_20, sensor_200)
    ]
    detected = Mock(return_value=True)
    monkeypatch.setattr(
        sensor_module,
        "_load_active_config",
        lambda _connection: (config, None),
    )
    monkeypatch.setattr(
        sensor_module,
        "_load_active_incidents",
        lambda _connection: {},
    )
    monkeypatch.setattr(
        sensor_module,
        "_sensor_statuses",
        lambda _connection, _lookback: statuses,
    )
    monkeypatch.setattr(
        sensor_module,
        "_sensor_dimensions",
        lambda _connection, _sensor_ids: {},
    )
    monkeypatch.setattr(
        sensor_module,
        "_ingestions",
        lambda _connection: [{
            "ingestion_name": "ingestion-without-status",
            "last_run_status": None,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_run_error": None,
        }],
    )
    monkeypatch.setattr(sensor_module, "_record_detected", detected)
    context = SimpleNamespace(
        resources=SimpleNamespace(sqlserver=_SqlResource("missing")),
        cursor=None,
        update_cursor=Mock(),
        log=Mock(),
    )
    result = sensor_module.operational_alerts_sensor._raw_fn(context)
    detected.assert_called_once()
    parameters = detected.call_args.args
    assert parameters[4] == sensor_20
    assert parameters[5] == statuses[0]["last_reading_at"] + timedelta(
        minutes=20
    )
    assert parameters[7]["thresholds"]["no_readings_minutes"] == 20
    assert "Señales detectadas: 1" in result.skip_message


@pytest.mark.parametrize(
    ("enabled", "reading_offset_seconds", "should_clear"),
    ((True, 0, False), (True, 1, True), (False, 1, False)),
)
def test_recovery_requires_later_evidence_and_enabled_rule(
    enabled,
    reading_offset_seconds,
    should_clear,
    monkeypatch,
):
    now = datetime.now(timezone.utc)
    sensor_id = "00000000-0000-0000-0000-000000000020"
    detected_at = now - timedelta(minutes=10)
    connection_rule = {**_rule(), "is_enabled": enabled}
    disabled_rules = {
        alert_type: {**_rule(alert_type), "is_enabled": False}
        for alert_type in (
            "late_readings",
            "pipeline_failure",
            "pipeline_stale",
            "dagster_run_failure",
        )
    }
    config = {
        "version_token": "config-1",
        "sensor_lookback_hours": 1,
        "dagster_failure_lookback_minutes": 120,
        "dagster_failure_batch_size": 200,
        "rules": {
            ("connection_lost", None): connection_rule,
            **{
                (alert_type, None): rule
                for alert_type, rule in disabled_rules.items()
            },
        },
    }
    incident = {
        "alert_id": uuid.uuid4(),
        "entity_key": sensor_id,
        "detected_at": detected_at,
        "no_readings_minutes": 20,
        "source_max_age_minutes": 60,
    }
    status = {
        "sensor_id": sensor_id,
        "observed_at": now - timedelta(minutes=1),
        "last_reading_at": detected_at + timedelta(
            seconds=reading_offset_seconds
        ),
        "minutes_without_readings": 0,
        "modified_at": now - timedelta(minutes=1),
    }
    cleared = Mock(return_value=True)
    monkeypatch.setattr(
        sensor_module,
        "_load_active_config",
        lambda _connection: (config, None),
    )
    monkeypatch.setattr(
        sensor_module,
        "_load_active_incidents",
        lambda _connection: {("connection_lost", sensor_id): incident},
    )
    monkeypatch.setattr(
        sensor_module,
        "_sensor_statuses",
        lambda _connection, _lookback: [status],
    )
    monkeypatch.setattr(
        sensor_module,
        "_sensor_dimensions",
        lambda _connection, _sensor_ids: {},
    )
    monkeypatch.setattr(
        sensor_module,
        "_ingestions",
        lambda _connection: [],
    )
    monkeypatch.setattr(sensor_module, "_record_cleared", cleared)
    context = SimpleNamespace(
        resources=SimpleNamespace(sqlserver=_SqlResource("missing")),
        cursor=None,
        update_cursor=Mock(),
        log=Mock(),
    )
    result = sensor_module.operational_alerts_sensor._raw_fn(context)
    if should_clear:
        cleared.assert_called_once()
        assert cleared.call_args.args[2] == status["last_reading_at"]
        assert "señales finalizadas: 1" in result.skip_message
    else:
        cleared.assert_not_called()
        assert "señales finalizadas: 0" in result.skip_message


@pytest.mark.parametrize(
    "user_code",
    ("projectD", "projectA", "projectB"),
)
@pytest.mark.parametrize("enabled", (True, False))
def test_dagster_recovery_clears_each_older_incident(
    user_code, enabled, monkeypatch
):
    module = importlib.import_module(
        f"{user_code}.defs.sensors.operational_alerts_sensor"
    )
    now = datetime.now(timezone.utc)
    recovered_at = now - timedelta(days=1)
    incidents = [
        {
            "alert_id": f"incident-{index}",
            "entity_id": f"failed-run-{index}",
            "entity_key": f"{user_code}/asset",
            "detected_at": now - timedelta(days=days),
        }
        for index, days in enumerate((3, 2, 0.5))
    ]
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.description = [(key,) for key in incidents[0]]
    cursor.fetchall.return_value = [tuple(row.values()) for row in incidents]
    rule = {**_rule("dagster_run_failure"), "is_enabled": enabled}
    config = {
        "version_token": "config-1",
        "dagster_failure_batch_size": 10,
        "rules": {("dagster_run_failure", None): rule},
    }
    instance = Mock()
    instance.get_run_by_id.side_effect = lambda run_id: (
        None if run_id == "missing" else SimpleNamespace(
            remote_job_origin=SimpleNamespace(
                repository_origin=SimpleNamespace(
                    code_location_origin=SimpleNamespace(
                        location_name="another_tenant" if run_id == "foreign"
                        else user_code
                    )
                )
            ),
            status=module.DagsterRunStatus.FAILURE,
        )
    )

    def materializations(records_filter, limit, cursor, ascending):
        assert limit == 10
        assert ascending
        assert records_filter.asset_key == AssetKey([user_code, "asset"])
        assert records_filter.before_timestamp == now.timestamp()
        incident = next(
            row for row in incidents
            if row["detected_at"].timestamp() == records_filter.after_timestamp
        )
        candidates = (
            [("foreign", recovered_at), ("missing", recovered_at),
             (incident["entity_id"], now - timedelta(minutes=1)),
             ("recovery", incident["detected_at"]),
             ("recovery", now + timedelta(minutes=1))]
            if cursor is None else [("recovery", recovered_at)]
        )
        return SimpleNamespace(
            records=[
                SimpleNamespace(run_id=run_id, timestamp=at.timestamp(), storage_id=7)
                for run_id, at in candidates
            ],
            has_more=cursor is None,
            cursor="next-page",
        )

    instance.fetch_materializations.side_effect = materializations
    record_cleared = Mock(return_value=True)
    monkeypatch.setattr(module, "_record_cleared", record_cleared)
    context = SimpleNamespace(instance=instance)
    checks = {}
    assert module._clear_dagster_failures(
        context, connection, config, checks, now
    ) == (2 if enabled else 0)
    assert [call.args[1]["alert_id"] for call in record_cleared.call_args_list] == (
        ["incident-0", "incident-1"] if enabled else []
    )
    for call in record_cleared.call_args_list:
        assert call.args[2] == recovered_at
        assert call.args[4]["evidence"]["recovery_run_id"] == "recovery"
        assert call.args[4]["evidence"]["failed_run_id"] == call.args[1]["entity_id"]
    if not enabled:
        instance.fetch_materializations.assert_not_called()
    assert module._clear_dagster_failures(
        context, connection, config, checks, now
    ) == 0


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
    )
    parameters = connection.cursor_value.parameters
    assert parameters[0] == 1
    assert parameters[5] == 1
    assert parameters[26] == 0
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


def test_row_count_capture_skips_event_without_timestamp(monkeypatch):
    record = SimpleNamespace(
        asset_materialization=SimpleNamespace(metadata={"row_count": 42}),
        asset_key=AssetKey(["projectA", "asset"]),
        run_id="run-id",
        timestamp=None,
        storage_id=10,
    )
    config = {
        "version_token": "config-1",
        "capture_interval_seconds": 30,
        "metadata_key": "row_count",
        "backfill_days": 28,
        "batch_size": 500,
    }
    recorded = Mock(return_value=True)
    monkeypatch.setattr(
        trend_module,
        "_load_active_config",
        lambda _connection: (config, None),
    )
    monkeypatch.setattr(
        trend_module,
        "_record_observation",
        recorded,
    )
    monkeypatch.setattr(
        trend_module,
        "_active_trend_incidents",
        lambda _connection: {},
    )
    monkeypatch.setattr(
        trend_module,
        "_latest_trends",
        lambda _connection, _metadata_key: [],
    )
    context = SimpleNamespace(
        instance=SimpleNamespace(
            event_log_storage=SimpleNamespace(
                get_event_records=Mock(return_value=[record]),
            ),
        ),
        resources=SimpleNamespace(sqlserver=_SqlResource("missing")),
        cursor=None,
        update_cursor=Mock(),
        log=Mock(),
    )
    result = trend_module.asset_row_count_trend_sensor._raw_fn(context)
    recorded.assert_not_called()
    assert "observaciones registradas: 0" in result.skip_message
