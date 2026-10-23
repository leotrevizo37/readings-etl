import copy
import importlib
import json
import sys
from datetime import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
REPOSITORY_ROOT = PROJECT_SRC.parents[2]
original_sys_path = list(sys.path)
sys.path[:] = [str(PROJECT_SRC), *[
    path for path in original_sys_path
    if Path(path or ".").resolve() != REPOSITORY_ROOT
]]
original_dagster_module = sys.modules.pop("dagster", None)
try:
    sensor_module = importlib.import_module("projectA.defs.sensors.telemetry_configuration_sensor")
    asset_module = importlib.import_module("projectA.defs.assets.telemetry_configuration")
    transformation_module = importlib.import_module("projectA.defs.transformations.telemetry_configuration")
    dagster_module = importlib.import_module("dagster")
    ai_module = importlib.import_module("projectA.defs.resources.telemetry_configuration_ai")
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


@pytest.fixture
def snapshot():
    return {
        "devices": [{"DeviceId": "device-id", "DeviceTyId": "device-type-id", "Name": "Congelador", "Active": True}],
        "device_types": [{"DeviceTyId": "device-type-id", "Name": "Congelador", "Active": True}],
        "sensors": [{"SensorId": "sensor-id", "DeviceId": "device-id", "SensorTyId": "sensor-type-id", "Name": "Temperatura", "Active": True}],
        "sensor_types": [{"SensorTyId": "sensor-type-id", "Name": "Temperatura F", "UnitName": "Fahrenheit", "UnitAbbreviation": "F", "Active": True}],
        "alerts": [{"AlertId": "alert-id", "Name": "Temperatura alta", "DeviceTyId": "device-type-id", "Active": True}],
        "conditions": [{"AlertConditionId": "condition-id", "AlertId": "alert-id", "SensorTyId": "sensor-type-id", "Operator": ">=", "Value": 10.0, "Active": True}],
        "stopped": [],
        "expected": [{"SensorId": "sensor-id", "DeviceId": "device-id", "min_expected_value": Decimal("0"), "max_expected_value": Decimal("10"), "StartTime": time(0), "EndTime": time(23, 59), "scheduledDays": "1,2,3,4,5,6,7"}],
    }


@pytest.mark.parametrize("spacing", ["   ", "\t", "\n", "\u00a0"])
def test_payload_normalizes_text_before_sending_to_llm(snapshot, azure, spacing):
    expected = transformation_module._candidates(snapshot)
    for rows in snapshot.values():
        for row in rows:
            for key in ("Name", "UnitName", "UnitAbbreviation", "Operator", "scheduledDays"):
                if isinstance(row.get(key), str):
                    row[key] = spacing + row[key].replace(" ", spacing) + spacing
    original = copy.deepcopy(snapshot)
    actual = transformation_module._candidates(snapshot)
    assert actual == expected and snapshot == original
    client, connection, _, _, _ = azure
    for key, candidate in actual.items():
        assert transformation_module._fingerprint(candidate) == transformation_module._fingerprint(expected[key])
        client.evaluate(candidate["payload"])
        body = json.loads(connection.request.call_args.args[2])
        assert json.loads(body["messages"][1]["content"]) == expected[key]["payload"]


@pytest.mark.parametrize("name", [None, "", "   \t\n"])
def test_missing_or_blank_device_name_remains_incomplete(snapshot, name):
    snapshot["devices"][0]["Name"] = name
    candidate = transformation_module._candidates(snapshot)[(transformation_module._DEVICE_TYPE, "device:device-id")]
    assert candidate["payload"]["device"] == (None if name is None else "")
    assert not candidate["complete"] and not candidate["problems"]


def test_payload_preserves_ranges_and_includes_entity_ids(snapshot):
    second = dict(snapshot["expected"][0], StartTime=time(12), min_expected_value=None, max_expected_value=Decimal("12"), scheduledDays="6,7")
    snapshot["expected"].append(second)
    candidates = transformation_module._candidates(snapshot)
    alert = candidates[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert alert["complete"] and not alert["problems"]
    assert candidates[(sensor_module._DEVICE_TYPE, "device:device-id")]["payload"]["device_id"] == "device-id"
    assert alert["payload"]["alert_id"] == "alert-id"
    assert alert["payload"]["alert_name"] == "Temperatura alta"
    item = alert["payload"]["devices"][0]["conditions"][0]
    assert item["alert_condition_id"] == "condition-id"
    assert item["threshold"] == 10 and item["operator"] == ">="
    ranges = item["sensors"][0]["expected_ranges"]
    assert ranges[0]["min"] == 0 and ranges[1]["min"] is None
    assert ranges[1]["start"] == "12:00:00" and ranges[1]["days"] == "6,7"
    for candidate in candidates.values():
        assert "source_ids" not in candidate["payload"]


@pytest.mark.parametrize("missing", ["expected", "device_types", "sensor_types", "conditions", "sensors"])
def test_missing_evidence_is_not_consistent(snapshot, missing):
    snapshot[missing] = []
    alert = transformation_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    if missing == "expected":
        assert alert["complete"] and not alert["problems"]
        assert alert["payload"]["devices"][0]["conditions"][0]["sensors"][0]["expected_ranges"] == []
    else:
        assert alert["problems"] or not alert["complete"]


def test_stopped_devices_and_inactive_alerts_are_excluded(snapshot):
    snapshot["stopped"] = [{"AlertId": "alert-id", "DeviceId": "device-id"}]
    alert = transformation_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert not alert["complete"] and alert["payload"]["devices"] == []
    snapshot["alerts"][0]["Active"] = False
    assert all(key[0] != sensor_module._ALERT_TYPE for key in transformation_module._candidates(snapshot))


def test_orphans_invalid_operator_and_inverted_range(snapshot):
    snapshot["devices"] = []
    assert transformation_module._candidates(snapshot)[(sensor_module._DEVICE_TYPE, "device:device-id")]["problems"]
    snapshot["alerts"] = []
    assert transformation_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "condition:condition-id")]["problems"]


@pytest.mark.parametrize("value", [None, float("nan"), float("inf")])
def test_invalid_threshold_is_structural_and_serializable(snapshot, value):
    snapshot["conditions"][0]["Value"] = value
    alert = transformation_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert alert["problems"]
    assert transformation_module._fingerprint(alert)


def test_invalid_operator_and_range(snapshot):
    snapshot["conditions"][0]["Operator"] = None
    snapshot["expected"][0]["min_expected_value"] = Decimal("20")
    alert = transformation_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert len(alert["problems"]) == 2


@pytest.fixture
def evaluation(monkeypatch, snapshot):
    connection = MagicMock()
    rule = {
        "alert_type": sensor_module._DEVICE_TYPE, "alert_type_id": "type-id",
        "is_enabled": True, "evaluation_interval_seconds": 3600,
        "rule_version": b"1", "alert_rule_id": "rule-id",
        "severity": "WARNING", "severity_id": "warning-id",
    }
    config = {"version_token": "1", "rules": {(sensor_module._DEVICE_TYPE, None): rule}}
    evaluator = SimpleNamespace(policy="policy-1", evaluate=Mock(return_value={
        "status": "suspicious", "confidence": 0.95,
        "reasons": ["Incongruencia semántica comprobable."], "model": "test-model",
    }))
    context = SimpleNamespace(
        cursor=None, update_cursor=Mock(), instance=Mock(), run_id="synthetic-run",
        op_config={"alert_type": sensor_module._DEVICE_TYPE, "entity_key": "device:device-id",
                   "config_version": "1", "rule_version": "31", "policy": "policy-1"},
        resources=SimpleNamespace(
            sqlserver=SimpleNamespace(_get_connection=lambda: connection),
            dim_sqlserver=Mock(), telemetry_configuration_ai=evaluator,
        ),
    )
    monkeypatch.setattr(sensor_module, "_load_active_config", Mock(return_value=(config, None)))
    context.instance.get_runs.return_value = []
    monkeypatch.setattr(asset_module, "_load_active_config", sensor_module._load_active_config)
    monkeypatch.setattr(asset_module, "_snapshot", Mock(return_value=snapshot))
    monkeypatch.setattr(sensor_module, "_snapshot", Mock(side_effect=lambda source, warehouse, watermark_only=False, entities_only=False: (
        {"devices": [{"modified_at": "2026-09-28T12:00:00", "row_count": 1}]}
        if watermark_only else {key: [row for row in snapshot[key] if key in ("devices", "alerts") or row["Active"]]
                                for key in ("devices", "sensors", "alerts", "conditions")}
    )))
    monkeypatch.setattr(asset_module, "_rows", Mock(return_value=[]))
    detected = Mock(return_value=True)
    cleared = Mock(return_value=True)
    monkeypatch.setattr(asset_module, "_record_detected", detected)
    monkeypatch.setattr(asset_module, "_record_cleared", cleared)
    return context, evaluator, detected, cleared, config, connection


def test_detects_using_existing_signal_writer(evaluation):
    context, evaluator, detected, cleared, _, connection = evaluation
    asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert detected.call_count == 1 and not cleared.called
    assert detected.call_args.args[2] == "sensor_monitoring"
    assert detected.call_args.args[3] == "device:device-id"
    connection.__exit__.assert_called_once_with(None, None, None)
    assert evaluator.evaluate.call_count == 1


@pytest.mark.parametrize("status,confidence,error,detects", [
    ("insufficient_evidence", 0.8, None, False), ("insufficient_evidence", 0.95, None, True),
    ("suspicious", 0.89, None, True), ("consistent", 0.89, None, False),
    ("consistent", 0.95, TimeoutError(), False), ("consistent", 0.95, ValueError(), False),
])
def test_inconclusive_evaluation_warning(evaluation, status, confidence, error, detects):
    context, evaluator, detected, cleared, _, _ = evaluation
    evaluator.evaluate.return_value.update(status=status, confidence=confidence)
    evaluator.evaluate.side_effect = error
    if error is None:
        asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    else:
        with pytest.raises(dagster_module.Failure):
            asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert detected.called == detects and not cleared.called
    if detects:
        assert detected.call_args.kwargs["severity_id"] == "warning-id"


def test_low_confidence_uses_warning_severity_with_other_rule_severity(evaluation):
    context, evaluator, detected, _, config, _ = evaluation
    config["rules"][(sensor_module._DEVICE_TYPE, None)].update(severity="CRITICAL", severity_id="critical-id")
    evaluator.evaluate.return_value["confidence"] = 0.8
    asset_module._rows.side_effect = [[], [{"severity_id": "warning-id"}]]
    asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert detected.call_args.kwargs["severity_id"] == "warning-id"


def test_structural_detection_does_not_need_azure(evaluation, snapshot):
    context, evaluator, detected, _, _, _ = evaluation
    snapshot["sensor_types"] = []
    asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert detected.called and not evaluator.evaluate.called


@pytest.mark.parametrize("status,confidence,detects,expected_clear", [
    ("consistent", 0.89, False, False), ("consistent", 0.9, False, True),
    ("consistent", 0.95, False, True), ("suspicious", 0.95, True, False),
    ("insufficient_evidence", 0.89, False, False), ("insufficient_evidence", 0.95, True, False),
])
def test_alert_without_expected_ranges_uses_recovery_confidence(evaluation, snapshot, status, confidence, detects, expected_clear):
    context, evaluator, detected, cleared, config, _ = evaluation
    snapshot["expected"] = []
    evaluator.evaluate.return_value.update(status=status, confidence=confidence)
    rule = config["rules"].pop((sensor_module._DEVICE_TYPE, None))
    rule["alert_type"] = sensor_module._ALERT_TYPE
    config["rules"][(sensor_module._ALERT_TYPE, None)] = rule
    context.op_config.update(alert_type=sensor_module._ALERT_TYPE, entity_key="alert:alert-id")
    asset_module._rows.return_value = [{"metadata_json": json.dumps({
        "source_fingerprint": "previous-source", "policy": evaluator.policy,
        "assessment": {"model": "test-model"},
    })}]
    result = asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert evaluator.evaluate.call_args.args[0]["devices"][0]["conditions"][0]["sensors"][0]["expected_ranges"] == []
    assert detected.called == detects and cleared.called == expected_clear
    assert (result.metadata["signal"] == "CLEARED") == expected_clear
    if expected_clear:
        assert result.metadata["evaluation"].value["recovery_conditions"]["expected_ranges_available"] is False


@pytest.mark.parametrize("status,detects,expected_clear", [("suspicious", True, False), ("consistent", False, True)])
def test_alert_with_mixed_expected_ranges_evaluates_all_devices(evaluation, snapshot, status, detects, expected_clear):
    context, evaluator, detected, cleared, config, _ = evaluation
    evaluator.evaluate.return_value["status"] = status
    snapshot["devices"].append(dict(snapshot["devices"][0], DeviceId="device-two", Name="Otro congelador"))
    snapshot["sensors"].append(dict(snapshot["sensors"][0], SensorId="sensor-two", DeviceId="device-two"))
    rule = config["rules"].pop((sensor_module._DEVICE_TYPE, None))
    rule["alert_type"] = sensor_module._ALERT_TYPE
    config["rules"][(sensor_module._ALERT_TYPE, None)] = rule
    context.op_config.update(alert_type=sensor_module._ALERT_TYPE, entity_key="alert:alert-id")
    asset_module._rows.return_value = [{"metadata_json": json.dumps({
        "source_fingerprint": "previous-source", "policy": evaluator.policy,
        "assessment": {"model": "test-model"},
    })}]
    asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    devices = evaluator.evaluate.call_args.args[0]["devices"]
    assert len(devices) == 2
    assert devices[0]["conditions"][0]["sensors"][0]["expected_ranges"]
    assert devices[1]["conditions"][0]["sensors"][0]["expected_ranges"] == []
    assert detected.called == detects and cleared.called == expected_clear


def test_disabled_entity_override_is_respected(evaluation):
    context, evaluator, detected, cleared, config, _ = evaluation
    rule = config["rules"][(sensor_module._DEVICE_TYPE, None)]
    config["rules"][(sensor_module._DEVICE_TYPE, "device:device-id")] = dict(rule, is_enabled=False)
    sensor_module.telemetry_configuration_sensor._raw_fn(context)
    assert not evaluator.evaluate.called and not detected.called and not cleared.called


def test_sensor_asset_and_job_are_registered():
    definitions = importlib.import_module("projectA.defs")._build_defs()
    detector = sensor_module.telemetry_configuration_sensor
    assert detector in definitions.sensors
    assert asset_module.telemetry_configuration in definitions.assets
    assert sensor_module.telemetry_configuration_job in definitions.jobs
    loaded = importlib.import_module("projectA.definitions").defs()
    job = loaded.resolve_job_def("telemetry_configuration_job")
    assert job.has_node_named("projectA__telemetry_configuration")
    assert detector.default_status == sensor_module.DefaultSensorStatus.STOPPED
    assert detector.required_resource_keys <= definitions.resources.keys()


@pytest.mark.parametrize("change", ["source", "config"])
def test_changes_during_evaluation_emit_nothing(monkeypatch, evaluation, snapshot, change):
    context, _, detected, cleared, config, _ = evaluation
    if change == "source":
        changed = copy.deepcopy(snapshot)
        changed["devices"][0]["Name"] = "Otra configuración"
        asset_module._snapshot.side_effect = [snapshot, changed]
    else:
        changed = dict(config, version_token="2")
        sensor_module._load_active_config.side_effect = [(config, None), (changed, None)]
    with pytest.raises(dagster_module.Failure):
        asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert not detected.called and not cleared.called


@pytest.mark.parametrize("same_source,policy,model,expected_clear", [
    (False, "policy-1", "test-model", True),
    (False, "policy-1", "structural", True),
    (True, "policy-1", "test-model", False),
    (False, "old-policy", "test-model", False),
    (False, "policy-1", "old-model", False),
])
@pytest.mark.parametrize("without_expected_ranges", [False, True])
def test_recovery_requires_changed_evidence_and_same_policy(monkeypatch, evaluation, snapshot, same_source, policy, model, expected_clear, without_expected_ranges):
    context, evaluator, detected, cleared, config, _ = evaluation
    if without_expected_ranges:
        snapshot["expected"] = []
        rule = config["rules"].pop((sensor_module._DEVICE_TYPE, None))
        rule["alert_type"] = sensor_module._ALERT_TYPE
        config["rules"][(sensor_module._ALERT_TYPE, None)] = rule
        context.op_config.update(alert_type=sensor_module._ALERT_TYPE, entity_key="alert:alert-id")
    evaluator.evaluate.return_value["status"] = "consistent"
    candidate = transformation_module._candidates(snapshot)[(context.op_config["alert_type"], context.op_config["entity_key"])]
    previous = {
        "source_fingerprint": transformation_module._fingerprint(candidate) if same_source else "previous-source",
        "policy": policy, "assessment": {"model": model},
    }
    monkeypatch.setattr(asset_module, "_rows", Mock(return_value=[{"alert_id": "incident-id", "metadata_json": json.dumps(previous)}]))
    result = asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert cleared.called == expected_clear and not detected.called
    assert result.metadata["outcome"] == ("cleared" if expected_clear else "recovery_not_confirmed")
    assert result.metadata["signal"] == ("CLEARED" if expected_clear else "NONE")
    conditions = result.metadata["evaluation"].value["recovery_conditions"]
    assert all(value for key, value in conditions.items() if key != "expected_ranges_available") == expected_clear


@pytest.fixture
def azure(monkeypatch):
    connection = MagicMock()
    response = connection.getresponse.return_value
    response.status = 200
    assessment = {"verdict": "no_problem_found", "confidence": 0.95, "reasons": ["Tipos compatibles."], "reasoning": {"analysis": "Comprobación de relaciones."}}
    result = {"model": "test-model", "choices": [{"finish_reason": "stop", "message": {"content": json.dumps(assessment)}}]}
    response.read.side_effect = lambda _: json.dumps(result).encode()
    factory = Mock(return_value=connection)
    monkeypatch.setattr(ai_module.http.client, "HTTPSConnection", factory)
    client = ai_module.TelemetryConfigurationAI("https://example.openai.azure.com", "deployment-name", "fake-test-key")
    return client, connection, result, assessment, factory


def test_azure_request_contract(azure):
    client, connection, _, _, _ = azure
    evaluated = client.evaluate({"kind": "device_configuration"})
    assert evaluated["model"] == "test-model" and evaluated["status"] == "consistent"
    assert "reasoning" not in evaluated and "verdict" not in evaluated
    args = connection.request.call_args.args
    assert args[:2] == ("POST", "/openai/v1/chat/completions")
    body = json.loads(args[2])
    assert body["model"] == "deployment-name" and body["store"] is False
    assert body["response_format"]["json_schema"]["strict"] is True
    assert "temperature" not in body
    connection.close.assert_called_once()


@pytest.mark.parametrize("verdict,status", [("problem", "suspicious"), ("no_problem_found", "consistent"), ("insufficient_evidence", "insufficient_evidence")])
def test_ai_verdict_maps_to_sensor_status(azure, verdict, status):
    client, _, result, assessment, _ = azure
    assessment["verdict"] = verdict
    result["choices"][0]["message"]["content"] = json.dumps(assessment)
    assert client.evaluate({})["status"] == status


@pytest.mark.parametrize("count", [1, 2, 3])
def test_ai_accepts_up_to_three_reasons(azure, count):
    client, _, result, assessment, _ = azure
    assessment["reasons"] = ["Razón válida."] * count
    result["choices"][0]["message"]["content"] = json.dumps(assessment)
    assert client.evaluate({})["reasons"] == assessment["reasons"]


@pytest.mark.parametrize("invalid", ["truncated", "refusal", "redirect", "nan", "bool", "empty", "excessive", "reasoning", "extra", "status", "model"])
def test_invalid_azure_response_is_rejected(azure, invalid):
    client, connection, result, assessment, _ = azure
    if invalid == "truncated":
        result["choices"][0]["finish_reason"] = "length"
    elif invalid == "refusal":
        result["choices"][0]["message"]["refusal"] = "refused"
    elif invalid == "redirect":
        connection.getresponse.return_value.status = 302
    elif invalid == "nan":
        assessment["confidence"] = float("nan")
    elif invalid == "bool":
        assessment["confidence"] = True
    elif invalid == "empty":
        assessment["reasons"] = []
    elif invalid == "excessive":
        assessment["reasons"].extend(["Otra razón."] * 3)
    elif invalid == "reasoning":
        assessment["reasoning"] = {"analysis": ""}
    elif invalid == "extra":
        assessment["unrequested"] = "data"
    elif invalid == "model":
        result["model"] = None
    else:
        assessment["verdict"] = "invalid"
    result["choices"][0]["message"]["content"] = json.dumps(assessment)
    with pytest.raises(ValueError):
        client.evaluate({})
    connection.close.assert_called_once()


@pytest.mark.parametrize("endpoint", ["http://example.openai.azure.com", "https://untrusted.example", "https://example.openai.azure.com@untrusted.example", "https://example.openai.azure.com/path"])
def test_invalid_endpoint_is_not_contacted(azure, endpoint):
    client, _, _, _, factory = azure
    client.endpoint = endpoint
    with pytest.raises(ValueError):
        client.evaluate({})
    factory.assert_not_called()


def test_snapshot_reads_only_required_configuration(monkeypatch):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = MagicMock(spec_set=transformation_module.pyodbc.Cursor)
    connection.cursor.return_value.__enter__.return_value.description = []
    connection.cursor.return_value.__enter__.return_value.fetchall.return_value = []
    connect = Mock(return_value=connection)
    monkeypatch.setattr(transformation_module.pyodbc, "connect", connect)
    source = SimpleNamespace(host="localhost", port=1433, database="synthetic", user="test", password="fake", connect_timeout=10)
    warehouse = SimpleNamespace(_get_connection=lambda: connection)
    snapshot = transformation_module._snapshot(source, warehouse)
    assert len(snapshot) == 8 and all(rows == [] for rows in snapshot.values())
    assert connect.call_args.kwargs["driver"] == "{ODBC Driver 18 for SQL Server}"
    assert connection.close.call_count == 2
    assert connection.timeout == 20


def test_watermark_covers_all_tables_and_counts_inactive_rows(monkeypatch):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = MagicMock(spec_set=transformation_module.pyodbc.Cursor)
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.description = [("modified_at",), ("row_count",)]
    cursor.fetchall.side_effect = [[("2026-09-28T12:00:00.1234567", 3)]] * 7 + [[(None, 0)]]
    monkeypatch.setattr(transformation_module.pyodbc, "connect", Mock(return_value=connection))
    source = SimpleNamespace(host="localhost", port=1433, database="synthetic", user="test", password="fake", connect_timeout=10)
    warehouse = SimpleNamespace(_get_connection=lambda: connection)
    watermark = transformation_module._snapshot(source, warehouse, watermark_only=True)
    assert set(watermark) == {"devices", "device_types", "sensors", "sensor_types", "alerts", "conditions", "stopped", "expected"}
    assert watermark["devices"] == [{"modified_at": "2026-09-28T12:00:00.1234567", "row_count": 3}]
    assert watermark["expected"] == [{"modified_at": None, "row_count": 0}]
    queries = [call.args[0] for call in cursor.execute.call_args_list]
    assert all("MAX(ModifiedAt)" in query and "COUNT_BIG(*)" in query and "WHERE" not in query for query in queries)
    assert "dbo.AlertStoppedOnDevices" in queries[6]
    assert "dwh.dimSensorExpectedVal" in queries[7]


def test_sensor_dispatches_without_evaluation_or_writes_and_waits_for_success(evaluation):
    context, evaluator, detected, cleared, _, _ = evaluation
    request = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert isinstance(request, dagster_module.RunRequest)
    assert state["pending"] == [[sensor_module._DEVICE_TYPE, "device:device-id"]]
    assert state["in_flight"]["run_key"] == request.run_key
    assert request.run_config["ops"]["projectA__telemetry_configuration"]["config"] == context.op_config
    assert not evaluator.evaluate.called and not detected.called and not cleared.called
    assert not asset_module._snapshot.called
    context.cursor = json.dumps(state)
    context.instance.get_runs.return_value = [SimpleNamespace(is_finished=True, status=dagster_module.DagsterRunStatus.SUCCESS)]
    sensor_module._snapshot.reset_mock()
    result = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    assert isinstance(result, dagster_module.SkipReason)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert state["pending"] == [] and "in_flight" not in state
    assert sensor_module._snapshot.call_count == 1
    assert sensor_module._snapshot.call_args.kwargs == {"watermark_only": True}
    assert not evaluator.evaluate.called


@pytest.mark.parametrize("change", ["modified_at", "row_count", "config", "rule", "policy"])
def test_watermark_or_evaluation_changes_restart_processing(evaluation, snapshot, change):
    context, evaluator, detected, _, config, _ = evaluation
    first = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    context.cursor = json.dumps(state)
    context.instance.get_runs.return_value = [SimpleNamespace(is_finished=True, status=dagster_module.DagsterRunStatus.SUCCESS)]
    watermark = copy.deepcopy(state["watermark"])
    if change == "modified_at":
        watermark["devices"][0]["modified_at"] = "2026-09-28T13:00:00"
    elif change == "row_count":
        watermark["devices"][0]["row_count"] = 0
    elif change == "config":
        config["version_token"] = "2"
    elif change == "rule":
        config["rules"][(sensor_module._DEVICE_TYPE, None)]["rule_version"] = b"2"
    else:
        evaluator.policy = "policy-2"
    sensor_module._snapshot.side_effect = lambda source, warehouse, watermark_only=False, entities_only=False: (
        watermark if watermark_only else snapshot
    )
    request = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    assert isinstance(request, dagster_module.RunRequest) and request.run_key != first.run_key
    assert not evaluator.evaluate.called and not detected.called


@pytest.mark.parametrize("status", ["NOT_STARTED", "QUEUED", "STARTING", "STARTED", "CANCELING"])
def test_in_flight_run_blocks_new_dispatch_even_if_source_changes(evaluation, status):
    context, evaluator, detected, _, _, _ = evaluation
    sensor_module.telemetry_configuration_sensor._raw_fn(context)
    context.cursor = context.update_cursor.call_args.args[0]
    context.instance.get_runs.return_value = [SimpleNamespace(is_finished=False, status=getattr(dagster_module.DagsterRunStatus, status))]
    sensor_module._snapshot.reset_mock()
    assert isinstance(sensor_module.telemetry_configuration_sensor._raw_fn(context), dagster_module.SkipReason)
    assert not sensor_module._snapshot.called and not evaluator.evaluate.called and not detected.called


def test_missing_run_reuses_original_request_and_key(evaluation):
    context, _, _, _, _, _ = evaluation
    request = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    context.cursor = context.update_cursor.call_args.args[0]
    sensor_module._snapshot.reset_mock()
    retry = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    assert retry == request
    assert not sensor_module._snapshot.called


def test_pending_entities_survive_between_ticks(evaluation, snapshot):
    context, evaluator, detected, _, _, _ = evaluation
    snapshot["devices"].append(dict(snapshot["devices"][0], DeviceId="device-two"))
    snapshot["sensors"].append(dict(snapshot["sensors"][0], DeviceId="device-two", SensorId="sensor-two"))
    first = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert len(state["pending"]) == 2
    context.cursor = json.dumps(state)
    context.instance.get_runs.return_value = [SimpleNamespace(is_finished=True, status=dagster_module.DagsterRunStatus.SUCCESS)]
    second = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    assert first.tags["entity_key"] == "device:device-id" and second.tags["entity_key"] == "device:device-two"
    assert json.loads(context.update_cursor.call_args.args[0])["pending"] == [[sensor_module._DEVICE_TYPE, "device:device-two"]]
    assert not evaluator.evaluate.called and not detected.called


@pytest.mark.parametrize("status", ["FAILURE", "CANCELED"])
def test_failed_run_remains_pending_and_retries_after_interval(evaluation, status):
    context, evaluator, detected, _, _, _ = evaluation
    first = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    context.cursor = context.update_cursor.call_args.args[0]
    context.instance.get_runs.return_value = [SimpleNamespace(is_finished=True, status=getattr(dagster_module.DagsterRunStatus, status))]
    assert isinstance(sensor_module.telemetry_configuration_sensor._raw_fn(context), dagster_module.SkipReason)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert state["pending"] == [[sensor_module._DEVICE_TYPE, "device:device-id"]]
    for check in state["checks"].values():
        check["at"] = 0
    context.cursor = json.dumps(state)
    retry = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    assert isinstance(retry, dagster_module.RunRequest) and retry.run_key != first.run_key
    assert retry.run_config == first.run_config
    assert not evaluator.evaluate.called and not detected.called


def test_persistence_failure_does_not_materialize(evaluation):
    context, _, detected, _, _, connection = evaluation
    detected.side_effect = RuntimeError("Synthetic persistence failure")
    with pytest.raises(RuntimeError):
        asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert connection.__exit__.call_args.args[0] is RuntimeError


@pytest.mark.parametrize("endpoint,host,port,tls", [
    ("http://localhost:1234", "localhost", 1234, False),
    ("http://127.0.0.1:8080", "127.0.0.1", 8080, False),
    ("http://[::1]:1234", "::1", 1234, False),
    ("http://host.docker.internal:1234", "host.docker.internal", 1234, False),
    ("https://localhost:8443", "localhost", 8443, True),
])
def test_local_request_without_azure_credentials(monkeypatch, azure, endpoint, host, port, tls):
    _, connection, _, _, _ = azure
    http_factory = Mock(return_value=connection)
    https_factory = Mock(return_value=connection)
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", http_factory)
    monkeypatch.setattr(ai_module.http.client, "HTTPSConnection", https_factory)
    client = ai_module.TelemetryConfigurationAI(endpoint, "local-model", provider="local", timeout_seconds=40)
    assert client.evaluate({"kind": "device_configuration"})["status"] == "consistent"
    factory, unused = (https_factory, http_factory) if tls else (http_factory, https_factory)
    factory.assert_called_once_with(host, port=port, timeout=40)
    unused.assert_not_called()
    args = connection.request.call_args.args
    assert args[:2] == ("POST", "/v1/chat/completions")
    assert args[3] == {"Content-Type": "application/json"}
    body = json.loads(args[2])
    assert body["model"] == "local-model" and body["max_tokens"] == 4096
    assert body["stream"] is False
    assert "store" not in body and "max_completion_tokens" not in body
    assert body["response_format"]["json_schema"]["schema"] == ai_module._SCHEMA
    assert body["messages"][0]["content"] == ai_module._PROMPT
    connection.close.assert_called_once()


@pytest.mark.parametrize("allowed_host,accepted", [
    ("", False), ("other.example", False), ("MODEL.EXAMPLE ", True),
])
def test_local_configured_host(monkeypatch, azure, allowed_host, accepted):
    _, connection, _, _, _ = azure
    factory = Mock(return_value=connection)
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", factory)
    client = ai_module.TelemetryConfigurationAI(
        "http://model.example:1234", "local-model", provider="local",
        local_allowed_host=allowed_host,
    )
    if accepted:
        assert client.evaluate({})["status"] == "consistent"
        factory.assert_called_once_with("model.example", port=1234, timeout=20)
    else:
        with pytest.raises(ValueError):
            client.evaluate({})
        factory.assert_not_called()


def test_local_optional_authentication(monkeypatch, azure):
    _, connection, _, _, _ = azure
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", Mock(return_value=connection))
    client = ai_module.TelemetryConfigurationAI("http://localhost:1234", "local-model", "fake-local-key", provider="local")
    client.evaluate({})
    headers = connection.request.call_args.args[3]
    assert headers["Authorization"] == "Bearer fake-local-key"
    assert "api-key" not in headers


@pytest.mark.parametrize("endpoint", [
    "http://untrusted.example:1234", "http://localhost.untrusted.example:1234",
    "http://localhost:1234/v1", "http://localhost:1234?token=example",
    "http://user@localhost:1234", "http://localhost:0", "ftp://localhost:1234",
])
def test_local_rejects_invalid_endpoints_without_request(monkeypatch, endpoint):
    http_factory = Mock()
    https_factory = Mock()
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", http_factory)
    monkeypatch.setattr(ai_module.http.client, "HTTPSConnection", https_factory)
    client = ai_module.TelemetryConfigurationAI(endpoint, "local-model", provider="local")
    with pytest.raises(ValueError):
        client.evaluate({})
    http_factory.assert_not_called()
    https_factory.assert_not_called()


@pytest.mark.parametrize("status", [302, 500])
def test_local_http_error_closes_connection_without_fallback(monkeypatch, azure, status):
    _, connection, _, _, https_factory = azure
    connection.getresponse.return_value.status = status
    http_factory = Mock(return_value=connection)
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", http_factory)
    client = ai_module.TelemetryConfigurationAI("http://localhost:1234", "local-model", provider="local")
    with pytest.raises(ValueError):
        client.evaluate({})
    assert http_factory.call_count == 1
    https_factory.assert_not_called()
    connection.close.assert_called_once()


@pytest.mark.parametrize("provider", [None, "azure", "local"])
def test_resource_selects_only_configured_provider(monkeypatch, provider):
    values = {"PROJECTA_TELEMETRY_AI_TIMEOUT_SECONDS": "40"}
    if provider is not None:
        values["PROJECTA_TELEMETRY_AI_PROVIDER"] = provider
    if provider == "local":
        values["PROJECTA_TELEMETRY_LOCAL_MODEL"] = "local-model"
        values["PROJECTA_TELEMETRY_LOCAL_ALLOWED_HOST"] = "model.example"
    getenv = Mock(side_effect=values.get)
    monkeypatch.setattr(ai_module.os, "getenv", getenv)
    client = ai_module.telemetry_configuration_ai_resource.resource_fn(None)
    assert client.provider == (provider or "local")
    assert client.timeout_seconds == 40
    if provider == "local":
        assert client.endpoint == "http://localhost:1234" and client.api_key == ""
        assert client.deployment == "local-model"
        assert client.local_allowed_host == "model.example"
        assert all("AZURE" not in call.args[0] for call in getenv.call_args_list)


@pytest.mark.parametrize("timeout", [0, -1, 301, float("inf"), True])
def test_invalid_local_timeout_is_rejected(timeout):
    with pytest.raises(ValueError):
        ai_module.TelemetryConfigurationAI("http://localhost:1234", "local-model", provider="local", timeout_seconds=timeout)


def test_unknown_provider_is_rejected():
    with pytest.raises(ValueError):
        ai_module.TelemetryConfigurationAI("http://localhost:1234", "local-model", provider="unknown")


@pytest.mark.parametrize("status,confidence,inserted,outcome,signal,passed", [
    ("suspicious", 0.95, True, "detected", "DETECTED", False),
    ("suspicious", 0.95, False, "deduplicated", "NONE", False),
    ("suspicious", 0.8, True, "detected", "DETECTED", False),
    ("insufficient_evidence", 0.95, True, "detected", "DETECTED", False),
    ("insufficient_evidence", 0.8, False, "inconclusive", "NONE", False),
    ("consistent", 0.8, False, "inconclusive", "NONE", False),
    ("consistent", 0.95, False, "consistent", "NONE", True),
])
def test_job_materialization_checks_and_sensor_acknowledgement(evaluation, status, confidence, inserted, outcome, signal, passed):
    context, evaluator, detected, _, _, _ = evaluation
    evaluator.evaluate.return_value.update(status=status, confidence=confidence)
    detected.return_value = inserted
    request = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    context.cursor = context.update_cursor.call_args.args[0]
    definitions = dagster_module.Definitions(
        assets=[asset_module.telemetry_configuration], jobs=[sensor_module.telemetry_configuration_job],
        resources={key: dagster_module.ResourceDefinition.hardcoded_resource(value)
                   for key, value in vars(context.resources).items()},
    )
    job = definitions.resolve_job_def("telemetry_configuration_job")
    with dagster_module.DagsterInstance.ephemeral() as instance:
        instance.create_run_for_job(job, run_config=request.run_config, status=dagster_module.DagsterRunStatus.SUCCESS, tags={
            "user_code": "other-tenant", "dagster/run_key": request.run_key, "dagster/sensor_name": "telemetry_configuration_sensor",
        })
        context.instance = instance
        assert sensor_module.telemetry_configuration_sensor._raw_fn(context) == request
        result = job.execute_in_process(
            instance=instance, run_config=request.run_config,
            tags={**request.tags, "dagster/run_key": request.run_key, "dagster/sensor_name": "telemetry_configuration_sensor"},
        )
        assert result.success
        materializations = result.get_asset_materialization_events()
        assert len(materializations) == 1
        metadata = materializations[0].event_specific_data.materialization.metadata
        assert metadata["outcome"].value == outcome and metadata["signal"].value == signal
        assert metadata["entity_key"].value == "device:device-id"
        assert metadata["evaluation"].value["run_id"] == result.run_id
        check = result.get_asset_check_evaluations()[0]
        assert check.passed == passed and check.severity == dagster_module.AssetCheckSeverity.WARN
        context.instance = instance
        assert isinstance(sensor_module.telemetry_configuration_sensor._raw_fn(context), dagster_module.SkipReason)
        assert json.loads(context.update_cursor.call_args.args[0])["pending"] == []


@pytest.mark.parametrize("failure", ["provider", "source", "persistence", "commit"])
def test_job_failure_keeps_entity_pending_without_materialization(evaluation, failure):
    context, evaluator, detected, _, _, connection = evaluation
    request = sensor_module.telemetry_configuration_sensor._raw_fn(context)
    context.cursor = context.update_cursor.call_args.args[0]
    if failure == "provider":
        evaluator.evaluate.side_effect = TimeoutError("Synthetic provider failure")
    elif failure == "source":
        asset_module._snapshot.side_effect = RuntimeError("Synthetic source failure")
    elif failure == "commit":
        connection.__exit__.side_effect = RuntimeError("Synthetic commit failure")
    else:
        detected.side_effect = RuntimeError("Synthetic persistence failure")
    definitions = dagster_module.Definitions(
        assets=[asset_module.telemetry_configuration], jobs=[sensor_module.telemetry_configuration_job],
        resources={key: dagster_module.ResourceDefinition.hardcoded_resource(value)
                   for key, value in vars(context.resources).items()},
    )
    with dagster_module.DagsterInstance.ephemeral() as instance:
        result = definitions.resolve_job_def("telemetry_configuration_job").execute_in_process(
            instance=instance, run_config=request.run_config, raise_on_error=False,
            tags={"dagster/run_key": request.run_key, "dagster/sensor_name": "telemetry_configuration_sensor"},
        )
        assert not result.success and not result.get_asset_materialization_events()
        context.instance = instance
        assert isinstance(sensor_module.telemetry_configuration_sensor._raw_fn(context), dagster_module.SkipReason)
        state = json.loads(context.update_cursor.call_args.args[0])
        assert state["pending"] == [[sensor_module._DEVICE_TYPE, "device:device-id"]]
        assert "in_flight" not in state


@pytest.mark.parametrize("change", ["disabled", "config", "rule", "policy", "missing_entity"])
def test_job_revalidates_request_before_evaluation(evaluation, snapshot, change):
    context, evaluator, detected, cleared, config, _ = evaluation
    if change == "disabled":
        config["rules"][(sensor_module._DEVICE_TYPE, None)]["is_enabled"] = False
    elif change == "config":
        config["version_token"] = "2"
    elif change == "rule":
        config["rules"][(sensor_module._DEVICE_TYPE, None)]["rule_version"] = b"2"
    elif change == "policy":
        evaluator.policy = "policy-2"
    else:
        snapshot["devices"] = []
        snapshot["sensors"] = []
    with pytest.raises(dagster_module.Failure):
        asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert not evaluator.evaluate.called and not detected.called and not cleared.called


def test_changed_rule_without_config_version_change_prevents_write(evaluation):
    context, _, detected, cleared, config, _ = evaluation
    changed = copy.deepcopy(config)
    changed["rules"][(sensor_module._DEVICE_TYPE, None)]["rule_version"] = b"2"
    asset_module._load_active_config.side_effect = [(config, None), (changed, None)]
    with pytest.raises(dagster_module.Failure):
        asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert not detected.called and not cleared.called


def test_incomplete_evidence_materializes_without_signal(evaluation, snapshot):
    context, evaluator, detected, cleared, _, _ = evaluation
    snapshot["sensors"] = []
    result = asset_module.telemetry_configuration.op.compute_fn.decorated_fn(context)
    assert result.metadata["outcome"] == "inconclusive"
    assert result.metadata["evidence_complete"] is False
    assert not result.check_results[0].passed
    assert not evaluator.evaluate.called and not detected.called and not cleared.called


def test_sensor_inventory_matches_existing_candidate_eligibility(monkeypatch, evaluation, snapshot):
    context, _, _, _, config, _ = evaluation
    rule = config["rules"][(sensor_module._DEVICE_TYPE, None)]
    config["rules"][(sensor_module._ALERT_TYPE, None)] = dict(rule, alert_type=sensor_module._ALERT_TYPE)
    snapshot["devices"][0]["Active"] = False
    snapshot["devices"].append(dict(snapshot["devices"][0], DeviceId="inactive-without-sensors"))
    snapshot["sensors"].append(dict(snapshot["sensors"][0], DeviceId="orphan-device"))
    snapshot["sensors"].append(dict(snapshot["sensors"][0], DeviceId="inactive-sensor", Active=False))
    snapshot["alerts"].append(dict(snapshot["alerts"][0], AlertId="inactive-alert", Active=False))
    snapshot["conditions"].append(dict(snapshot["conditions"][0], AlertId="missing-alert", AlertConditionId="orphan-condition"))
    snapshot["conditions"].append(dict(snapshot["conditions"][0], AlertId="missing-alert", AlertConditionId="inactive-condition", Active=False))
    expected = set(transformation_module._candidates(snapshot))
    monkeypatch.setattr(transformation_module, "_candidates", Mock(side_effect=AssertionError("Sensor must not evaluate evidence")))
    sensor_module.telemetry_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert {tuple(key) for key in state["pending"]} == expected


def test_entity_inventory_reads_identifiers_without_warehouse_evidence(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.description = []
    cursor.fetchall.return_value = []
    monkeypatch.setattr(transformation_module.pyodbc, "connect", Mock(return_value=connection))
    source = SimpleNamespace(host="localhost", port=1433, database="synthetic", user="test", password="fake", connect_timeout=10)
    warehouse = Mock()
    snapshot = transformation_module._snapshot(source, warehouse, entities_only=True)
    assert set(snapshot) == {"devices", "sensors", "alerts", "conditions"}
    assert cursor.execute.call_count == 4
    assert all("Name" not in call.args[0] and "Value" not in call.args[0] for call in cursor.execute.call_args_list)
    warehouse._get_connection.assert_not_called()
    connection.close.assert_called_once()
