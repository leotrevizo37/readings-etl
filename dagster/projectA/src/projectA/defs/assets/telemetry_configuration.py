import http.client
import json
from contextlib import closing
from datetime import datetime, timezone

from dagster import AssetCheckResult, AssetCheckSeverity, AssetCheckSpec, Failure, MaterializeResult, MetadataValue, asset

from ..sensors.operational_alerts_sensor import (
    _load_active_config,
    _record_cleared,
    _record_detected,
    _rows,
    _select_rule,
)
from ..transformations.telemetry_configuration import _ALERT_TYPE, _DEVICE_TYPE, _candidates, _fingerprint, _snapshot


@asset(
    name="telemetry_configuration",
    group_name="projectA",
    key_prefix=["projectA"],
    required_resource_keys={"sqlserver", "dim_sqlserver", "telemetry_configuration_ai"},
    config_schema={"alert_type": str, "entity_key": str, "config_version": str, "rule_version": str, "policy": str},
    check_specs=[AssetCheckSpec(name="configuration_consistent", asset=["projectA", "telemetry_configuration"])],
)
def telemetry_configuration(context):
    warehouse = context.resources.sqlserver
    source = context.resources.dim_sqlserver
    evaluator = context.resources.telemetry_configuration_ai
    requested = context.op_config
    kind, entity_key = requested["alert_type"], requested["entity_key"]
    now = datetime.now(timezone.utc)
    run_metadata = {"alert_type": kind, "entity_key": entity_key, "run_id": context.run_id}
    with closing(warehouse._get_connection()) as connection:
        config, reason = _load_active_config(connection)
    if config is None:
        raise Failure(reason, metadata=run_metadata)
    if kind not in (_DEVICE_TYPE, _ALERT_TYPE) or (kind, None) not in config["rules"]:
        raise Failure("No hay una regla de configuración TELEMETRY aplicable.", metadata=run_metadata)
    rule = _select_rule(config, kind, entity_key)
    if (
        not rule["is_enabled"] or config["version_token"] != requested["config_version"]
        or bytes(rule["rule_version"]).hex() != requested["rule_version"]
        or evaluator.policy != requested["policy"]
    ):
        raise Failure("La configuración del detector cambió antes de la evaluación.", metadata=run_metadata)
    candidate = _candidates(_snapshot(source, warehouse)).get((kind, entity_key))
    if candidate is None:
        raise Failure("La entidad TELEMETRY ya no es evaluable.", metadata=run_metadata)
    if candidate["problems"]:
        assessment = {"status": "suspicious", "confidence": 1, "reasons": candidate["problems"], "model": "structural"}
    elif not candidate["complete"]:
        assessment = {
            "status": "insufficient_evidence", "confidence": 0,
            "reasons": ["Evidencia insuficiente para evaluar la configuración TELEMETRY."], "model": "not_evaluated",
        }
    else:
        try:
            assessment = evaluator.evaluate(candidate["payload"])
        except (ValueError, OSError, http.client.HTTPException) as exc:
            raise Failure("Evaluación TELEMETRY AI no disponible o inválida; no se emiten señales.", metadata=run_metadata) from exc
    warning = (assessment["status"] == "suspicious" and assessment["confidence"] < 0.9
               or assessment["status"] == "insufficient_evidence" and assessment["confidence"] >= 0.9)
    fingerprint = _fingerprint(candidate)
    metadata = {
        "source_fingerprint": fingerprint, "policy": evaluator.policy,
        "assessment": assessment, "evidence": candidate, **run_metadata,
        "config_version": config["version_token"], "rule_version": bytes(rule["rule_version"]).hex(),
        "alert_rule_id": str(rule["alert_rule_id"]),
    }
    outcome, signal = "inconclusive", "NONE"
    severity = rule["severity"]
    if warning or (assessment["status"] != "insufficient_evidence" and assessment["confidence"] >= 0.9):
        fresh = _candidates(_snapshot(source, warehouse)).get((kind, entity_key))
        if fresh != candidate:
            raise Failure("La configuración TELEMETRY cambió durante la evaluación.", metadata=run_metadata)
        with closing(warehouse._get_connection()) as connection, connection:
            current, reason = _load_active_config(connection)
            if (
                current is None or current["version_token"] != config["version_token"]
                or (kind, None) not in current["rules"]
                or _select_rule(current, kind, entity_key) != rule
            ):
                raise Failure(reason or "La configuración del detector cambió.", metadata=run_metadata)
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT alert_id, detected_at, metadata_json
                    FROM alerting.alert_incident
                    WHERE user_code = N'projectA' AND alert_type_id = ?
                        AND entity_key = ? AND cleared_at IS NULL
                        AND is_point_in_time = 0
                """, rule["alert_type_id"], entity_key)
                incidents = _rows(cursor)
            if assessment["status"] == "suspicious" or warning:
                severity_id = rule["severity_id"]
                if warning and rule["severity"] != "WARNING":
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT severity_id FROM alerting.dim_severity WHERE code = N'WARNING'")
                        severity_id = _rows(cursor)[0]["severity_id"]
                inserted = _record_detected(
                    connection, rule, "sensor_monitoring", entity_key, entity_key,
                    now, "; ".join(assessment["reasons"]), metadata, severity_id=severity_id,
                )
                outcome, signal = ("detected", "DETECTED") if inserted else ("deduplicated", "NONE")
                severity = "WARNING" if warning else severity
            else:
                outcome = "recovery_not_confirmed" if incidents else "consistent"
                metadata["recovery_conditions"] = {
                    "evidence_complete": bool(candidate["complete"]), "open_incident": bool(incidents),
                    "expected_ranges_available": all(
                        sensor["expected_ranges"] for device in candidate["payload"].get("devices", [])
                        for condition in device["conditions"] for sensor in condition["sensors"]
                    ),
                }
                if candidate["complete"] and incidents:
                    incident = incidents[0]
                    previous = json.loads(incident["metadata_json"])
                    metadata["recovery_conditions"].update(
                        changed_evidence=bool(previous.get("source_fingerprint") and previous["source_fingerprint"] != fingerprint),
                        same_policy=previous.get("policy") == evaluator.policy,
                        compatible_model=previous.get("assessment", {}).get("model") in ("structural", assessment["model"]),
                    )
                    if all(metadata["recovery_conditions"][key] for key in (
                        "changed_evidence", "same_policy", "compatible_model",
                    )):
                        cleared = _record_cleared(
                            connection, incident, now, "; ".join(assessment["reasons"]), metadata,
                        )
                        outcome, signal = ("cleared", "CLEARED") if cleared else ("deduplicated", "NONE")
    return MaterializeResult(
        metadata={
            **run_metadata, "outcome": outcome, "signal": signal, "severity": severity,
            "assessment_status": assessment["status"], "confidence": assessment["confidence"],
            "model": assessment["model"], "source_fingerprint": fingerprint,
            "evidence_complete": bool(candidate["complete"]),
            "evaluation": MetadataValue.json(json.loads(json.dumps(metadata, ensure_ascii=False, default=str))),
        },
        check_results=[AssetCheckResult(
            check_name="configuration_consistent", severity=AssetCheckSeverity.WARN,
            passed=assessment["status"] == "consistent" and assessment["confidence"] >= 0.9,
            metadata={**run_metadata, "reasons": assessment["reasons"], "outcome": outcome},
        )],
    )
