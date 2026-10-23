import json
from contextlib import closing
from datetime import datetime, timezone

from dagster import DagsterRunStatus, DefaultSensorStatus, RunRequest, RunsFilter, SkipReason, sensor

from ..jobs.telemetry_configuration_job import telemetry_configuration_job
from ..transformations.telemetry_configuration import _ALERT_TYPE, _DEVICE_TYPE, _fingerprint, _snapshot
from .operational_alerts_sensor import _is_due, _load_active_config, _mark_checked, _select_rule


@sensor(
    job=telemetry_configuration_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    required_resource_keys={"sqlserver", "dim_sqlserver", "telemetry_configuration_ai"},
)
def telemetry_configuration_sensor(context):
    warehouse = context.resources.sqlserver
    source = context.resources.dim_sqlserver
    evaluator = context.resources.telemetry_configuration_ai
    now = datetime.now(timezone.utc)
    state = json.loads(context.cursor) if context.cursor else {}
    in_flight = state.get("in_flight")
    if in_flight:
        runs = context.instance.get_runs(filters=RunsFilter(
            job_name=telemetry_configuration_job.name,
            tags={"user_code": "projectB", "dagster/sensor_name": "telemetry_configuration_sensor", "dagster/run_key": in_flight["run_key"]},
        ), limit=1)
        if not runs:
            return RunRequest(
                run_key=in_flight["run_key"],
                run_config={"ops": {"projectB__telemetry_configuration": {"config": in_flight["config"]}}},
                tags={"alert_type": in_flight["config"]["alert_type"], "entity_key": in_flight["config"]["entity_key"]},
            )
        if not runs[0].is_finished:
            return SkipReason("La evaluación TELEMETRY anterior sigue pendiente o en ejecución.")
        if runs[0].status == DagsterRunStatus.SUCCESS:
            state["pending"].remove([in_flight["config"]["alert_type"], in_flight["config"]["entity_key"]])
        del state["in_flight"]
        context.update_cursor(json.dumps(state))
    checks = state.get("checks", {})
    with closing(warehouse._get_connection()) as connection:
        config, reason = _load_active_config(connection)
    if config is None:
        return SkipReason(reason)
    if not any((kind, None) in config["rules"] for kind in (_DEVICE_TYPE, _ALERT_TYPE)):
        return SkipReason("No hay reglas de configuración TELEMETRY habilitadas.")
    watermark = _snapshot(source, warehouse, watermark_only=True)
    evaluation_version = _fingerprint({
        "config": config["version_token"], "policy": evaluator.policy,
        "rules": sorted(
            (kind, entity_key or "", bytes(rule["rule_version"]).hex())
            for (kind, entity_key), rule in config["rules"].items()
            if kind in (_DEVICE_TYPE, _ALERT_TYPE)
        ),
    })
    if state.get("watermark") != watermark or state.get("evaluation_version") != evaluation_version:
        state = {"watermark": watermark, "evaluation_version": evaluation_version, "attempt": state.get("attempt", 0)}
        checks = {}
    if state.get("pending") == []:
        return SkipReason("No hay cambios en la configuración TELEMETRY.")
    if "pending" in state and not any(
        _is_due(config, _select_rule(config, kind, entity_key), entity_key, checks, now)
        for kind, entity_key in state["pending"]
    ):
        return SkipReason("Las configuraciones TELEMETRY pendientes esperan su intervalo de evaluación.")
    entities = _snapshot(source, warehouse, entities_only=True)
    device_ids = {str(row["DeviceId"]) for row in entities["devices"] if row["Active"]}
    device_ids.update(str(row["DeviceId"]) for row in entities["sensors"])
    alert_ids = {str(row["AlertId"]) for row in entities["alerts"]}
    candidates = [(_DEVICE_TYPE, f"device:{device_id}") for device_id in sorted(device_ids)]
    candidates.extend((_ALERT_TYPE, f"condition:{row['AlertConditionId']}") for row in entities["conditions"] if str(row["AlertId"]) not in alert_ids)
    candidates.extend((_ALERT_TYPE, f"alert:{row['AlertId']}") for row in entities["alerts"] if row["Active"])
    candidate_keys = {"::".join(item) for item in candidates}
    checks = {key: value for key, value in checks.items() if key in candidate_keys}
    state["checks"] = checks
    state["pending"] = [
        [kind, entity_key] for kind, entity_key in state.get("pending", candidates)
        if (kind, entity_key) in candidates and (kind, None) in config["rules"]
        and _select_rule(config, kind, entity_key)["is_enabled"]
    ]
    context.update_cursor(json.dumps(state))
    pending = sorted(state["pending"], key=lambda key: checks.get("::".join(key), {}).get("at", 0))
    for kind, entity_key in pending:
        if (kind, None) not in config["rules"]:
            continue
        rule = _select_rule(config, kind, entity_key)
        if not rule["is_enabled"] or not _is_due(config, rule, entity_key, checks, now):
            continue
        _mark_checked(config, rule, entity_key, checks, now)
        state["attempt"] = state.get("attempt", 0) + 1
        run_key = _fingerprint([watermark, evaluation_version, kind, entity_key, state["attempt"]])
        run_config = {
            "alert_type": kind, "entity_key": entity_key, "config_version": config["version_token"],
            "rule_version": bytes(rule["rule_version"]).hex(), "policy": evaluator.policy,
        }
        state["in_flight"] = {"run_key": run_key, "config": run_config}
        context.update_cursor(json.dumps(state))
        return RunRequest(
            run_key=run_key,
            run_config={"ops": {"projectB__telemetry_configuration": {"config": run_config}}},
            tags={"alert_type": kind, "entity_key": entity_key},
        )
    return SkipReason("No hay configuraciones TELEMETRY pendientes de evaluación.")
