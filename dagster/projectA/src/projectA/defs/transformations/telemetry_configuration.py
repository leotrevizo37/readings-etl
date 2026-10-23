import hashlib
import json
import math
from contextlib import closing

import pyodbc


_DEVICE_TYPE = "telemetry_device_configuration"
_ALERT_TYPE = "telemetry_alert_configuration"


def _fingerprint(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False,
    ).encode("utf-8")).hexdigest()


def _snapshot(source, warehouse, watermark_only=False, entities_only=False):
    from ..sensors.operational_alerts_sensor import _rows

    tables = {
        "devices": "SELECT DeviceId, DeviceTyId, Name, Active FROM dbo.Devices ORDER BY DeviceId",
        "device_types": "SELECT DeviceTyId, Name, Active FROM dbo.DeviceTys ORDER BY DeviceTyId",
        "sensors": "SELECT SensorId, DeviceId, SensorTyId, Name, Active FROM dbo.Sensors ORDER BY SensorId",
        "sensor_types": "SELECT SensorTyId, Name, UnitName, UnitAbbreviation, Active FROM dbo.SensorTys ORDER BY SensorTyId",
        "alerts": "SELECT AlertId, Name, DeviceTyId, Active FROM dbo.Alerts ORDER BY AlertId",
        "conditions": "SELECT AlertConditionId, AlertId, SensorTyId, Operator, Value, Active FROM dbo.AlertConditions ORDER BY AlertConditionId",
        "stopped": "SELECT AlertId, DeviceId FROM dbo.AlertStoppedOnDevices WHERE Active = 1 ORDER BY AlertId, DeviceId",
    }
    watermark_query = "SELECT CONVERT(VARCHAR(33), MAX(ModifiedAt), 126) AS modified_at, COUNT_BIG(*) AS row_count FROM {}"
    if entities_only:
        tables = {
            "devices": "SELECT DeviceId, Active FROM dbo.Devices ORDER BY DeviceId",
            "sensors": "SELECT DeviceId FROM dbo.Sensors WHERE Active = 1 ORDER BY SensorId",
            "alerts": "SELECT AlertId, Active FROM dbo.Alerts ORDER BY AlertId",
            "conditions": "SELECT AlertConditionId, AlertId FROM dbo.AlertConditions WHERE Active = 1 ORDER BY AlertConditionId",
        }
    elif watermark_only:
        tables = {
            name: watermark_query.format(table)
            for name, table in (
                ("devices", "dbo.Devices"),
                ("device_types", "dbo.DeviceTys"),
                ("sensors", "dbo.Sensors"),
                ("sensor_types", "dbo.SensorTys"),
                ("alerts", "dbo.Alerts"),
                ("conditions", "dbo.AlertConditions"),
                ("stopped", "dbo.AlertStoppedOnDevices"),
            )
        }
    snapshot = {}
    with closing(pyodbc.connect(
        driver="{ODBC Driver 18 for SQL Server}",
        server=f"{source.host},{source.port}", database=source.database,
        uid=source.user, pwd=source.password, timeout=source.connect_timeout,
        TrustServerCertificate="yes",
    )) as connection:
        connection.timeout = 20
        with connection.cursor() as cursor:
            for name, query in tables.items():
                cursor.execute(query)
                snapshot[name] = _rows(cursor)
    if entities_only:
        return snapshot
    with closing(warehouse._get_connection()) as connection:
        connection.timeout = 20
        with connection.cursor() as cursor:
            cursor.execute(watermark_query.format("dwh.dimSensorExpectedVal") if watermark_only else """
                SELECT SensorId, DeviceId, min_expected_value, max_expected_value,
                    StartTime, EndTime, scheduledDays
                FROM dwh.dimSensorExpectedVal
                ORDER BY SensorId, DeviceId, StartTime, scheduledDays
            """)
            snapshot["expected"] = _rows(cursor)
    return snapshot


def _candidates(snapshot):
    snapshot = {
        name: [{
            key: " ".join(value.split()) if isinstance(value, str)
            and key in ("Name", "UnitName", "UnitAbbreviation", "Operator", "scheduledDays") else value
            for key, value in row.items()
        } for row in rows]
        for name, rows in snapshot.items()
    }
    devices = {str(row["DeviceId"]): row for row in snapshot["devices"]}
    device_types = {str(row["DeviceTyId"]): row for row in snapshot["device_types"]}
    sensor_types = {str(row["SensorTyId"]): row for row in snapshot["sensor_types"]}
    sensors = {}
    expected = {}
    for row in snapshot["sensors"]:
        if row["Active"]:
            sensors.setdefault(str(row["DeviceId"]), []).append(row)
    for row in snapshot["expected"]:
        expected.setdefault((str(row["DeviceId"]), str(row["SensorId"])), []).append(row)
    candidates = {}
    for device_id in sorted(set(devices) | set(sensors)):
        device = devices.get(device_id)
        attached = sensors.get(device_id, [])
        if device and not device["Active"] and not attached:
            continue
        device_type = device_types.get(str(device["DeviceTyId"])) if device else None
        problems = []
        if not device or not device["Active"]:
            problems.append("Sensores activos asociados a un dispositivo inexistente o inactivo.")
        if device and (not device_type or not device_type["Active"]):
            problems.append("El tipo del dispositivo no existe o está inactivo.")
        payload = {
            "kind": "device_configuration",
            "device_id": device_id,
            "device": device["Name"] if device else None,
            "device_type": device_type["Name"] if device_type else None,
            "sensors": [],
        }
        complete = bool(attached and device and device["Name"] and device_type and device_type["Name"])
        for row in attached:
            sensor_type = sensor_types.get(str(row["SensorTyId"]))
            if not sensor_type or not sensor_type["Active"]:
                problems.append("Un sensor activo tiene un tipo inexistente o inactivo.")
            payload["sensors"].append({
                "sensor": row["Name"],
                "sensor_type": sensor_type["Name"] if sensor_type else None,
                "unit": sensor_type["UnitAbbreviation"] if sensor_type else None,
                "unit_name": sensor_type["UnitName"] if sensor_type else None,
            })
            complete = complete and bool(row["Name"] and sensor_type and sensor_type["Name"])
        candidates[(_DEVICE_TYPE, f"device:{device_id}")] = {
            "payload": payload, "problems": problems, "complete": complete,
            "source_ids": [device_id, *[str(row["SensorId"]) for row in attached]],
        }
    alerts = {str(row["AlertId"]): row for row in snapshot["alerts"]}
    conditions = {}
    for row in snapshot["conditions"]:
        if row["Active"]:
            alert_id = str(row["AlertId"])
            conditions.setdefault(alert_id, []).append(row)
            if alert_id not in alerts:
                candidates[(_ALERT_TYPE, f"condition:{row['AlertConditionId']}")] = {
                    "payload": {"kind": "alert_configuration"},
                    "problems": ["Una condición activa referencia una alerta inexistente."],
                    "complete": False, "source_ids": [str(row["AlertConditionId"])],
                }
    stopped = {(str(row["AlertId"]), str(row["DeviceId"])) for row in snapshot["stopped"]}
    for alert_id, alert in alerts.items():
        if not alert["Active"]:
            continue
        device_type = device_types.get(str(alert["DeviceTyId"]))
        active_conditions = conditions.get(alert_id, [])
        problems = []
        if not device_type or not device_type["Active"]:
            problems.append("La alerta tiene un tipo de dispositivo inexistente o inactivo.")
        if not active_conditions:
            problems.append("La alerta activa no tiene condiciones activas.")
        payload = {
            "kind": "alert_configuration",
            "alert_id": alert_id,
            "alert_name": alert["Name"],
            "device_type": device_type["Name"] if device_type else None,
            "devices": [],
        }
        source_ids = [alert_id, *[str(row["AlertConditionId"]) for row in active_conditions]]
        complete = bool(device_type and device_type["Name"] and active_conditions)
        for condition in active_conditions:
            sensor_type = sensor_types.get(str(condition["SensorTyId"]))
            if not sensor_type or not sensor_type["Active"]:
                problems.append("Una condición usa un tipo de sensor inexistente o inactivo.")
            if condition["Operator"] not in ("<", "<=", ">", ">=", "=", "==", "!=", "<>"):
                problems.append("Una condición tiene un operador ausente o no reconocido.")
            if condition["Value"] is None or not math.isfinite(float(condition["Value"])):
                problems.append("Una condición tiene un umbral ausente o no finito.")
        for device_id, device in devices.items():
            if (
                not device["Active"] or device["DeviceTyId"] != alert["DeviceTyId"]
                or (alert_id, device_id) in stopped
            ):
                continue
            source_ids.append(device_id)
            evaluation = {"device": device["Name"], "conditions": []}
            for condition in active_conditions:
                sensor_type = sensor_types.get(str(condition["SensorTyId"]))
                matching = [row for row in sensors.get(device_id, []) if row["SensorTyId"] == condition["SensorTyId"]]
                if not matching:
                    problems.append("Una condición no tiene sensores activos de su tipo en un dispositivo aplicable.")
                item = {
                    "alert_condition_id": str(condition["AlertConditionId"]),
                    "sensor_type": sensor_type["Name"] if sensor_type else None,
                    "unit": sensor_type["UnitAbbreviation"] if sensor_type else None,
                    "unit_name": sensor_type["UnitName"] if sensor_type else None,
                    "operator": condition["Operator"],
                    "threshold": (
                        condition["Value"] if condition["Value"] is not None
                        and math.isfinite(float(condition["Value"])) else None
                    ), "sensors": [],
                }
                complete = complete and bool(sensor_type and sensor_type["Name"] and sensor_type["UnitAbbreviation"])
                for row in matching:
                    sensor_id = str(row["SensorId"])
                    source_ids.append(sensor_id)
                    ranges = []
                    for reference in expected.get((device_id, sensor_id), []):
                        lower = reference["min_expected_value"]
                        upper = reference["max_expected_value"]
                        if lower is not None and upper is not None and lower > upper:
                            problems.append("Un rango esperado tiene mínimo mayor que máximo.")
                        ranges.append({
                            "min": float(lower) if lower is not None else None,
                            "max": float(upper) if upper is not None else None,
                            "start": str(reference["StartTime"]),
                            "end": str(reference["EndTime"]),
                            "days": reference["scheduledDays"],
                        })
                        complete = complete and bool(
                            (lower is not None or upper is not None)
                            and reference["StartTime"] is not None
                            and reference["EndTime"] is not None
                            and reference["scheduledDays"]
                        )
                    item["sensors"].append({"sensor": row["Name"], "expected_ranges": ranges})
                evaluation["conditions"].append(item)
            payload["devices"].append(evaluation)
        candidates[(_ALERT_TYPE, f"alert:{alert_id}")] = {
            "payload": payload, "problems": sorted(set(problems)),
            "complete": complete and bool(payload["devices"]), "source_ids": source_ids,
        }
    return candidates
