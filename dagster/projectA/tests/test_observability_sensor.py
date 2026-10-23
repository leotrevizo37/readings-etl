import importlib
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

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
    importlib.import_module("dagster")
    sensor_module = importlib.import_module(
        "projectA.defs.sensors.observability_dim_aspnet_users_sensor"
    )
    schedule_module = importlib.import_module(
        "projectA.defs.schedules.observability_telemetry_user_usage_schedule"
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path

observability_dimaspnetusers_sensor = (
    sensor_module.observability_dimaspnetusers_sensor
)
observability_telemetry_user_usage_daily = (
    schedule_module.observability_telemetry_user_usage_daily
)


class DimSqlResourceStub:
    def __init__(self, watermark):
        self.watermark = watermark
        self.tables = None

    def _get_max_modified_at(self, tables):
        self.tables = tables
        return self.watermark


class ObservabilitySensorTests(unittest.TestCase):
    def test_sensor_bootstraps_and_tracks_aspnetusers_watermark(self) -> None:
        watermark = datetime(2026, 7, 29, 12, 30)
        resource = DimSqlResourceStub(watermark)
        context = SimpleNamespace(
            resources=SimpleNamespace(dim_sqlserver=resource),
            cursor=None,
            update_cursor=MagicMock(),
            log=MagicMock(),
        )

        result = observability_dimaspnetusers_sensor._raw_fn(context)

        self.assertEqual(resource.tables, ["AspNetUsers"])
        self.assertEqual(
            result.run_key,
            "bootstrap:2026-07-29T12:30:00",
        )
        context.update_cursor.assert_called_once_with(
            "2026-07-29T12:30:00"
        )

    def test_sensor_skips_unchanged_watermark(self) -> None:
        watermark = datetime(2026, 7, 29, 12, 30)
        context = SimpleNamespace(
            resources=SimpleNamespace(
                dim_sqlserver=DimSqlResourceStub(watermark)
            ),
            cursor="2026-07-29T12:30:00",
            update_cursor=MagicMock(),
            log=MagicMock(),
        )

        result = observability_dimaspnetusers_sensor._raw_fn(context)

        self.assertIn("No AspNetUsers changes detected", result.skip_message)
        context.update_cursor.assert_not_called()

    def test_sensor_runs_when_watermark_changes(self) -> None:
        watermark = datetime(2026, 7, 29, 13, 30)
        context = SimpleNamespace(
            resources=SimpleNamespace(
                dim_sqlserver=DimSqlResourceStub(watermark)
            ),
            cursor="2026-07-29T12:30:00",
            update_cursor=MagicMock(),
            log=MagicMock(),
        )

        result = observability_dimaspnetusers_sensor._raw_fn(context)

        self.assertEqual(
            result.run_key,
            "2026-07-29T13:30:00",
        )
        context.update_cursor.assert_called_once_with(
            "2026-07-29T13:30:00"
        )

    def test_fact_schedule_is_daily_at_0130(self) -> None:
        self.assertEqual(
            observability_telemetry_user_usage_daily.name,
            "121_observability_telemetry_user_usage_daily",
        )
        self.assertEqual(
            observability_telemetry_user_usage_daily.cron_schedule,
            "30 1 * * *",
        )
        self.assertEqual(
            observability_telemetry_user_usage_daily.execution_timezone,
            "America/Ciudad_Juarez",
        )


if __name__ == "__main__":
    unittest.main()
