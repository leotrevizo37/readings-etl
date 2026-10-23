import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]


def _install_stubs() -> None:
    if "pyodbc" not in sys.modules:
        pyodbc = types.ModuleType("pyodbc")
        pyodbc.Connection = object
        pyodbc.Error = Exception
        sys.modules["pyodbc"] = pyodbc

    dagster = sys.modules.get("dagster")
    if dagster is None:
        dagster = types.ModuleType("dagster")
        sys.modules["dagster"] = dagster
    if not hasattr(dagster, "resource"):
        def resource(fn=None, **_kwargs):
            return fn if fn else lambda wrapped: wrapped
        dagster.resource = resource


def _load_sql_server_module():
    _install_stubs()
    path = PROJECT_ROOT / "src" / "projectA" / "defs" / "resources" / "sql_server.py"
    spec = importlib.util.spec_from_file_location("projectA_sql_server_resource", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FactReadingsMergeSqlTests(unittest.TestCase):
    def test_merge_sql_is_idempotent_by_operation_id(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_fact_readings_measurement_merge_sql()

        self.assertIn("MERGE dwh.factReadingsMeasurement", sql)
        self.assertIn("FROM stg.factReadingsMeasurement", sql)
        self.assertIn("WHERE RunId = ?", sql)
        self.assertIn("target.OperationId = source.OperationId", sql)
        self.assertIn("target.SensorId = source.SensorId", sql)
        self.assertIn("target.TimeSpan = source.TimeSpan", sql)
        self.assertIn("source.TimeSpan", sql)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", sql)
        self.assertIn("target.[Min] = source.[Min]", sql)
        self.assertIn("target.[Max] = source.[Max]", sql)
        self.assertIn("target.SensorName = source.SensorName", sql)
        self.assertIn("target.Anomalies = source.Anomalies", sql)
        self.assertIn("target.HasAnomalies = source.HasAnomalies", sql)
        self.assertIn(
            "target.CorrectReadingsJson = source.CorrectReadingsJson",
            sql,
        )
        self.assertIn(
            "target.AnomalousReadingsJson = source.AnomalousReadingsJson",
            sql,
        )
        self.assertIn("WHEN MATCHED THEN", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)

    def test_global_ddl_adds_measurement_detail_columns_and_index(self) -> None:
        sql = (
            REPOSITORY_ROOT
            / "sql"
            / "init"
            / "011_dwh_stg_factReadingsMeasurement.sql"
        ).read_text()

        self.assertEqual(sql.count("[Min] DECIMAL(9, 4) NULL"), 4)
        self.assertEqual(sql.count("[Max] DECIMAL(9, 4) NULL"), 4)
        self.assertEqual(sql.count("SensorName NVARCHAR(255) NULL"), 4)
        self.assertEqual(sql.count("HasAnomalies BIT NULL"), 4)
        self.assertEqual(sql.count("CorrectReadingsJson NVARCHAR(MAX) NULL"), 4)
        self.assertEqual(sql.count("AnomalousReadingsJson NVARCHAR(MAX) NULL"), 4)
        self.assertIn(
            "CREATE INDEX ix_factReadingsMeasurement_hasAnomalies",
            sql,
        )
        self.assertIn("HasAnomalies,", sql)

    def test_readings_serving_views_are_symmetric_and_weekly_is_indexed(self) -> None:
        hourly = (
            REPOSITORY_ROOT
            / "sql"
            / "init"
            / "018_serving_vwSensorReadingsHourly.sql"
        ).read_text()
        aggregates = (
            REPOSITORY_ROOT
            / "sql"
            / "init"
            / "019_serving_vwSensorReadingsDaily.sql"
        ).read_text()

        self.assertIn("Anomalies AS AnomaliesSum", hourly)
        self.assertIn("dwh.vwFactReadingsMeasurementWeekly", aggregates)
        self.assertIn("WITH SCHEMABINDING", aggregates)
        self.assertIn(
            "CREATE UNIQUE CLUSTERED INDEX "
            "ix_vwFactReadingsMeasurementWeekly_SensorId_WeekStartDate",
            aggregates,
        )
        self.assertIn("serving.vwSensorReadingsWeekly", aggregates)
        self.assertIn("DATEADD(DAY, 6, WeekStartDate) AS WeekEndDate", aggregates)
        self.assertEqual(aggregates.count("WITH (NOEXPAND)"), 2)

if __name__ == "__main__":
    unittest.main()
