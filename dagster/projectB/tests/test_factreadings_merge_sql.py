import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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
    path = PROJECT_ROOT / "src" / "projectB" / "defs" / "resources" / "sql_server.py"
    spec = importlib.util.spec_from_file_location("projectB_sql_server_resource", path)
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


if __name__ == "__main__":
    unittest.main()
