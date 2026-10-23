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
    path = PROJECT_ROOT / "src" / "projectA" / "defs" / "resources" / "sql_server.py"
    spec = importlib.util.spec_from_file_location("projectA_sql_server_resource", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class DimTelemetryProdDimensionsMergeSqlTests(unittest.TestCase):
    def test_merge_sql_upserts_and_deactivates_by_sensor_id(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_dim_telemetry_prod_dimensions_merge_sql()

        self.assertIn("MERGE dwh.dimTelemetryProdDimensions", sql)
        self.assertIn("FROM stg.dimTelemetryProdDimensions", sql)
        self.assertIn("WHERE RunId = ?", sql)
        self.assertIn("target.SensorId = source.SensorId", sql)
        self.assertIn("WHEN MATCHED AND EXISTS", sql)
        self.assertIn("EXCEPT", sql)
        self.assertIn("target.Active = source.Active", sql)
        self.assertIn("target.PhysicalIdentifier = source.PhysicalIdentifier", sql)
        self.assertIn("target.PerEvent = source.PerEvent", sql)
        self.assertIn("target.ModelId = source.ModelId", sql)
        self.assertIn("target.SensorHubId = source.SensorHubId", sql)
        self.assertIn("target.model_name = source.model_name", sql)
        self.assertIn("target.hub_model_name = source.hub_model_name", sql)
        self.assertIn("target.hub_name = source.hub_name", sql)
        self.assertIn("target.TimeZone = source.TimeZone", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)
        self.assertIn("WHEN NOT MATCHED BY SOURCE AND target.Active = 1 THEN", sql)
        self.assertIn("target.Active = 0", sql)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", sql)
        self.assertNotIn("DELETE", sql.upper())


if __name__ == "__main__":
    unittest.main()
