import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parents[1]


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
    path = (
        PROJECT_ROOT
        / "src"
        / "projectA"
        / "defs"
        / "resources"
        / "sql_server.py"
    )
    spec = importlib.util.spec_from_file_location(
        "projectA_observability_sql_server_resource",
        path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ObservabilityMergeSqlTests(unittest.TestCase):
    def test_dim_merge_uses_run_scoped_stage_and_updates_metadata(self) -> None:
        sql = _load_sql_server_module().build_dim_aspnet_users_merge_sql()

        self.assertIn("MERGE observability.dimAspNetUsers", sql)
        self.assertIn("FROM stg.dimAspNetUsers", sql)
        self.assertIn("WHERE RunId = ?", sql)
        self.assertIn("target.Id = source.Id", sql)
        self.assertIn("WHEN MATCHED AND EXISTS", sql)
        self.assertIn("EXCEPT", sql)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)
        self.assertNotIn("IngestionControl", sql)

    def test_fact_merge_is_idempotent_by_date_and_user(self) -> None:
        sql = _load_sql_server_module().build_fact_telemetry_user_usage_merge_sql()

        self.assertIn("MERGE observability.factTelemetryUserUsage", sql)
        self.assertIn("FROM stg.factTelemetryUserUsage", sql)
        self.assertIn("WHERE RunId = ?", sql)
        self.assertIn("target.[Date] = source.[Date]", sql)
        self.assertIn("target.UserId = source.UserId", sql)
        self.assertIn("AvgLatency", sql)
        self.assertIn("Latency95thPercentile", sql)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", sql)
        self.assertIn("WHEN MATCHED AND EXISTS", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)

    def test_ddl_contains_only_allowed_user_dimension_columns(self) -> None:
        dim_path = (
            REPO_ROOT
            / "sql"
            / "init"
            / "015_observability_dimAspNetUsers.sql"
        )
        fact_path = (
            REPO_ROOT
            / "sql"
            / "init"
            / "016_observability_factTelemetryUserUsage.sql"
        )
        if not dim_path.exists() or not fact_path.exists():
            self.skipTest("Repository SQL files are not mounted.")
        dim_sql = dim_path.read_text(encoding="utf-8")
        fact_sql = fact_path.read_text(encoding="utf-8")
        dim_stage_sql = dim_sql.split(
            "CREATE TABLE stg.dimAspNetUsers",
            1,
        )[1]
        fact_stage_sql = fact_sql.split(
            "CREATE TABLE stg.factTelemetryUserUsage",
            1,
        )[1]

        self.assertIn("CREATE TABLE observability.dimAspNetUsers", dim_sql)
        self.assertIn("CreatedAt DATETIME2(0)", dim_sql)
        self.assertIn("ModifiedAt DATETIME2(0)", dim_sql)
        self.assertNotIn("CreatedAt DATETIME2", dim_stage_sql)
        self.assertNotIn("ModifiedAt DATETIME2", dim_stage_sql)
        self.assertIn("UserId UNIQUEIDENTIFIER NOT NULL", fact_sql)
        self.assertIn("HasConnected BIT NOT NULL", fact_sql)
        self.assertIn("MadeCompleteInteraction BIT NOT NULL", fact_sql)
        self.assertIn("TimeConnected BIGINT NOT NULL", fact_sql)
        self.assertIn("AvgLatency BIGINT NOT NULL", fact_sql)
        self.assertIn("Latency95thPercentile DECIMAL(18, 2)", fact_sql)
        self.assertIn("ModifiedAt DATETIME2(0)", fact_sql)
        self.assertNotIn("ModifiedAt DATETIME2", fact_stage_sql)
        self.assertNotIn("IngestionControl", dim_sql)
        self.assertNotIn("IngestionControl", fact_sql)
        self.assertNotIn("LocationId", fact_sql)
        for forbidden_column in [
            "Email",
            "PasswordHash",
            "PhoneNumber",
            "SecurityStamp",
            "ConcurrencyStamp",
            "EmployeeNumber",
            "LockoutEnd",
        ]:
            self.assertNotIn(forbidden_column, dim_sql)


if __name__ == "__main__":
    unittest.main()
