import ast
import unittest
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
JOB_FILE = PACKAGE_ROOT / "src" / "projectB" / "defs" / "jobs" / "index_maintenance_job.py"
SCHEDULE_FILE = PACKAGE_ROOT / "src" / "projectB" / "defs" / "schedules" / "index_maintenance_schedule.py"


def _assignment_value(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                return ast.literal_eval(node.value)
    raise AssertionError(f"{name} assignment not found in {path}")


def _schedule_keyword(path: Path, keyword: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "index_maintenance_schedule" for target in node.targets):
            continue
        for kwarg in node.value.keywords:
            if kwarg.arg == keyword:
                return kwarg.value
    raise AssertionError(f"{keyword} keyword not found in {path}")


class IndexMaintenanceTests(unittest.TestCase):
    def test_procedure_call_uses_conservative_defaults(self) -> None:
        query = _assignment_value(JOB_FILE, "INDEX_MAINTENANCE_QUERY")
        defaults = _assignment_value(JOB_FILE, "INDEX_MAINTENANCE_DEFAULTS")

        self.assertIn("EXEC ctl.usp_IndexMaintenance_Run", query)
        self.assertIsNone(defaults["schema_name"])
        self.assertEqual(defaults["min_page_count"], 1000)
        self.assertEqual(defaults["reorganize_threshold"], 30.0)
        self.assertEqual(defaults["rebuild_threshold"], 80.0)
        self.assertEqual(defaults["max_dop"], 1)
        self.assertEqual(defaults["use_online_rebuild"], 0)

    def test_schedule_runs_at_expected_times(self) -> None:
        cron = _schedule_keyword(SCHEDULE_FILE, "cron_schedule")
        timezone = _schedule_keyword(SCHEDULE_FILE, "execution_timezone")
        status = _schedule_keyword(SCHEDULE_FILE, "default_status")

        self.assertEqual(ast.literal_eval(cron), ["30 5 * * 0", "0 2 * * 3"])
        self.assertEqual(ast.literal_eval(timezone), "America/Ciudad_Juarez")
        self.assertEqual(status.attr, "RUNNING")


if __name__ == "__main__":
    unittest.main()
