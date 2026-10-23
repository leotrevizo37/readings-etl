import importlib
import io
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import yaml

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
    availability_module = importlib.import_module(
        "projectA.defs.assets.observability_factUrlAvailabilityDaily"
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


class SqlServerStub:
    def __init__(self):
        self.watermark = datetime(2026, 8, 2)
        self.started = []
        self.completed = []
        self.queries = []

    def get_watermark(self, ingestion_name):
        return self.watermark

    def mark_run_started(self, ingestion_name):
        self.started.append(ingestion_name)

    def execute_query(self, query, *args):
        self.queries.append((query, *args))
        return 1

    def mark_run_completed(self, **kwargs):
        self.completed.append(kwargs)


class UrlAvailabilityDailyTests(unittest.TestCase):
    def test_aggregates_daily_probe_metrics(self) -> None:
        timestamp = datetime(
            2026,
            8,
            3,
            tzinfo=timezone.utc,
        ).timestamp()
        payloads = {
            "probe_success": {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "instance": "https://example.test/"
                            },
                            "values": [
                                [timestamp, "1"],
                                [timestamp + 60, "1"],
                                [timestamp + 120, "0"],
                            ],
                        }
                    ],
                },
            },
            "probe_duration_seconds": {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "instance": "https://example.test/"
                            },
                            "values": [
                                [timestamp, "0.1"],
                                [timestamp + 60, "0.2"],
                                [timestamp + 120, "9.6"],
                            ],
                        }
                    ],
                },
            },
        }
        targets_payload = {
            "status": "success",
            "data": {
                "activeTargets": [
                    {
                        "labels": {
                            "job": "blackbox_probe",
                            "user_code": "projectA",
                            "instance": "https://example.test/",
                        }
                    }
                ]
            },
        }

        def fake_urlopen(request_url, timeout):
            parsed_url = urlparse(request_url)
            if parsed_url.path.endswith("/targets"):
                return io.BytesIO(json.dumps(targets_payload).encode())
            query = parse_qs(parsed_url.query)["query"][0]
            self.assertIn('job="blackbox_probe"', query)
            self.assertIn('user_code="projectA"', query)
            metric_name = query.split("{")[0]
            return io.BytesIO(
                json.dumps(payloads[metric_name]).encode()
            )

        sqlserver = SqlServerStub()
        context = SimpleNamespace(
            resources=SimpleNamespace(sqlserver=sqlserver)
        )
        with patch.object(
            availability_module,
            "urlopen",
            side_effect=fake_urlopen,
        ):
            availability_module.observability_facturlavailabilitydaily.op.compute_fn.decorated_fn(
                context
            )

        self.assertEqual(
            sqlserver.started,
            ["factUrlAvailabilityDaily"],
        )
        self.assertEqual(len(sqlserver.queries), 1)
        self.assertEqual(
            sqlserver.queries[0][1:],
            (
                "https://example.test/",
                datetime(2026, 8, 3).date(),
                66.6667,
                3.3,
                9.6,
                False,
                True,
            ),
        )
        self.assertTrue(sqlserver.completed[-1]["succeeded"])

    def test_empty_prometheus_result_succeeds_without_rows(self) -> None:
        payload = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [],
            },
        }
        targets_payload = {
            "status": "success",
            "data": {"activeTargets": []},
        }
        sqlserver = SqlServerStub()
        context = SimpleNamespace(
            resources=SimpleNamespace(sqlserver=sqlserver)
        )

        def fake_urlopen(request_url, timeout):
            if urlparse(request_url).path.endswith("/targets"):
                return io.BytesIO(json.dumps(targets_payload).encode())
            return io.BytesIO(json.dumps(payload).encode())

        with patch.object(
            availability_module,
            "urlopen",
            side_effect=fake_urlopen,
        ):
            availability_module.observability_facturlavailabilitydaily.op.compute_fn.decorated_fn(
                context
            )

        self.assertEqual(sqlserver.queries, [])
        self.assertTrue(sqlserver.completed[-1]["succeeded"])
        self.assertNotEqual(
            sqlserver.completed[-1]["new_last_loaded_at"],
            sqlserver.watermark,
        )

    def test_configured_target_without_samples_preserves_watermark(
        self,
    ) -> None:
        payload = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [],
            },
        }
        targets_payload = {
            "status": "success",
            "data": {
                "activeTargets": [
                    {
                        "labels": {
                            "job": "blackbox_probe",
                            "user_code": "projectA",
                            "instance": "https://example.test/",
                        }
                    }
                ]
            },
        }
        sqlserver = SqlServerStub()
        context = SimpleNamespace(
            resources=SimpleNamespace(sqlserver=sqlserver)
        )

        def fake_urlopen(request_url, timeout):
            if urlparse(request_url).path.endswith("/targets"):
                return io.BytesIO(json.dumps(targets_payload).encode())
            return io.BytesIO(json.dumps(payload).encode())

        with (
            patch.object(
                availability_module,
                "urlopen",
                side_effect=fake_urlopen,
            ),
            self.assertRaisesRegex(
                RuntimeError,
                "no probe_success samples for https://example.test/",
            ),
        ):
            availability_module.observability_facturlavailabilitydaily.op.compute_fn.decorated_fn(
                context
            )

        self.assertEqual(sqlserver.queries, [])
        self.assertFalse(sqlserver.completed[-1]["succeeded"])
        self.assertEqual(
            sqlserver.completed[-1]["new_last_loaded_at"],
            sqlserver.watermark,
        )

    def test_runtime_recovery_and_loopback_contract(self) -> None:
        repository_root = Path(__file__).resolve().parents[3]
        dagster_config = yaml.safe_load(
            (
                repository_root
                / "dagster"
                / "dagster_home"
                / "dagster.yaml"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            dagster_config["run_monitoring"],
            {
                "enabled": True,
                "start_timeout_seconds": 60,
                "cancel_timeout_seconds": 60,
                "poll_interval_seconds": 15,
            },
        )

        compose_config = yaml.safe_load(
            (
                repository_root
                / "compose"
                / "docker-compose.yml"
            ).read_text(encoding="utf-8")
        )
        daemon_dependencies = compose_config["services"][
            "dagster-daemon"
        ]["depends_on"]
        for service_name in (
            "dagster-user-code-projectA",
        ):
            self.assertEqual(
                daemon_dependencies[service_name]["condition"],
                "service_healthy",
            )
        self.assertNotIn("dagster-user-code-sampleRetail", daemon_dependencies)
        self.assertIn(
            "host.docker.internal:host-gateway",
            compose_config["services"]["blackbox-exporter"][
                "extra_hosts"
            ],
        )

        prometheus_config = yaml.safe_load(
            (
                repository_root
                / "compose"
                / "prometheus"
                / "prometheus.yml"
            ).read_text(encoding="utf-8")
        )
        blackbox_job = next(
            scrape_config
            for scrape_config in prometheus_config["scrape_configs"]
            if scrape_config["job_name"] == "blackbox_probe"
        )
        replacements = {
            relabel_config.get("replacement")
            for relabel_config in blackbox_job["relabel_configs"]
        }
        self.assertIn(
            "${1}host.docker.internal${3}",
            replacements,
        )
        self.assertIn("host.docker.internal${2}", replacements)

    def test_sql_contract_is_scoped_to_standard_user_codes(self) -> None:
        repository_root = Path(__file__).resolve().parents[3]
        standard_user_codes = ("projectA", "projectB", "projectD")
        sql_path = (
            repository_root
            / "sql"
            / "init"
            / "022_observability_factUrlAvailabilityDaily.sql"
        )
        sql = sql_path.read_text(encoding="utf-8")
        self.assertIn(
            "CREATE TABLE observability.factUrlAvailabilityDaily",
            sql,
        )
        self.assertIn("PRIMARY KEY CLUSTERED ([Date], Url)", sql)
        self.assertIn("TimeoutsPresent BIT NOT NULL", sql)
        self.assertIn("N'factUrlAvailabilityDaily'", sql)

        for user_code in standard_user_codes:
            self.assertFalse(
                (
                    repository_root
                    / "sql"
                    / "user_codes"
                    / user_code
                    / "init"
                    / "016_observability_factUrlAvailabilityDaily.sql"
                ).exists()
            )




if __name__ == "__main__":
    unittest.main()
