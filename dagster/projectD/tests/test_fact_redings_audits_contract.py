import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]


class FactRedingsAuditsContractTests(unittest.TestCase):
    def test_table_contract_uses_only_sensor_and_hour_keys(self) -> None:
        sql = (
            REPOSITORY_ROOT
            / "sql"
            / "init"
            / "014_observability_factRedingsAudits.sql"
        ).read_text()

        self.assertIn("observability.factRedingsAudits", sql)
        self.assertEqual(
            sql.count("            TimeSpan DATETIME2(0) NOT NULL,"),
            2,
        )
        self.assertIn(
            "PRIMARY KEY CLUSTERED (SensorId, TimeSpan)",
            sql,
        )
        self.assertNotIn("OperationId", sql)
        self.assertNotIn("DeviceId", sql)
        self.assertNotIn("LocationId", sql)
        self.assertNotIn("SensorTyId", sql)
        self.assertNotIn("TimeZone", sql)

    def test_merge_is_idempotent_and_preserves_detected_lateness(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "resources"
            / "sql_server.py"
        ).read_text()
        merge = source.split("FACT_REDINGS_AUDITS_MERGE_SQL = ", 1)[1].split(
            "\n\n\nclass ",
            1,
        )[0]
        spark = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "resources"
            / "spark.py"
        ).read_text()
        stage_writer = spark.split(
            "    def write_fact_redings_audits_stage(",
            1,
        )[1].split("\n    def ", 1)[0]

        self.assertIn("MERGE observability.factRedingsAudits", merge)
        self.assertIn("FROM stg.factRedingsAudits", merge)
        self.assertIn("WHERE RunId = ?", merge)
        self.assertIn("target.SensorId = source.SensorId", merge)
        self.assertIn(
            "AND target.TimeSpan = source.TimeSpan",
            merge,
        )
        self.assertIn(
            "target.LocalTimeSpan = source.LocalTimeSpan,",
            merge,
        )
        self.assertIn(
            "target.HasLateReadings = 1 OR source.HasLateReadings = 1",
            merge,
        )
        self.assertIn(
            'dropDuplicates(["SensorId", "TimeSpan"])',
            spark,
        )
        for column in (
            "AverageGapMinutes",
            "MaximumGapMinutes",
            "MinimumGapMinutes",
            "ReadingsCoveragePercentage",
            "CoveredReadingBlocks",
        ):
            self.assertIn(column, merge)
            self.assertIn(f'"{column}"', stage_writer)
        self.assertIn(
            '"(SELECT SensorId, TimeSpan, LastReadingAt "',
            spark,
        )
        self.assertIn('"WHERE TimeSpan >= \'"', spark)
        self.assertIn(
            '"PARTITION BY SensorId ORDER BY TimeSpan DESC"',
            spark,
        )

    def test_overlap_and_schedule_are_scoped_to_projectD(self) -> None:
        compose = (
            REPOSITORY_ROOT / "compose" / "docker-compose.yml"
        ).read_text()
        example = (
            REPOSITORY_ROOT / "compose" / ".env.example"
        ).read_text()
        schedule = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "schedules"
            / "factredingsaudits_scheduler.py"
        ).read_text()
        job = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "jobs"
            / "factredingsaudits_job.py"
        ).read_text()
        asset = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "assets"
            / "observability_factRedingsAudits.py"
        ).read_text()

        self.assertIn(
            "FACTREDINGSAUDITS_OVERLAP_HOURS: "
            "${PROJECTD_DEFAULT_FACTREDINGSAUDITS_OVERLAP_HOURS:-1}",
            compose,
        )
        self.assertIn(
            "PROJECTD_DEFAULT_FACTREDINGSAUDITS_OVERLAP_HOURS=1",
            example,
        )
        self.assertIn('cron_schedule="*/10 * * * *"', schedule)
        old_schedule, new_schedule = schedule.split("redings_audits_sched_every_40m =", 1)
        self.assertIn("DefaultScheduleStatus.STOPPED", old_schedule)
        self.assertIn("DefaultScheduleStatus.RUNNING", new_schedule)
        self.assertIn('name="022_Sensor_redings_audits_every_40m"', new_schedule)
        self.assertIn(
            'cron_schedule=["0,40 */2 * * *", "20 1-23/2 * * *"]',
            new_schedule,
        )
        self.assertIn('name="022_Sensor_redings_audits"', job)
        self.assertIn(
            'name="022_Sensor_redings_audits_every_10m"',
            schedule,
        )
        self.assertIn(
            "read_lecturas_range(from_utc, to_utc)",
            asset,
        )
        self.assertNotIn("read_lecturas_local_range", asset)
        self.assertIn(
            'name="observability_factredingsaudits"',
            asset,
        )


if __name__ == "__main__":
    unittest.main()
