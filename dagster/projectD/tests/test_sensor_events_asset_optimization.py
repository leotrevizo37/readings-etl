import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SensorEventsAssetOptimizationTests(unittest.TestCase):
    def test_projectD_sensor_events_uses_one_extended_reading_call(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "assets"
            / "dwh_factSensorEvents.py"
        ).read_text()

        self.assertEqual(source.count("datalake_readings_extended("), 1)
        self.assertIn("readings_from = min(from_utc, temperature_from_utc, light_from_utc)", source)
        self.assertIn("readings_extended_df.localCheckpoint(", source)
        self.assertIn("readings_extended_df.unpersist(blocking=True)", source)
        self.assertIn("use_utc_range=True", source)
        self.assertIn('col("TimeSpan") >= from_utc', source)
        self.assertIn('col("TimeSpan") >= temperature_from_utc', source)
        self.assertIn("read_completed_sensor_events_range(", source)
        self.assertIn("previous_completed_events_df", source)
        self.assertIn('"TimeSpan",', source)
        self.assertNotIn('.dropDuplicates(["SensorId", "TimeSpan"])', source)
        self.assertNotIn("head(1)", source)

    def test_completed_sensor_events_range_uses_utc_intersection(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "resources"
            / "spark.py"
        ).read_text()
        method = source.split(
            "def read_completed_sensor_events_range", 1
        )[1].split("    def ", 1)[0]

        self.assertIn("IsEventCompleted = 1", method)
        self.assertIn("TimeSpan < '", method)
        self.assertIn("PreviousEndTime", method)
        self.assertIn("DATEADD(SECOND", method)

    def test_datalake_readings_extended_exposes_time_zone(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "transformations"
            / "datalake_readings_extended.py"
        ).read_text()

        self.assertIn("def datalake_readings_extended(", source)
        self.assertIn("use_utc_range: bool = False", source)
        self.assertIn('col("rd.LocalTimeSpan")', source)
        self.assertIn('col("dim.TimeZone")', source)
        self.assertIn("if len(readings_extended_df.head(1)) == 0:", source)
        self.assertNotIn("validate_non_empty", source)

    def test_temperature_events_does_not_normalize_duplicate_readings(self) -> None:
        source = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "transformations"
            / "temperature_events.py"
        ).read_text()

        self.assertNotIn("collect_set", source)
        self.assertIn('orderBy("TimeSpan", "ReadId")', source)

    def test_merge_upserts_columns_and_reconciles_legacy_events(self) -> None:
        source = (
            PROJECT_ROOT / "src" / "projectD" / "defs" / "resources" / "sql_server.py"
        ).read_text()
        merge_section = source.split(
            "def build_fact_sensor_events_merge_sql", 1
        )[1].split("\n\n\ndef ", 1)[0]
        columns = (
            "SensorId",
            "DeviceId",
            "LocationId",
            "LocationName",
            "DeviceType",
            "SensorType",
            "SensorName",
            "TimeSpan",
            "LocalTimeSpan",
            "Value",
            "EventMinutes",
            "IsEventCompleted",
        )
        insert_section = merge_section.split("WHEN NOT MATCHED BY TARGET THEN", 1)[1]

        self.assertIn("MERGE dwh.factSensorEvents", merge_section)
        self.assertIn("FROM stg.factSensorEvents", merge_section)
        self.assertIn("DECLARE @RunId NVARCHAR(100) = ?", merge_section)
        self.assertIn("WHERE RunId = @RunId", merge_section)
        self.assertIn("target.EventId = source.EventId", merge_section)
        self.assertIn("target.ReadId = source.ReadId", merge_section)
        for column in columns:
            self.assertIn("target." + column + " = source." + column, merge_section)
        for column in ("EventId", "ReadId", *columns):
            self.assertIn("source." + column, insert_section)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", merge_section)
        self.assertIn("DELETE target", merge_section)
        self.assertIn("target.SensorId = source.SensorId", merge_section)
        self.assertIn("target.LocalTimeSpan = source.LocalTimeSpan", merge_section)
        self.assertIn("target.TimeSpan IS NULL", merge_section)
        self.assertNotIn("WHEN NOT MATCHED BY SOURCE", merge_section)

    def test_asset_uses_stage_merge_without_dwh_cleanup(self) -> None:
        asset_source = (
            PROJECT_ROOT
            / "src"
            / "projectD"
            / "defs"
            / "assets"
            / "dwh_factSensorEvents.py"
        ).read_text()
        write_section = asset_source.split(
            "def transform_and_load_sensor_events_op", 1
        )[1].split("@op(", 1)[0]
        spark_source = (
            PROJECT_ROOT / "src" / "projectD" / "defs" / "resources" / "spark.py"
        ).read_text()
        writer_section = spark_source.split("def write_fact_sensor_events", 1)[1].split(
            "    def ", 1
        )[0]

        self.assertIn("write_fact_sensor_events(events_df, run_id)", write_section)
        self.assertIn("merge_fact_sensor_events(run_id)", write_section)
        self.assertIn("clear_fact_sensor_events_stage(run_id)", write_section)
        self.assertNotIn("delete_range(", write_section)
        self.assertNotIn("delete_sensor_events_range(", write_section)
        self.assertIn('.withColumn("RunId", lit(run_id))', writer_section)
        self.assertIn('"EventId",', writer_section)
        self.assertIn('"TimeSpan",', writer_section)
        self.assertIn('.dropDuplicates(["EventId"])', writer_section)
        self.assertIn('chunk_column="EventId"', writer_section)
        self.assertIn('table="stg.factSensorEvents"', writer_section)


if __name__ == "__main__":
    unittest.main()
