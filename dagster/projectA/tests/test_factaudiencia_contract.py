import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]


class FactAudienciaContractTests(unittest.TestCase):
    def test_table_matches_audiencia_parquet_contract(self) -> None:
        sql = (
            REPOSITORY_ROOT
            / "sql"
            / "user_codes"
            / "projectA"
            / "init"
            / "018_dwh_factAudiencia.sql"
        ).read_text()

        columns = [
            "EventId UNIQUEIDENTIFIER NOT NULL",
            "SensorId UNIQUEIDENTIFIER NOT NULL",
            "PhysicalIdentifier NVARCHAR(255) NULL",
            "TimeSpan DATETIME2(7) NOT NULL",
            "LocalTimeSpan DATETIME2(7) NOT NULL",
            "Age INT NOT NULL",
            "AgeGroup NVARCHAR(255) NULL",
            "IsKid BIT NOT NULL",
            "Gender NVARCHAR(255) NULL",
            "EmotionRaw NVARCHAR(255) NULL",
            "Emotion NVARCHAR(255) NULL",
            "Sentiment NVARCHAR(255) NULL",
        ]
        positions = [sql.index(column) for column in columns]

        self.assertEqual(positions, sorted(positions))
        self.assertIn("PRIMARY KEY CLUSTERED (EventId)", sql)

    def test_pipeline_uses_audience_root_and_daily_backfill_only(self) -> None:
        spark = (
            PROJECT_ROOT
            / "src"
            / "projectA"
            / "defs"
            / "resources"
            / "spark.py"
        ).read_text()
        backfill = (
            PROJECT_ROOT
            / "src"
            / "projectA"
            / "defs"
            / "jobs"
            / "factaudiencia_backfill_job.py"
        ).read_text()

        self.assertIn('self._build_adls_path("audience")', spark)
        self.assertIn('dropDuplicates(["EventId"])', spark)
        self.assertIn('table="dwh.factAudiencia"', spark)
        self.assertIn('name="015_Sensor_audiencia_backfill_daily"', backfill)
        self.assertNotIn("sensor_ids", backfill)


if __name__ == "__main__":
    unittest.main()
