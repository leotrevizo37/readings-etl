import unittest
from pathlib import Path


SCHEDULE_SOURCE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "projectA"
    / "defs"
    / "schedules"
    / "factaudiencia_scheduler.py"
)


class FactAudienciaScheduleTests(unittest.TestCase):
    def test_weekly_schedule_uses_business_and_weekend_windows(self) -> None:
        source = SCHEDULE_SOURCE.read_text()

        self.assertIn(
            'name="014_Sensor_audiencia_ingestion_every_w"',
            source,
        )
        self.assertIn('"0 7-17/2 * * 1-5"', source)
        self.assertIn('"0 23 * * 1-5"', source)
        self.assertIn('"0 11,23 * * 0,6"', source)
        self.assertIn(
            'execution_timezone="America/Ciudad_Juarez"',
            source,
        )
        self.assertIn(
            "default_status=DefaultScheduleStatus.RUNNING",
            source,
        )


if __name__ == "__main__":
    unittest.main()
