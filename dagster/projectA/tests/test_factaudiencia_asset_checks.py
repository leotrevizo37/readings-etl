import sys
import unittest
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from dagster import AssetCheckSeverity
from projectA.defs.asset_checks import (
    _build_factaudiencia_original_lte_to_utc_result,
    _build_factaudiencia_row_count_result,
    factaudiencia_original_lte_to_utc,
    factaudiencia_row_count_gt_0,
    quality_asset_checks,
)


class FactAudienciaAssetChecksTests(unittest.TestCase):
    def test_row_count_check_matches_factlecturas_contract(self) -> None:
        passing = _build_factaudiencia_row_count_result({"row_count": 1})
        failing = _build_factaudiencia_row_count_result({"row_count": 0})
        missing = _build_factaudiencia_row_count_result({})

        self.assertTrue(passing.passed)
        self.assertFalse(failing.passed)
        self.assertFalse(missing.passed)
        self.assertEqual(passing.severity, AssetCheckSeverity.WARN)
        self.assertEqual(failing.severity, AssetCheckSeverity.WARN)

    def test_original_watermark_must_not_exceed_to_utc(self) -> None:
        passing = _build_factaudiencia_original_lte_to_utc_result(
            {
                "original_last_loaded_at": "2026-08-26 02:00:00",
                "to_utc": "2026-08-26 03:00:00",
            }
        )
        equal = _build_factaudiencia_original_lte_to_utc_result(
            {
                "original_last_loaded_at": "2026-08-26 03:00:00",
                "to_utc": "2026-08-26 03:00:00",
            }
        )
        failing = _build_factaudiencia_original_lte_to_utc_result(
            {
                "original_last_loaded_at": "2026-08-26 04:00:00",
                "to_utc": "2026-08-26 03:00:00",
            }
        )
        missing = _build_factaudiencia_original_lte_to_utc_result({})

        self.assertTrue(passing.passed)
        self.assertTrue(equal.passed)
        self.assertFalse(failing.passed)
        self.assertFalse(missing.passed)
        self.assertEqual(passing.severity, AssetCheckSeverity.WARN)

    def test_checks_target_factaudiencia_and_are_registered(self) -> None:
        source = (
            PROJECT_SRC / "projectA" / "defs" / "asset_checks.py"
        ).read_text()

        self.assertIn(
            'asset=FACTAUDIENCIA_ASSET_KEY,\n    name="row_count_gt_0"',
            source,
        )
        self.assertIn(
            'asset=FACTAUDIENCIA_ASSET_KEY,\n'
            '    name="original_last_loaded_at_lte_to_utc"',
            source,
        )
        self.assertIn(factaudiencia_row_count_gt_0, quality_asset_checks)
        self.assertIn(factaudiencia_original_lte_to_utc, quality_asset_checks)


if __name__ == "__main__":
    unittest.main()
