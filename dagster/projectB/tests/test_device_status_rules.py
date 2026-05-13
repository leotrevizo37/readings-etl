import sys
import unittest
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from projectB.device_status_rules import (  # noqa: E402
    DEFAULT_DEVICE_STATUS_CODE,
    DEVICE_STATUS_RULES,
    STATUS_FIELDS,
    resolve_device_status,
)


class ResolveDeviceStatusTests(unittest.TestCase):
    def test_every_explicit_rule_resolves_expected_code(self) -> None:
        for expected_values, expected_code in DEVICE_STATUS_RULES:
            with self.subTest(rule=expected_values):
                resolved = resolve_device_status(**expected_values)
                self.assertEqual(resolved, expected_code)

    def test_unmatched_combination_falls_back_to_default_status(self) -> None:
        resolved = resolve_device_status(
            amperage_status=0,
            pressure_status=0,
            temperature_status=0,
            current_status=0,
            termostatus=0,
        )

        self.assertEqual(resolved, DEFAULT_DEVICE_STATUS_CODE)

    def test_all_null_combination_falls_back_to_default_status(self) -> None:
        resolved = resolve_device_status(
            amperage_status=None,
            pressure_status=None,
            temperature_status=None,
            current_status=None,
            termostatus=None,
        )

        self.assertEqual(resolved, DEFAULT_DEVICE_STATUS_CODE)

    def test_pressure_failure_maps_to_status_two(self) -> None:
        resolved = resolve_device_status(
            amperage_status=1,
            pressure_status=0,
            temperature_status=None,
            current_status=None,
            termostatus=None,
        )

        self.assertEqual(resolved, 2)

    def test_temperature_failure_maps_to_status_four(self) -> None:
        resolved = resolve_device_status(
            amperage_status=1,
            pressure_status=None,
            temperature_status=0,
            current_status=None,
            termostatus=None,
        )

        self.assertEqual(resolved, 4)


class DeviceStatusRuleShapeTests(unittest.TestCase):
    def test_every_rule_declares_the_full_status_shape(self) -> None:
        for expected_values, _ in DEVICE_STATUS_RULES:
            with self.subTest(rule=expected_values):
                self.assertEqual(tuple(expected_values.keys()), STATUS_FIELDS)


if __name__ == "__main__":
    unittest.main()
