from __future__ import annotations

from typing import Optional


DEFAULT_DEVICE_STATUS_CODE = 3

STATUS_FIELDS = (
    "amperage_status",
    "pressure_status",
    "temperature_status",
    "current_status",
    "termostatus",
)

DEVICE_STATUS_RULES = (
    (
        {
            "amperage_status": 1,
            "pressure_status": None,
            "temperature_status": None,
            "current_status": None,
            "termostatus": None,
        },
        1,
    ),
    (
        {
            "amperage_status": 1,
            "pressure_status": 1,
            "temperature_status": None,
            "current_status": None,
            "termostatus": None,
        },
        1,
    ),
    (
        {
            "amperage_status": 1,
            "pressure_status": 0,
            "temperature_status": None,
            "current_status": None,
            "termostatus": None,
        },
        2,
    ),
    (
        {
            "amperage_status": 1,
            "pressure_status": None,
            "temperature_status": 0,
            "current_status": None,
            "termostatus": None,
        },
        4,
    ),
    (
        {
            "amperage_status": 1,
            "pressure_status": None,
            "temperature_status": 1,
            "current_status": None,
            "termostatus": None,
        },
        1,
    ),
    (
        {
            "amperage_status": None,
            "pressure_status": None,
            "temperature_status": 1,
            "current_status": None,
            "termostatus": None,
        },
        1,
    ),
    (
        {
            "amperage_status": None,
            "pressure_status": None,
            "temperature_status": None,
            "current_status": 1,
            "termostatus": None,
        },
        1,
    ),
    (
        {
            "amperage_status": 1,
            "pressure_status": None,
            "temperature_status": None,
            "current_status": 1,
            "termostatus": None,
        },
        1,
    ),
    (
        {
            "amperage_status": 0,
            "pressure_status": None,
            "temperature_status": None,
            "current_status": 1,
            "termostatus": None,
        },
        1,
    ),
    (
        {
            "amperage_status": None,
            "pressure_status": None,
            "temperature_status": 1,
            "current_status": None,
            "termostatus": 1,
        },
        1,
    ),
    (
        {
            "amperage_status": None,
            "pressure_status": None,
            "temperature_status": 0,
            "current_status": None,
            "termostatus": 1,
        },
        1,
    ),
)


def resolve_device_status(
    *,
    amperage_status: Optional[int],
    pressure_status: Optional[int],
    temperature_status: Optional[int],
    current_status: Optional[int],
    termostatus: Optional[int],
) -> int:
    current_values = {
        "amperage_status": amperage_status,
        "pressure_status": pressure_status,
        "temperature_status": temperature_status,
        "current_status": current_status,
        "termostatus": termostatus,
    }

    for expected_values, resolved_code in DEVICE_STATUS_RULES:
        if all(current_values[field] == expected for field, expected in expected_values.items()):
            return resolved_code

    return DEFAULT_DEVICE_STATUS_CODE
