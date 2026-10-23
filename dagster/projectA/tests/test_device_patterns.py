import re
import ast
import unittest
from pathlib import Path


TRANSFORMATION_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "projectA"
    / "defs"
    / "transformations"
    / "cuartos_refrigeracion.py"
)


def _load_patron_device() -> str:
    tree = ast.parse(TRANSFORMATION_FILE.read_text(encoding="utf-8"))
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "PATRON_DEVICE"
                for target in node.targets
            )
            and isinstance(node.value, ast.Constant)
        ):
            return str(node.value.value)
    raise AssertionError("PATRON_DEVICE was not found")


class ProjectADevicePatternTests(unittest.TestCase):
    def test_cuartos_refrigeracion_matches_conservacion_and_frio_names(self) -> None:
        pattern = re.compile(_load_patron_device())
        matching_names = [
            "Cuarto de conservación",
            "Ambiente conservacion",
            "Cuarto frío",
            "Cuarto frio",
            "Camara fria",
            "Refrigerador",
            "Congelador",
        ]

        for device_type in matching_names:
            with self.subTest(device_type=device_type):
                self.assertRegex(device_type, pattern)


if __name__ == "__main__":
    unittest.main()
