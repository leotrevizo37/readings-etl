import sys
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
PROJECT_PACKAGE = PROJECT_SRC / "projectB"

if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

loaded_projectB = sys.modules.get("projectB")
if loaded_projectB is not None and hasattr(loaded_projectB, "__path__"):
    package_paths = list(loaded_projectB.__path__)
    if str(PROJECT_PACKAGE) not in package_paths:
        loaded_projectB.__path__.insert(0, str(PROJECT_PACKAGE))
