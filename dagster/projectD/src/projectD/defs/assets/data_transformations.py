from typing import Optional, Dict, Any

from dagster import asset, AssetKey


@asset(
    name="data_transformations",
    deps=[
        AssetKey(["projectD", "dwh_dimtelemetryproddimensions"]),
        AssetKey(["projectD", "dwh_factlecturas"]),
    ],
    group_name="projectD",
    key_prefix=["projectD"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
