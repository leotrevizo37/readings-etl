from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey("dwh_dimensions"),
        AssetKey("dwh_readings"),
    ],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
