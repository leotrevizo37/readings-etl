from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey(["projectC", "dwh_dimsidonproddimensions"]),
        AssetKey(["projectC", "dwh_factlecturas"]),
    ],
    group_name="projectC",
    key_prefix=["projectC"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
