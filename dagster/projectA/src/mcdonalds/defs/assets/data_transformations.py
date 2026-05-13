from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey(["projectA", "dwh_dimsidonproddimensions"]),
        AssetKey(["projectA", "dwh_factlecturas"]),
    ],
    group_name="projectA",
    key_prefix=["projectA"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
