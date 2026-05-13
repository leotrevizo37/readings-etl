from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey(["projectB", "dwh_dimsidonproddimensions"]),
        AssetKey(["projectB", "dwh_factlecturas"]),
    ],
    group_name="projectB",
    key_prefix=["projectB"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
