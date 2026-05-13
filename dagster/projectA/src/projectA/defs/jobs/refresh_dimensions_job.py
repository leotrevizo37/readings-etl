from dagster import AssetKey, AssetSelection, define_asset_job

refresh_dimensions_job = define_asset_job(
    name="refresh_dimensions_job",
    selection=AssetSelection.keys(AssetKey(["projectA", "dwh_dimsidonproddimensions"])),
    tags={"job_type": "dimensiones", "user_code": "projectA"},
)
