from dagster import AssetKey, AssetSelection, define_asset_job

refresh_dimensions_job = define_asset_job(
    name="refresh_dimensions_job",
    selection=AssetSelection.keys(AssetKey(["projectC", "dwh_dimtelemetryproddimensions"])),
    tags={"job_type": "dimensiones", "user_code": "projectC"},
)
