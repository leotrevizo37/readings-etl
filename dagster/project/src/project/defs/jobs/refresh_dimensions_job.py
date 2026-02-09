from dagster import AssetSelection, define_asset_job

refresh_dimensions_job = define_asset_job(
    name="refresh_dimensions_job",
    selection=AssetSelection.keys("dwh_dimensions"),
)
