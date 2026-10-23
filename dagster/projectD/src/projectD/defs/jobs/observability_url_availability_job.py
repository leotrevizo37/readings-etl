from dagster import AssetKey, AssetSelection, define_asset_job


observability_url_availability_job = define_asset_job(
    name="122_observability_url_availability",
    selection=AssetSelection.keys(
        AssetKey(["projectD", "observability_facturlavailabilitydaily"]),
    ),
    tags={"job_type": "url_availability", "user_code_run_lane": "projectD_observability", "user_code": "projectD"},
)

