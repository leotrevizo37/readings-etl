from dagster import AssetKey, AssetSelection, define_asset_job


observability_dim_aspnet_users_job = define_asset_job(
    name="120_observability_dim_aspnetusers",
    selection=AssetSelection.keys(
        AssetKey(["projectA", "observability_dimaspnetusers"]),
    ),
    tags={"job_type": "dimensiones", "user_code_run_lane": "projectA_observability", "user_code": "projectA"},
)
