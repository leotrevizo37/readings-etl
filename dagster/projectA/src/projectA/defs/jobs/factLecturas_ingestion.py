from dagster import AssetKey, AssetSelection, define_asset_job

fact_lecturas_ingestion_job = define_asset_job(
    name="fact_lecturas_ingestion_job",
    selection=AssetSelection.keys(AssetKey(["projectA", "dwh_factlecturas"])),
    tags={"job_type": "lecturas", "user_code": "projectA"},
)
