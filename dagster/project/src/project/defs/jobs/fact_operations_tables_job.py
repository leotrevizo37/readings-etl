from dagster import define_asset_job, AssetSelection

fact_operations_tables_job = define_asset_job(
    name="fact_operations_tables_job",
    selection=AssetSelection.keys(
        "data_transformations",
        "dwh_factHourlyDeviceStatus",
        "dwh_factHourlyReadingStatus",
        "ctl_IngestionControl"
    ),
)