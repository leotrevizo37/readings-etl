from dagster import (
        AssetKey,
        asset,
        MaterializeResult,
        AssetExecutionContext)
from time import perf_counter

from ..resources import (SparkResource,
                         SqlDimServerResource,
                         SqlServerResource)
from ..transformations import raw_dimensions

@asset(
    non_argument_deps={
        AssetKey(["projectA", "dbo_sensors"]),
        AssetKey(["projectA", "dbo_sensor_tys"]),
        AssetKey(["projectA", "dbo_devices"]),
        AssetKey(["projectA", "dbo_device_tys"]),
        AssetKey(["projectA", "dbo_sublocations"]),
        AssetKey(["projectA", "dbo_sublocation_tys"])
    },
    required_resource_keys={
        "spark",
        "dim_sqlserver",
        "sqlserver"
    },
    group_name="projectA",
    key_prefix=["projectA"],
)
def dwh_dimtelemetryproddimensions(
    context: AssetExecutionContext) -> MaterializeResult:
    start_time = perf_counter()
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver
    run_id = context.run_id

    _staged = 0
    _merged = 0
    try:
        context.log.info("Changes detected, materializing dimensions...")
        raw_dims_df = raw_dimensions(spark_res, dim_sql)
        context.log.info("Inserting new entries to stg.dimTelemetryProdDimensions...")
        _staged = spark_res.write_dim_prod_dimensions(raw_dims_df, run_id)
        _merged = sqlserver.merge_dim_telemetry_prod_dimensions(run_id)
    finally:
        sqlserver.clear_dim_telemetry_prod_dimensions_stage(run_id)

    return MaterializeResult(
        metadata={
            "row_count": _staged,
            "merged_rows": _merged,
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        },
    )
