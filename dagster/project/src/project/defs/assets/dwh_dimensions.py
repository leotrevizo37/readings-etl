from dagster import (
        AssetKey,
        asset,
        MaterializeResult,
        AssetExecutionContext)

from ..resources import (SparkResource,
                         SqlDimServerResource)
from ..transformations import raw_dimensions

@asset(
    non_argument_deps={
        AssetKey("dbo_sensors"),
        AssetKey("dbo_sensor_tys"),
        AssetKey("dbo_devices"),
        AssetKey("dbo_device_tys"),
        AssetKey("dbo_sublocations"),
        AssetKey("dbo_sublocation_tys")
    },
    required_resource_keys={
        "spark",
        "dim_sqlserver"
    },
)
def dwh_dimsidonproddimensions(
    context: AssetExecutionContext) -> MaterializeResult:
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver

    context.log.info("Changes detected, materializing dimensions...")
    raw_dims_df = raw_dimensions(spark_res, dim_sql)
    context.log.info("Inserting new entries to stg.dimSidonProdDimensions...")
    _inserted = spark_res.write_dim_prod_dimensions(raw_dims_df)

    return MaterializeResult(
        metadata={"row_count": _inserted},
    )