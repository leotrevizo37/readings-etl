from dagster import (
        AssetKey,
        asset,
        MaterializeResult,
        AssetExecutionContext)
from time import perf_counter

from ..resources import (
    SparkResource,
    SqlDimServerResource,
    SqlServerResource,
)
from ..transformations import sensor_expected_values


@asset(
    non_argument_deps={
        AssetKey(["projectA", "dbo_ProgramProfileTys"]),
        AssetKey(["projectA", "dbo_ProgramProfiles"]),
        AssetKey(["projectA", "dbo_ProgramSchedules"]),
        AssetKey(["projectA", "dbo_GroupSensors"]),
        AssetKey(["projectA", "dbo_GroupHasSchedules"]),
        AssetKey(["projectA", "dbo_GroupHasSensors"]),
        AssetKey(["projectA", "dbo_sensors"]),
        AssetKey(["projectA", "dbo_sensor_tys"]),
        AssetKey(["projectA", "dbo_devices"]),
        AssetKey(["projectA", "dbo_device_tys"]),
    },
    required_resource_keys={
        "spark",
        "dim_sqlserver",
        "sqlserver",
    },
    group_name="projectA",
    key_prefix=["projectA"],
)
def dwh_dimsensorexpectedval(
    context: AssetExecutionContext) -> MaterializeResult:
    start_time = perf_counter()
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver

    context.log.info("Changes detected, materializing dimSensorExpectedVal...")
    expected_values_df = sensor_expected_values(spark_res, dim_sql)

    context.log.info("Refreshing dwh.dimSensorExpectedVal...")
    cleared = sqlserver.execute_query("DELETE FROM dwh.dimSensorExpectedVal")
    if isinstance(cleared, Exception):
        raise RuntimeError("Failed to clear dwh.dimSensorExpectedVal.") from cleared

    inserted = spark_res.write_dim_sensor_expected_values(expected_values_df)

    return MaterializeResult(
        metadata={
            "row_count": inserted,
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        },
    )
