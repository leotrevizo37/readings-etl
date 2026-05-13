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
        AssetKey(["projectB", "dbo_ProgramProfileTys"]),
        AssetKey(["projectB", "dbo_ProgramProfiles"]),
        AssetKey(["projectB", "dbo_ProgramSchedules"]),
        AssetKey(["projectB", "dbo_GroupSensors"]),
        AssetKey(["projectB", "dbo_GroupHasSchedules"]),
        AssetKey(["projectB", "dbo_GroupHasSensors"]),
        AssetKey(["projectB", "dbo_sensors"]),
        AssetKey(["projectB", "dbo_sensor_tys"]),
        AssetKey(["projectB", "dbo_devices"]),
        AssetKey(["projectB", "dbo_device_tys"]),
    },
    required_resource_keys={
        "spark",
        "dim_sqlserver",
        "sqlserver",
    },
    group_name="projectB",
    key_prefix=["projectB"],
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
