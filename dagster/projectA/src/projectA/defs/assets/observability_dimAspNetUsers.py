from time import perf_counter

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset,
)

from .source_assets import dbo_aspnetusers
from ..resources import SparkResource, SqlDimServerResource, SqlServerResource
from ..transformations.telemetry_user_usage import build_dim_aspnet_users_df


@asset(
    deps=[dbo_aspnetusers],
    required_resource_keys={"spark", "dim_sqlserver", "sqlserver"},
    group_name="projectA",
    key_prefix=["projectA"],
)
def observability_dimaspnetusers(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start_time = perf_counter()
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver
    run_id = context.run_id
    staged_rows = 0
    merged_rows = 0

    try:
        spark = spark_res.get_spark()
        users_df = (
            spark.read
            .format("jdbc")
            .option(
                "dbtable",
                "(SELECT Id, UserName, Name, LastName, Position "
                "FROM dbo.AspNetUsers) AS aspnet_users",
            )
            .options(**dim_sql.get_basic_options())
            .load()
        )
        result_df = build_dim_aspnet_users_df(users_df)
        sqlserver.clear_dim_aspnet_users_stage(run_id)
        staged_rows = spark_res.write_dim_aspnet_users(result_df, run_id) or 0
        merged_rows = sqlserver.merge_dim_aspnet_users(run_id)
    finally:
        sqlserver.clear_dim_aspnet_users_stage(run_id)

    return MaterializeResult(
        metadata={
            "row_count": staged_rows,
            "merged_rows": merged_rows,
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        },
    )
