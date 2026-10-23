from dagster import In, Out, job, op
from pyspark.sql import DataFrame

from ..resources import SparkResource


@op(
    required_resource_keys={"spark"},
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
)
def write_spark_df_io_manager_healthcheck_op(context) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    spark = spark_res.get_spark()
    context.log.info("Writing spark_df_io_manager healthcheck DataFrame.")
    return spark.createDataFrame([(1, "ok")], ["id", "status"])


@op(
    ins={"df": In(DataFrame)},
)
def read_spark_df_io_manager_healthcheck_op(context, df: DataFrame) -> None:
    row_count = df.count()
    if row_count != 1:
        raise RuntimeError(
            "spark_df_io_manager healthcheck expected 1 row, got "
            + str(row_count)
        )
    context.log.info("Read spark_df_io_manager healthcheck DataFrame.")


@job(name="089_maintenance_spark_healthcheck", tags={"run_lane": "operational", "user_code_run_lane": "projectA_operational"})
def spark_df_io_manager_healthcheck_job():
    read_spark_df_io_manager_healthcheck_op(
        write_spark_df_io_manager_healthcheck_op()
    )
