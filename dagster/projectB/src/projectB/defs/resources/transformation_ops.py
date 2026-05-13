from typing import Optional

from ..transformations import (raw_dimensions,
                               factlecturas_extended,
                               factReadingsMeasurement_device,
                               termostatos,
                               luces)
from ..resources import (SqlServerResource,
                         SparkResource,
                         SqlDimServerResource)

from pyspark.sql import DataFrame
from dagster import resource

class TransformationOps:
    def factlecturas_extended(
            self,
            spark_res: SparkResource,
            sqlserver: SqlServerResource,
            raw_dimensions: DataFrame,
            fetch_factlecturas_from: str) -> Optional[DataFrame]:
        return factlecturas_extended(
            spark_res,
            sqlserver,
            raw_dimensions,
            fetch_factlecturas_from
        )
    def factReadingsMeasurement_device(self, df:DataFrame) -> Optional[DataFrame]:
        return factReadingsMeasurement_device(df)
    def luces(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return luces(df, dims, min_hour, max_hour)
    def raw_dimensions(
            self,
            spark_res: SparkResource,
            dim_sqlserver: SqlDimServerResource) -> Optional[DataFrame]:
        return raw_dimensions(spark_res, dim_sqlserver)
    def termostatos(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return termostatos(df, dims, min_hour, max_hour)

@resource
def transformation_ops():
    return TransformationOps()
