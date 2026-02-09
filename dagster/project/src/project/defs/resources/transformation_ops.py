from typing import Optional

from ..transformations import (raw_dimensions,
                               readings_extended,
                               factReadingsMeasurement_device,
                               maquinas_nieve,
                               maquinas_soda,
                               termostatos,
                               cuartos_congelacion,
                               cuartos_refrigeracion,
                               luces)
from ..resources import (SqlServerResource,
                         SparkResource,
                         SqlDimServerResource)

from pyspark.sql import DataFrame
from dagster import resource

class TransformationOps:
    def cuartos_congelacion(self, df:DataFrame) -> Optional[DataFrame]:
        return cuartos_congelacion(df)
    def cuartos_refrigeracion(self, df:DataFrame) -> Optional[DataFrame]:
        return cuartos_refrigeracion(df)
    def readings_extended(
            self,
            spark_res: SparkResource,
            sqlserver: SqlServerResource,
            raw_dimensions: DataFrame,
            fetch_readings_from: str) -> Optional[DataFrame]:
        return readings_extended(
            spark_res,
            sqlserver,
            raw_dimensions,
            fetch_readings_from
        )
    def factReadingsMeasurement_device(self, df:DataFrame) -> Optional[DataFrame]:
        return factReadingsMeasurement_device(df)
    def luces(self, df:DataFrame) -> Optional[DataFrame]:
        return luces(df)
    def maquinas_nieve(self, df:DataFrame) -> Optional[DataFrame]:
        return maquinas_nieve(df)
    def maquinas_soda(self, df:DataFrame) -> Optional[DataFrame]:
        return maquinas_soda(df)
    def raw_dimensions(
            self,
            spark_res: SparkResource,
            dim_sqlserver: SqlDimServerResource) -> Optional[DataFrame]:
        return raw_dimensions(spark_res, dim_sqlserver)
    def termostatos(self, df:DataFrame) -> Optional[DataFrame]:
        return termostatos(df)

@resource
def transformation_ops():
    return TransformationOps()
