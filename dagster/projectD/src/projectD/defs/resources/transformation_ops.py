from typing import Optional

from ..transformations import (raw_dimensions,
                               factlecturas_extended,
                               gabinete,
                               cisternas,
                               sensor_fase,
                               temperatura_humedad,
                               acometida,
                               weather_station,
                               sensor_events,
                               light_events,
                               temperature_events)
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
    def raw_dimensions(
            self,
            spark_res: SparkResource,
            dim_sqlserver: SqlDimServerResource) -> Optional[DataFrame]:
        return raw_dimensions(spark_res, dim_sqlserver)
    def gabinete(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return gabinete(df, dims, min_hour, max_hour)
    def cisternas(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return cisternas(df, dims, min_hour, max_hour)
    def sensor_fase(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return sensor_fase(df, dims, min_hour, max_hour)
    def temperatura_humedad(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return temperatura_humedad(df, dims, min_hour, max_hour)
    def acometida(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return acometida(df, dims, min_hour, max_hour)
    def weather_station(self, df:DataFrame, dims:DataFrame, min_hour: str, max_hour: str) -> Optional[DataFrame]:
        return weather_station(df, dims, min_hour, max_hour)
    def sensor_events(
            self,
            df: DataFrame,
            to_utc_str: str,
            previous_completed_events_df: DataFrame) -> Optional[DataFrame]:
        return sensor_events(df, to_utc_str, previous_completed_events_df)
    def light_events(
            self,
            df: DataFrame,
            to_utc_str: str,
            previous_completed_events_df: DataFrame) -> Optional[DataFrame]:
        return light_events(df, to_utc_str, previous_completed_events_df)
    def temperature_events(
            self,
            df: DataFrame,
            expected_values: DataFrame,
            to_utc_str: str) -> Optional[DataFrame]:
        return temperature_events(df, expected_values, to_utc_str)

@resource
def transformation_ops():
    return TransformationOps()
