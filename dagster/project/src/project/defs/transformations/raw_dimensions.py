from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..resources import SparkResource, SqlDimServerResource

def raw_dimensions(
        spark_res: SparkResource,
        dim_sqlserver: SqlDimServerResource) -> Optional[DataFrame]:
    spark = spark_res.get_spark()

    dbo_sensors_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.Sensors")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )

    dbo_sensor_tys_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.SensorTys")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )

    dbo_devices_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.Devices")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )

    dbo_device_tys_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.DeviceTys")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )

    dbo_sublocations_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.SubLocations")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )

    dbo_sublocation_tys_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.SubLocationTys")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )

    sensors_devices_locations_df = (
        dbo_sensors_df.alias("s")
        .join(dbo_sensor_tys_df.alias("sty"), on="SensorTyId", how="left")
        .select(
            col("s.SensorId"),
            col("s.SensorTyId"),
            col("s.DeviceId"),
            col("s.Name").alias("_sensor_name"),
            col("sty.Name").alias("_sensor_type")
        )
        .alias("st")
        .join(dbo_devices_df.alias("dv"), on="DeviceId", how="left")
        .select(
            col("st.SensorId"),
            col("st.SensorTyId"),
            col("st.DeviceId"),
            col("st._sensor_name"),
            col("st._sensor_type"),
            col("dv.DeviceTyId"),
            col("dv.SubLocationId"),
            col("dv.Name").alias("_device_name")
        )
        .alias("st_dv")
        .join(dbo_device_tys_df.alias("dvt"), on="DeviceTyId", how="left")
        .select(
            col("st_dv.SensorId"),
            col("st_dv.SensorTyId"),
            col("st_dv.DeviceId"),
            col("st_dv._sensor_name"),
            col("st_dv._sensor_type"),
            col("st_dv.DeviceTyId"),
            col("st_dv.SubLocationId"),
            col("st_dv._device_name"),
            col("dvt.Name").alias("_device_type")
        )
        .alias("st_dvt")
        .join(dbo_sublocations_df.alias("sl"), on="SubLocationId", how="left")
        .select(
            col("st_dvt.SensorId"),
            col("st_dvt.SensorTyId"),
            col("st_dvt.DeviceId"),
            col("st_dvt._sensor_name"),
            col("st_dvt._sensor_type"),
            col("st_dvt.DeviceTyId"),
            col("st_dvt.SubLocationId"),
            col("st_dvt._device_name"),
            col("st_dvt._device_type"),
            col("sl.SubLocationTyId"),
            col("sl.Name").alias("_sublocation_name"),
            col("sl.LocationId")
        )
        .alias("st_dvt_sl")
        .join(dbo_sublocation_tys_df.alias("slty"), on="SubLocationTyId", how="left")
        .select(
            col("st_dvt_sl.SensorId"),
            col("st_dvt_sl.SensorTyId"),
            col("st_dvt_sl.DeviceId"),
            col("st_dvt_sl._sensor_name"),
            col("st_dvt_sl._sensor_type"),
            col("st_dvt_sl.DeviceTyId"),
            col("st_dvt_sl.SubLocationId"),
            col("st_dvt_sl._device_name"),
            col("st_dvt_sl._device_type"),
            col("st_dvt_sl.SubLocationTyId"),
            col("st_dvt_sl._sublocation_name"),
            col("st_dvt_sl.LocationId"),
            col("slty.Name").alias("_sublocation_type")
        )
    )

    if sensors_devices_locations_df.rdd.isEmpty():
        return None

    return sensors_devices_locations_df
