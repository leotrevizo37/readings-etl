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

    dbo_locations_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.Locations")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )

    dbo_cities_df =(
        spark.read.format("jdbc")
        .option("dbtable", "dbo.Cities")
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
            col("s.Active"),
            col("s.Name").alias("sensor_name"),
            col("sty.Name").alias("sensor_type")
        )
        .alias("st")
        .join(dbo_devices_df.alias("dv"), on="DeviceId", how="left")
        .select(
            col("st.SensorId"),
            col("st.SensorTyId"),
            col("st.DeviceId"),
            col("st.sensor_name"),
            col("st.sensor_type"),
            col("dv.DeviceTyId"),
            col("dv.SubLocationId"),
            col("st.Active"),
            col("dv.Name").alias("device_name")
        )
        .alias("st_dv")
        .join(dbo_device_tys_df.alias("dvt"), on="DeviceTyId", how="left")
        .select(
            col("st_dv.SensorId"),
            col("st_dv.SensorTyId"),
            col("st_dv.DeviceId"),
            col("st_dv.sensor_name"),
            col("st_dv.sensor_type"),
            col("st_dv.DeviceTyId"),
            col("st_dv.SubLocationId"),
            col("st_dv.device_name"),
            col("st_dv.Active"),
            col("dvt.Name").alias("device_type")
        )
        .alias("st_dvt")
        .join(dbo_sublocations_df.alias("sl"), on="SubLocationId", how="left")
        .select(
            col("st_dvt.SensorId"),
            col("st_dvt.SensorTyId"),
            col("st_dvt.DeviceId"),
            col("st_dvt.sensor_name"),
            col("st_dvt.sensor_type"),
            col("st_dvt.DeviceTyId"),
            col("st_dvt.SubLocationId"),
            col("st_dvt.device_name"),
            col("st_dvt.device_type"),
            col("sl.SubLocationTyId"),
            col("st_dvt.Active"),
            col("sl.Name").alias("sublocation_name"),
            col("sl.LocationId")
        )
        .alias("st_dvt_sl")
        .join(dbo_locations_df.alias("loca"), on="LocationId", how="left")
        .select(
            col("st_dvt_sl.SensorId"),
            col("st_dvt_sl.SensorTyId"),
            col("st_dvt_sl.DeviceId"),
            col("st_dvt_sl.sensor_name"),
            col("st_dvt_sl.sensor_type"),
            col("st_dvt_sl.DeviceTyId"),
            col("st_dvt_sl.SubLocationId"),
            col("st_dvt_sl.device_name"),
            col("st_dvt_sl.device_type"),
            col("st_dvt_sl.SubLocationTyId"),
            col("st_dvt_sl.Active"),
            col("st_dvt_sl.sublocation_name"),
            col("st_dvt_sl.LocationId"),
            col("loca.Name").alias("location_name"),
            col("loca.CityId")
        )
        .alias("st_dvt_sl_l")
        .join(dbo_sublocation_tys_df.alias("slty"), on="SubLocationTyId", how="left")
        .select(
            col("st_dvt_sl_l.SensorId"),
            col("st_dvt_sl_l.SensorTyId"),
            col("st_dvt_sl_l.DeviceId"),
            col("st_dvt_sl_l.sensor_name"),
            col("st_dvt_sl_l.sensor_type"),
            col("st_dvt_sl_l.DeviceTyId"),
            col("st_dvt_sl_l.SubLocationId"),
            col("st_dvt_sl_l.device_name"),
            col("st_dvt_sl_l.device_type"),
            col("st_dvt_sl_l.SubLocationTyId"),
            col("st_dvt_sl_l.sublocation_name"),
            col("st_dvt_sl_l.LocationId"),
            col("st_dvt_sl_l.Active"),
            col("st_dvt_sl_l.location_name"),
            col("st_dvt_sl_l.CityId"),
            col("slty.Name").alias("sublocation_type")
        )
        .alias("st_dvt_sl_slt")
        .join(dbo_cities_df.alias("city"), on="CityId", how="left")
        .select(
            col("st_dvt_sl_slt.SensorId"),
            col("st_dvt_sl_slt.SensorTyId"),
            col("st_dvt_sl_slt.DeviceId"),
            col("st_dvt_sl_slt.sensor_name"),
            col("st_dvt_sl_slt.sensor_type"),
            col("st_dvt_sl_slt.DeviceTyId"),
            col("st_dvt_sl_slt.SubLocationId"),
            col("st_dvt_sl_slt.device_name"),
            col("st_dvt_sl_slt.device_type"),
            col("st_dvt_sl_slt.SubLocationTyId"),
            col("st_dvt_sl_slt.sublocation_name"),
            col("st_dvt_sl_slt.LocationId"),
            col("st_dvt_sl_slt.Active"),
            col("st_dvt_sl_slt.location_name"),
            col("st_dvt_sl_slt.CityId"),
            col("st_dvt_sl_slt.sublocation_type"),
            col("city.Name").alias("city_name"),
        )
    )

    if sensors_devices_locations_df.rdd.isEmpty():
        return None

    return sensors_devices_locations_df
