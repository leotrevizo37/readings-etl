from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

if TYPE_CHECKING:
    from ..resources import SparkResource, SqlDimServerResource


HVAC_DEVICE_TYPES = (
    "A/C de Cocina",
    "A/C de Comedor",
    "Ambiente playland",
)
HVAC_TEMPERATURE_SENSOR_TYPE = "Temperatura"
HVAC_SWITCH_SENSOR_TYPE = "On/Off Termostato"
LIGHT_DEVICE_TYPE = "Luminaria"
LIGHT_SENSOR_TYPE = "Encendido"
_DAY_REPLACEMENTS = (
    (r"mon", "1"),
    (r"tue", "2"),
    (r"wed", "3"),
    (r"thu", "4"),
    (r"fri", "5"),
    (r"sat", "6"),
    (r"sun", "7"),
)


def _internal_expected_values_schema() -> StructType:
    return StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("min_expected_value", DoubleType(), True),
            StructField("max_expected_value", DoubleType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("scheduled_days", StringType(), True),
        ]
    )


def _final_expected_values_schema() -> StructType:
    return StructType(
        [
            StructField("SensorId", StringType(), True),
            StructField("DeviceId", StringType(), True),
            StructField("min_expected_value", DecimalType(5, 3), True),
            StructField("max_expected_value", DecimalType(5, 3), True),
            StructField("StartTime", StringType(), True),
            StructField("EndTime", StringType(), True),
            StructField("scheduledDays", StringType(), True),
        ]
    )


def _empty_internal_expected_values_df(spark) -> DataFrame:
    return spark.createDataFrame([], schema=_internal_expected_values_schema())


def _empty_final_expected_values_df(spark) -> DataFrame:
    return spark.createDataFrame([], schema=_final_expected_values_schema())


def _read_dim_source_table(
    spark_res: SparkResource,
    dim_sqlserver: SqlDimServerResource,
    table_name: str,
) -> DataFrame:
    spark = spark_res.get_spark()

    return (
        spark.read.format("jdbc")
        .option("dbtable", f"dbo.{table_name}")
        .options(**dim_sqlserver.get_basic_options())
        .load()
    )


def _replace_day_tokens(column) -> F.Column:
    normalized = F.lower(F.trim(column.cast("string")))
    for pattern, replacement in _DAY_REPLACEMENTS:
        normalized = F.regexp_replace(normalized, pattern, replacement)
    return normalized


def _normalize_days(column) -> F.Column:
    normalized = _replace_day_tokens(column)
    return (
        F.when(normalized == F.lit("*"), F.lit("1-7"))
        .when(normalized == F.lit("1,2,3,4,5,6,7"), F.lit("1-7"))
        .otherwise(normalized)
    )


def _extract_hhmm(column_name: str) -> F.Column:
    text_value = F.trim(F.col(column_name).cast("string"))
    extracted_value = F.regexp_extract(text_value, r"(\d{1,2}:\d{2})(?::\d{2})?", 1)
    extracted_parts = F.split(extracted_value, ":")
    return F.when(
        extracted_value != "",
        F.format_string(
            "%02d:%02d",
            extracted_parts.getItem(0).cast("int"),
            extracted_parts.getItem(1).cast("int"),
        ),
    )


def _append_seconds(column_name: str) -> F.Column:
    return F.when(
        F.col(column_name).isNotNull(),
        F.concat(F.col(column_name), F.lit(":00")),
    )


def _split_cross_midnight(df: DataFrame) -> DataFrame:
    crosses_midnight = df.filter(F.col("start_time") > F.col("end_time"))
    does_not_cross = df.filter(F.col("start_time") <= F.col("end_time"))

    before_midnight = (
        crosses_midnight
        .filter(F.col("start_time") != F.lit("23:59"))
        .withColumn("end_time", F.lit("23:59"))
    )

    after_midnight = (
        crosses_midnight
        .filter(F.col("end_time") != F.lit("00:00"))
        .withColumn("start_time", F.lit("00:00"))
        .withColumn(
            "end_time",
            F.date_format(
                F.to_timestamp(
                    F.concat(F.lit("2000-01-01 "), F.col("end_time"), F.lit(":00")),
                    "yyyy-MM-dd HH:mm:ss",
                )
                - F.expr("INTERVAL 1 MINUTE"),
                "HH:mm",
            ),
        )
    )

    return does_not_cross.unionByName(before_midnight).unionByName(after_midnight)


def build_hvac_programs(
    program_profiles_df: DataFrame,
    program_schedules_df: DataFrame,
    program_profile_tys_df: DataFrame,
) -> DataFrame:
    return (
        program_profiles_df.alias("pf")
        .filter(F.coalesce(F.col("pf.Active"), F.lit(False)))
        .join(
            program_schedules_df.alias("ps")
            .filter(F.coalesce(F.col("ps.Active"), F.lit(False)))
            .dropDuplicates(["ProgramProfileId", "ScheduleCode", "Days", "Time"]),
            on="ProgramProfileId",
            how="inner",
        )
        .join(
            program_profile_tys_df.alias("pty")
            .filter(F.coalesce(F.col("pty.Active"), F.lit(False))),
            on="ProgramProfileTyId",
            how="left",
        )
        .select(
            F.col("pf.DeviceId").alias("device_id"),
            F.col("pf.Mode"),
            (F.col("pf.Temperature") * F.lit(9.0 / 5.0) + F.lit(32.0)).alias(
                "Temperature"
            ),
            (F.col("pf.TemperatureGrace") * F.lit(9.0 / 5.0)).alias(
                "temperature_grace"
            ),
            F.col("ps.Days"),
            _extract_hhmm("ps.Time").alias("Time"),
            F.lower(F.trim(F.col("pty.Name"))).alias("profile_type_name"),
        )
        .filter(F.col("Time").isNotNull())
        .dropDuplicates(
            [
                "device_id",
                "Mode",
                "Temperature",
                "temperature_grace",
                "Days",
                "Time",
                "profile_type_name",
            ]
        )
    )


def build_hvac_expected_values(
    hvac_programs_df: DataFrame,
    dimensions_df: DataFrame,
) -> DataFrame:
    spark = dimensions_df.sparkSession
    active_dimensions = dimensions_df.filter(F.coalesce(F.col("Active"), F.lit(False)))
    hvac_dimensions = active_dimensions.filter(F.col("device_type").isin(*HVAC_DEVICE_TYPES))

    temperature_dimensions = hvac_dimensions.filter(
        F.col("sensor_type") == F.lit(HVAC_TEMPERATURE_SENSOR_TYPE)
    ).select(
        F.col("DeviceId").alias("device_id"),
        F.col("SensorId").alias("sensor_id"),
    )

    switch_dimensions = hvac_dimensions.filter(
        F.col("sensor_type") == F.lit(HVAC_SWITCH_SENSOR_TYPE)
    ).select(
        F.col("DeviceId").alias("device_id"),
        F.col("SensorId").alias("sensor_id"),
    )

    programs_prepared = (
        hvac_programs_df
        .withColumn("scheduled_days", F.lower(F.trim(F.col("Days").cast("string"))))
        .withColumn("time_value", _extract_hhmm("Time"))
        .filter(F.col("time_value").isNotNull())
        .withColumn(
            "is_off_profile",
            F.coalesce(F.col("profile_type_name") == F.lit("off"), F.lit(False)),
        )
        .withColumn(
            "time_ts",
            F.to_timestamp(
                F.concat(F.lit("2000-01-01 "), F.col("time_value"), F.lit(":00")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
    )

    if programs_prepared.rdd.isEmpty():
        return _empty_internal_expected_values_df(spark)

    ordered_window = Window.partitionBy("device_id", "scheduled_days").orderBy("time_ts")
    grouped_window = Window.partitionBy("device_id", "scheduled_days")

    temperature_status = (
        programs_prepared
        .join(temperature_dimensions, on="device_id", how="inner")
        .withColumn("start_time", F.col("time_value"))
        .withColumn("next_time", F.lead("time_value").over(ordered_window))
        .withColumn("first_time", F.first("time_value").over(grouped_window))
        .withColumn("end_time", F.coalesce(F.col("next_time"), F.col("first_time")))
        .select(
            "sensor_id",
            "device_id",
            F.when(
                (~F.col("is_off_profile"))
                & F.col("Temperature").isNotNull()
                & F.col("temperature_grace").isNotNull(),
                (F.col("Temperature") - F.col("temperature_grace")).cast("double"),
            ).otherwise(F.lit(None).cast("double")).alias("min_expected_value"),
            F.when(
                (~F.col("is_off_profile"))
                & F.col("Temperature").isNotNull()
                & F.col("temperature_grace").isNotNull(),
                (F.col("Temperature") + F.col("temperature_grace")).cast("double"),
            ).otherwise(F.lit(None).cast("double")).alias("max_expected_value"),
            "start_time",
            "end_time",
            "scheduled_days",
        )
    )

    switch_status = (
        programs_prepared
        .join(switch_dimensions, on="device_id", how="inner")
        .withColumn("start_time", F.col("time_value"))
        .withColumn("next_time", F.lead("time_value").over(ordered_window))
        .withColumn("first_time", F.first("time_value").over(grouped_window))
        .withColumn("end_time", F.coalesce(F.col("next_time"), F.col("first_time")))
        .withColumn(
            "min_expected_value",
            F.when(F.col("is_off_profile"), F.lit(0.0)).otherwise(F.lit(1.0)),
        )
        .withColumn("max_expected_value", F.col("min_expected_value"))
        .select(
            "sensor_id",
            "device_id",
            "min_expected_value",
            "max_expected_value",
            "start_time",
            "end_time",
            "scheduled_days",
        )
    )

    return (
        _split_cross_midnight(temperature_status.unionByName(switch_status))
        .withColumn("scheduled_days", _normalize_days(F.col("scheduled_days")))
        .dropDuplicates(
            [
                "sensor_id",
                "device_id",
                "start_time",
                "end_time",
                "scheduled_days",
                "min_expected_value",
                "max_expected_value",
            ]
        )
    )


def build_lights_programs(
    group_has_schedules_df: DataFrame,
    group_sensors_df: DataFrame,
    group_has_sensors_df: DataFrame,
) -> DataFrame:
    return (
        group_has_sensors_df.alias("ghs")
        .join(
            group_sensors_df.alias("gs").filter(F.coalesce(F.col("gs.Active"), F.lit(False))),
            on="GroupSensorsId",
            how="inner",
        )
        .join(
            group_has_schedules_df.alias("sc").filter(
                F.coalesce(F.col("sc.Active"), F.lit(False))
            ),
            on="GroupSensorsId",
            how="inner",
        )
        .select(
            F.col("ghs.SensorsSensorId").alias("sensor_id"),
            F.col("sc.Crone"),
            F.col("sc.Value"),
        )
        .dropDuplicates(["sensor_id", "Crone", "Value"])
    )


def build_lights_expected_values(
    lights_programs_df: DataFrame,
    dimensions_df: DataFrame,
) -> DataFrame:
    spark = dimensions_df.sparkSession
    active_dimensions = dimensions_df.filter(F.coalesce(F.col("Active"), F.lit(False)))
    lights_dimensions = active_dimensions.filter(
        (F.col("device_type") == F.lit(LIGHT_DEVICE_TYPE))
        & (F.col("sensor_type") == F.lit(LIGHT_SENSOR_TYPE))
    ).select(
        F.col("SensorId").alias("sensor_id"),
        F.col("DeviceId").alias("device_id"),
    )

    cron_parts = F.split(F.trim(F.col("Crone").cast("string")), r"\s+")
    lights_parsed = (
        lights_programs_df
        .select(
            "sensor_id",
            F.when(F.col("Value").cast("boolean"), F.lit(1.0)).otherwise(F.lit(0.0)).alias(
                "is_on"
            ),
            cron_parts.getItem(0).alias("cron_minute"),
            cron_parts.getItem(1).alias("cron_hour"),
            cron_parts.getItem(4).alias("cron_dow"),
        )
        .withColumn("cron_minute_int", F.col("cron_minute").cast("int"))
        .withColumn("cron_hour_int", F.col("cron_hour").cast("int"))
        .filter(
            F.col("sensor_id").isNotNull()
            & F.col("cron_minute_int").isNotNull()
            & F.col("cron_hour_int").isNotNull()
            & F.col("cron_dow").isNotNull()
        )
        .withColumn(
            "start_time_candidate",
            F.format_string(
                "%02d:%02d",
                F.col("cron_hour_int"),
                F.col("cron_minute_int"),
            ),
        )
        .withColumn(
            "days_candidate",
            _normalize_days(F.regexp_replace(F.col("cron_dow"), r"\?", "*")),
        )
        .withColumn(
            "week_order",
            (
                (
                    F.when(F.col("days_candidate") == F.lit("1-7"), F.lit(1))
                    .otherwise(
                        F.regexp_extract(F.col("days_candidate"), r"^(\d+)", 1).cast("int")
                    )
                    - F.lit(1)
                )
                * F.lit(1440)
                + F.col("cron_hour_int") * F.lit(60)
                + F.col("cron_minute_int")
            ),
        )
    )

    if lights_parsed.rdd.isEmpty():
        return _empty_internal_expected_values_df(spark)

    ordered_window = Window.partitionBy("sensor_id").orderBy("week_order")

    lights_paired = (
        lights_parsed
        .withColumn(
            "end_time_candidate",
            F.coalesce(
                F.lead("start_time_candidate").over(ordered_window),
                F.first("start_time_candidate").over(ordered_window),
            ),
        )
    )

    return (
        _split_cross_midnight(
            lights_paired
            .join(lights_dimensions, on="sensor_id", how="inner")
            .withColumn("min_expected_value", F.col("is_on"))
            .withColumn("max_expected_value", F.col("is_on"))
            .select(
                "sensor_id",
                "device_id",
                "min_expected_value",
                "max_expected_value",
                F.col("start_time_candidate").alias("start_time"),
                F.col("end_time_candidate").alias("end_time"),
                F.col("days_candidate").alias("scheduled_days"),
            )
        )
        .dropDuplicates(
            [
                "sensor_id",
                "device_id",
                "start_time",
                "end_time",
                "scheduled_days",
                "min_expected_value",
                "max_expected_value",
            ]
        )
    )


def build_sensor_expected_values_from_dataframes(
    dimensions_df: DataFrame,
    program_profiles_df: DataFrame,
    program_schedules_df: DataFrame,
    program_profile_tys_df: DataFrame,
    group_sensors_df: DataFrame,
    group_has_schedules_df: DataFrame,
    group_has_sensors_df: DataFrame,
) -> DataFrame:
    spark = dimensions_df.sparkSession

    hvac_expected_values = build_hvac_expected_values(
        build_hvac_programs(
            program_profiles_df,
            program_schedules_df,
            program_profile_tys_df,
        ),
        dimensions_df,
    )
    lights_expected_values = build_lights_expected_values(
        build_lights_programs(
            group_has_schedules_df,
            group_sensors_df,
            group_has_sensors_df,
        ),
        dimensions_df,
    )

    combined = (
        hvac_expected_values.unionByName(lights_expected_values)
        .filter(
            F.col("sensor_id").isNotNull()
            & F.col("device_id").isNotNull()
            & F.col("start_time").isNotNull()
            & F.col("end_time").isNotNull()
            & F.col("scheduled_days").isNotNull()
        )
        .dropDuplicates(
            [
                "sensor_id",
                "device_id",
                "start_time",
                "end_time",
                "scheduled_days",
                "min_expected_value",
                "max_expected_value",
            ]
        )
    )

    if combined.rdd.isEmpty():
        return _empty_final_expected_values_df(spark)

    return (
        combined.select(
            F.col("sensor_id").cast("string").alias("SensorId"),
            F.col("device_id").cast("string").alias("DeviceId"),
            F.round(F.col("min_expected_value"), 3)
            .cast(DecimalType(5, 3))
            .alias("min_expected_value"),
            F.round(F.col("max_expected_value"), 3)
            .cast(DecimalType(5, 3))
            .alias("max_expected_value"),
            _append_seconds("start_time").alias("StartTime"),
            _append_seconds("end_time").alias("EndTime"),
            F.col("scheduled_days").cast("string").alias("scheduledDays"),
        )
        .dropDuplicates(
            [
                "SensorId",
                "DeviceId",
                "StartTime",
                "EndTime",
                "scheduledDays",
                "min_expected_value",
                "max_expected_value",
            ]
        )
    )


def sensor_expected_values(
    spark_res: SparkResource,
    dim_sqlserver: SqlDimServerResource,
) -> DataFrame:
    from .raw_dimensions import raw_dimensions

    spark = spark_res.get_spark()
    dimensions_df = raw_dimensions(spark_res, dim_sqlserver)

    if dimensions_df is None:
        return _empty_final_expected_values_df(spark)

    program_profile_tys_df = _read_dim_source_table(
        spark_res,
        dim_sqlserver,
        "ProgramProfileTys",
    )
    program_profiles_df = _read_dim_source_table(spark_res, dim_sqlserver, "ProgramProfiles")
    program_schedules_df = _read_dim_source_table(spark_res, dim_sqlserver, "ProgramSchedules")
    group_sensors_df = _read_dim_source_table(spark_res, dim_sqlserver, "GroupSensors")
    group_has_schedules_df = _read_dim_source_table(spark_res, dim_sqlserver, "GroupHasSchedules")
    group_has_sensors_df = _read_dim_source_table(spark_res, dim_sqlserver, "GroupHasSensors")

    return build_sensor_expected_values_from_dataframes(
        dimensions_df=dimensions_df,
        program_profile_tys_df=program_profile_tys_df,
        program_profiles_df=program_profiles_df,
        program_schedules_df=program_schedules_df,
        group_sensors_df=group_sensors_df,
        group_has_schedules_df=group_has_schedules_df,
        group_has_sensors_df=group_has_sensors_df,
    )
