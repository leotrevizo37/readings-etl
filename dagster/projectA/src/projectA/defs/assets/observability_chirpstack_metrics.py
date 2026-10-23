from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from time import perf_counter

import pyodbc
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dagster import AssetExecutionContext, AssetIn, AssetKey, AssetObservation, MaterializeResult, Output, RetryPolicy, asset

from ...operations_window import OPERATIONS_TIMEZONE
from ..resources import (
    ChirpStackApiResource,
    MqttEventHistoryApiResource,
    SparkResource,
    SqlServerResource,
)
from ..transformations.chirpstack_metrics import (
    build_device_event_metric_rows,
    build_device_metric_row,
    build_gateway_metric_row,
    device_profile_supports_battery,
    device_references,
    normalize_dev_eui,
)

INGESTION_NAME = "factChirpStackMetrics"
MQTT_INGESTION_NAME = "factChirpStackMqttMetrics"
CHIRPSTACK_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)
DEVICE_METRICS_SCHEMA = StructType(
    [
        StructField("DevEui", StringType(), False),
        StructField("TimeSpan", TimestampType(), False),
        StructField("TenantId", StringType(), False),
        StructField("ApplicationId", StringType(), False),
        StructField("DeviceProfileId", StringType(), False),
        StructField("UplinkIntervalSeconds", IntegerType(), True),
        StructField("ExpectedUplinkCount", DecimalType(18, 6), True),
        StructField("ReceivedUplinkCount", LongType(), True),
        StructField("AverageRssi", DecimalType(18, 6), True),
        StructField("AverageSnr", DecimalType(18, 6), True),
        StructField("LinkErrorCount", LongType(), True),
        StructField("LinkErrorCountsJson", StringType(), True),
        StructField("BatteryLevel", DecimalType(7, 4), True),
        StructField("ExternalPowerSource", BooleanType(), True),
        StructField("BatteryLevelSource", StringType(), True),
        StructField("BatteryObservedAt", TimestampType(), True),
        StructField("MetricsAvailable", BooleanType(), False),
        StructField("ExtractedAt", TimestampType(), False),
    ]
)
GATEWAY_METRICS_SCHEMA = StructType(
    [
        StructField("GatewayId", StringType(), False),
        StructField("TimeSpan", TimestampType(), False),
        StructField("TenantId", StringType(), False),
        StructField("GatewayName", StringType(), False),
        StructField("ReceivedPacketCount", LongType(), True),
        StructField("TransmittedPacketCount", LongType(), True),
        StructField("TxOkCount", LongType(), True),
        StructField("TxErrorCount", LongType(), True),
        StructField("TxPacketStatusCountsJson", StringType(), True),
        StructField("GatewayState", StringType(), False),
        StructField("GatewayLastSeenAt", TimestampType(), True),
        StructField("StatsIntervalSeconds", IntegerType(), True),
        StructField("MetricsAvailable", BooleanType(), False),
        StructField("ExtractedAt", TimestampType(), False),
    ]
)
DEVICE_EVENT_METRICS_SCHEMA = StructType(
    [
        StructField("DevEui", StringType(), False),
        StructField("TimeSpan", TimestampType(), False),
        StructField("TenantId", StringType(), False),
        StructField("CapturedUplinkCount", LongType(), False),
        StructField("ObservedGatewayCount", IntegerType(), False),
        StructField("MultiGatewayUplinkCount", LongType(), False),
        StructField("EventLogCountsJson", StringType(), False),
        StructField("RadioProfileCountsJson", StringType(), False),
        StructField("UplinkContextCountsJson", StringType(), False),
        StructField("RadioFieldQualityJson", StringType(), False),
        StructField("FrameCounterSummaryJson", StringType(), False),
        StructField("ExtractedAt", TimestampType(), False),
    ]
)
DEVICE_GATEWAY_METRICS_SCHEMA = StructType(
    [
        StructField("DevEui", StringType(), False),
        StructField("GatewayId", StringType(), False),
        StructField("TimeSpan", TimestampType(), False),
        StructField("TenantId", StringType(), False),
        StructField("GatewayReceptionCount", LongType(), False),
        StructField("ReceptionShare", DecimalType(18, 6), False),
        StructField("AverageRssi", DecimalType(18, 6), True),
        StructField("MinRssi", DecimalType(18, 6), True),
        StructField("MaxRssi", DecimalType(18, 6), True),
        StructField("AverageSnr", DecimalType(18, 6), True),
        StructField("MinSnr", DecimalType(18, 6), True),
        StructField("MaxSnr", DecimalType(18, 6), True),
        StructField("RadioReceptionStatsJson", StringType(), False),
        StructField("ReceptionFieldQualityJson", StringType(), False),
        StructField("FirstReceivedAt", TimestampType(), False),
        StructField("LastReceivedAt", TimestampType(), False),
        StructField("ExtractedAt", TimestampType(), False),
    ]
)


@asset(
    name="observability_chirpstackmetrics",
    group_name="projectA",
    key_prefix=["projectA"],
    required_resource_keys={
        "chirpstack_api",
        "spark",
        "sqlserver",
    },
    retry_policy=CHIRPSTACK_RETRY_POLICY,
)
def observability_chirpstackmetrics(
    context: AssetExecutionContext,
) -> Output:
    start_time = perf_counter()
    chirpstack: ChirpStackApiResource = context.resources.chirpstack_api
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    current_hour_utc = extracted_at.replace(minute=0, second=0, microsecond=0)
    to_utc = extracted_at
    last_loaded_at = sql.get_watermark(INGESTION_NAME)
    if last_loaded_at is not None and last_loaded_at.tzinfo is not None:
        last_loaded_at = (
            last_loaded_at
            .astimezone(timezone.utc)
            .replace(tzinfo=None, microsecond=0)
        )
    processing_anchor = min(
        last_loaded_at or current_hour_utc - timedelta(hours=1),
        current_hour_utc,
    )
    from_utc = processing_anchor.replace(minute=0, second=0, microsecond=0)
    sql.mark_run_started(INGESTION_NAME)
    run_id = context.run_id
    device_staged_rows = 0
    gateway_staged_rows = 0
    device_merged_rows = 0
    gateway_merged_rows = 0
    recovered_from_utc = from_utc

    try:
        sql.clear_chirpstack_metrics_stage(run_id)
        dimension_rows = (
            spark_res.read_dim_prod_dimensions()
            .filter(col("Active") == lit(True))
            .filter(col("PhysicalIdentifier").isNotNull())
            .select("PhysicalIdentifier")
            .distinct()
            .collect()
        )
        dev_euis = set()
        invalid_physical_identifiers = []
        for row in dimension_rows:
            physical_identifier = row["PhysicalIdentifier"]
            try:
                dev_euis.add(normalize_dev_eui(physical_identifier))
            except (TypeError, ValueError):
                invalid_physical_identifiers.append(str(physical_identifier))
        dev_euis = sorted(dev_euis)
        invalid_physical_identifiers = sorted(set(invalid_physical_identifiers))
        if not dev_euis:
            raise RuntimeError("No active ChirpStack DevEUIs were found in dimensions")

        time_spans = []
        time_span = from_utc
        while time_span < to_utc:
            time_spans.append(time_span)
            time_span += timedelta(hours=1)

        applications = {}
        device_profiles = {}
        device_rows = []
        devices = {}
        for dev_eui in dev_euis:
            device_payload = chirpstack.get_device(dev_eui)
            application_id, device_profile_id = device_references(
                device_payload,
                dev_eui,
            )
            if application_id not in applications:
                applications[application_id] = chirpstack.get_application(
                    application_id
                )
            if device_profile_id not in device_profiles:
                device_profiles[device_profile_id] = chirpstack.get_device_profile(
                    device_profile_id
                )
            link_metrics_payload = chirpstack.get_device_link_metrics(
                dev_eui,
                from_utc,
                to_utc,
            )
            device_metrics_payload = None
            if device_profile_supports_battery(
                device_profiles[device_profile_id]
            ):
                device_metrics_payload = chirpstack.get_device_metrics(
                    dev_eui,
                    from_utc,
                    to_utc,
                )
            for time_span in time_spans:
                row = build_device_metric_row(
                    dev_eui=dev_eui,
                    device_payload=(
                        device_payload
                        if time_span == current_hour_utc
                        else {
                            key: value
                            for key, value in device_payload.items()
                            if key != "deviceStatus"
                        }
                    ),
                    application_payload=applications[application_id],
                    device_profile_payload=device_profiles[device_profile_id],
                    link_metrics_payload=link_metrics_payload,
                    device_metrics_payload=device_metrics_payload,
                    time_span=time_span,
                    extracted_at=extracted_at,
                )
                devices[dev_eui] = row["TenantId"]
                device_rows.append(row)

        gateway_rows = []
        gateway_ids = set()
        for tenant_id in sorted(set(devices.values())):
            tenant_gateways = chirpstack.list_gateways(tenant_id)
            if not tenant_gateways:
                raise RuntimeError(
                    "ChirpStack returned no gateways for a monitored device tenant"
                )
            for gateway_list_item in tenant_gateways:
                gateway_id = normalize_dev_eui(gateway_list_item.get("gatewayId"))
                if gateway_id in gateway_ids:
                    raise RuntimeError("ChirpStack returned a duplicate gatewayId")
                gateway_ids.add(gateway_id)
                gateway_payload = chirpstack.get_gateway(gateway_id)
                gateway_metrics_payload = chirpstack.get_gateway_metrics(
                    gateway_id,
                    from_utc,
                    to_utc,
                )
                for time_span in time_spans:
                    gateway_rows.append(
                        build_gateway_metric_row(
                            gateway_list_item=gateway_list_item,
                            gateway_payload=gateway_payload,
                            gateway_metrics_payload=gateway_metrics_payload,
                            expected_tenant_id=tenant_id,
                            time_span=time_span,
                            extracted_at=extracted_at,
                        )
                    )

        if from_utc < current_hour_utc - timedelta(hours=1):
            evidence_time_spans = [
                row["TimeSpan"]
                for row in device_rows
                if (
                    (row["ReceivedUplinkCount"] or 0) > 0
                    or (row["LinkErrorCount"] or 0) > 0
                    or row["AverageRssi"] not in (None, 0)
                    or row["AverageSnr"] not in (None, 0)
                )
            ] + [
                row["TimeSpan"]
                for row in gateway_rows
                if (
                    (row["ReceivedPacketCount"] or 0) > 0
                    or (row["TransmittedPacketCount"] or 0) > 0
                    or (row["TxOkCount"] or 0) > 0
                    or (row["TxErrorCount"] or 0) > 0
                )
            ]
            recovered_from_utc = (
                min(evidence_time_spans) if evidence_time_spans else to_utc
            )
            device_rows = [
                row for row in device_rows
                if row["TimeSpan"] >= recovered_from_utc
            ]
            gateway_rows = [
                row for row in gateway_rows
                if row["TimeSpan"] >= recovered_from_utc
            ]

        spark = spark_res.get_spark()
        device_df = spark.createDataFrame(device_rows, DEVICE_METRICS_SCHEMA)
        gateway_df = spark.createDataFrame(gateway_rows, GATEWAY_METRICS_SCHEMA)
        device_staged_rows = spark_res.write_chirpstack_device_metrics_stage(
            device_df,
            run_id,
        )
        gateway_staged_rows = spark_res.write_chirpstack_gateway_metrics_stage(
            gateway_df,
            run_id,
        )
        (
            device_merged_rows,
            gateway_merged_rows,
        ) = sql.merge_chirpstack_metrics(run_id)
        sql.clear_chirpstack_metrics_stage(run_id)
        sql.mark_run_completed(
            ingestion_name=INGESTION_NAME,
            new_last_loaded_at=extracted_at,
            succeeded=True,
            error_message=None,
        )
    except Exception as ex:
        try:
            sql.clear_chirpstack_metrics_stage(run_id)
        except pyodbc.Error:
            context.log.warning("Could not clear ChirpStack staging after the failed run")
        try:
            sql.mark_run_completed(
                ingestion_name=INGESTION_NAME,
                new_last_loaded_at=last_loaded_at,
                succeeded=False,
                error_message=str(ex),
            )
        except pyodbc.Error:
            context.log.warning("Could not persist the failed ChirpStack run state")
        raise

    return Output(
        value={
            "devices": devices,
            "original_last_loaded_at": last_loaded_at,
        },
        metadata={
            "device_row_count": device_merged_rows,
            "device_staged_rows": device_staged_rows,
            "gateway_row_count": gateway_merged_rows,
            "gateway_staged_rows": gateway_staged_rows,
            "valid_dev_eui_count": len(dev_euis),
            "invalid_physical_identifier_count": len(invalid_physical_identifiers),
            "invalid_physical_identifiers": invalid_physical_identifiers,
            "from_utc": str(from_utc),
            "to_utc": str(to_utc),
            "recovered_from_utc": str(recovered_from_utc),
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        }
    )


@asset(
    name="observability_chirpstackmqttmetrics",
    output_required=False,
    group_name="projectA",
    key_prefix=["projectA"],
    ins={
        "chirpstack_scope": AssetIn(
            key=AssetKey(["projectA", "observability_chirpstackmetrics"])
        )
    },
    required_resource_keys={
        "mqtt_event_history_api",
        "spark",
        "sqlserver",
    },
    retry_policy=CHIRPSTACK_RETRY_POLICY,
)
def observability_chirpstackmqttmetrics(
    context: AssetExecutionContext,
    chirpstack_scope: dict,
) -> Iterator[MaterializeResult]:
    start_time = perf_counter()
    mqtt_history: MqttEventHistoryApiResource = (
        context.resources.mqtt_event_history_api
    )
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    last_loaded_at = sql.get_watermark(MQTT_INGESTION_NAME)
    if last_loaded_at is None:
        last_loaded_at = chirpstack_scope["original_last_loaded_at"]
    if last_loaded_at is not None and last_loaded_at.tzinfo is not None:
        last_loaded_at = (
            last_loaded_at
            .astimezone(timezone.utc)
            .replace(tzinfo=None, microsecond=0)
        )
    processing_anchor = min(last_loaded_at or extracted_at, extracted_at)
    from_utc = (
        processing_anchor.replace(minute=0, second=0, microsecond=0)
        - timedelta(hours=mqtt_history.lookback_hours - 1)
    )
    to_utc = extracted_at
    windows = []
    window_from_utc = from_utc
    while window_from_utc < to_utc:
        window_to_utc = window_from_utc + timedelta(hours=1)
        window_from_local = window_from_utc.replace(tzinfo=timezone.utc).astimezone(
            OPERATIONS_TIMEZONE
        )
        window_to_local = window_to_utc.replace(tzinfo=timezone.utc).astimezone(
            OPERATIONS_TIMEZONE
        )
        offset_decrease = window_from_local.utcoffset() - window_to_local.utcoffset()
        if offset_decrease > timedelta(0):
            window_to_utc += offset_decrease
        if (
            (offset_decrease > timedelta(0) or window_from_local.fold)
            and window_to_utc > to_utc
        ):
            context.log.info(
                f"Deferring ChirpStack MQTT until {window_to_utc} UTC "
                "to recover the complete repeated local hour; watermark unchanged"
            )
            context.log_event(AssetObservation(
                asset_key=context.asset_key,
                metadata={
                    "status": "DEFERRED_DST",
                    "resume_after_utc": str(window_to_utc),
                    "from_utc": str(from_utc),
                    "to_utc": str(to_utc),
                },
            ))
            return
        window_to_utc = min(window_to_utc, to_utc)
        windows.append((
            window_from_local.replace(tzinfo=None),
            window_to_utc.replace(tzinfo=timezone.utc)
            .astimezone(OPERATIONS_TIMEZONE)
            .replace(tzinfo=None),
        ))
        window_from_utc = window_to_utc
    sql.mark_run_started(MQTT_INGESTION_NAME)
    run_id = context.run_id
    device_event_staged_rows = 0
    device_gateway_staged_rows = 0
    device_event_merged_rows = 0
    device_gateway_merged_rows = 0
    mqtt_event_rejections_persisted = 0
    mqtt_event_rejections_expired = 0

    try:
        sql.clear_chirpstack_mqtt_metrics_stage(run_id)
        event_rows = []
        for window_from_local, window_to_local in windows:
            event_rows.extend(
                mqtt_history.fetch_range(
                    window_from_local,
                    window_to_local,
                )
            )

        (
            device_event_rows,
            device_gateway_rows,
            transformation_rejected_rows,
        ) = build_device_event_metric_rows(
            event_rows=event_rows,
            devices=chirpstack_scope["devices"],
            from_utc=from_utc,
            to_utc=to_utc,
            extracted_at=extracted_at,
        )
        rejected_rows = [
            *mqtt_history.rejected_rows,
            *transformation_rejected_rows,
        ]
        (
            mqtt_event_rejections_persisted,
            mqtt_event_rejections_expired,
        ) = sql.write_mqtt_event_history_rejected_rows(
            rejected_rows,
            run_id,
            mqtt_history.rejected_row_retention_days,
        )

        spark = spark_res.get_spark()
        device_event_df = spark.createDataFrame(
            device_event_rows,
            DEVICE_EVENT_METRICS_SCHEMA,
        )
        device_gateway_df = spark.createDataFrame(
            device_gateway_rows,
            DEVICE_GATEWAY_METRICS_SCHEMA,
        )
        device_event_staged_rows = (
            spark_res.write_chirpstack_device_event_metrics_stage(
                device_event_df,
                run_id,
            )
        )
        device_gateway_staged_rows = (
            spark_res.write_chirpstack_device_gateway_metrics_stage(
                device_gateway_df,
                run_id,
            )
        )
        (
            device_event_merged_rows,
            device_gateway_merged_rows,
        ) = sql.merge_chirpstack_mqtt_metrics(run_id)
        sql.clear_chirpstack_mqtt_metrics_stage(run_id)
        sql.mark_run_completed(
            ingestion_name=MQTT_INGESTION_NAME,
            new_last_loaded_at=extracted_at,
            succeeded=True,
            error_message=None,
        )
    except Exception as ex:
        try:
            sql.clear_chirpstack_mqtt_metrics_stage(run_id)
        except pyodbc.Error:
            context.log.warning("Could not clear ChirpStack MQTT staging after the failed run")
        try:
            sql.mark_run_completed(
                ingestion_name=MQTT_INGESTION_NAME,
                new_last_loaded_at=last_loaded_at,
                succeeded=False,
                error_message=str(ex),
            )
        except pyodbc.Error:
            context.log.warning("Could not persist the failed ChirpStack MQTT run state")
        raise

    yield MaterializeResult(
        metadata={
            "device_event_row_count": device_event_merged_rows,
            "device_event_staged_rows": device_event_staged_rows,
            "device_gateway_row_count": device_gateway_merged_rows,
            "device_gateway_staged_rows": device_gateway_staged_rows,
            "mqtt_event_history_requests": mqtt_history.requests_made,
            "mqtt_event_windows_completed": mqtt_history.windows_completed,
            "mqtt_event_windows_split": mqtt_history.windows_split,
            "mqtt_event_rows_received": mqtt_history.rows_received,
            "mqtt_event_lookback_hours": mqtt_history.lookback_hours,
            "mqtt_event_rejected_rows": len(rejected_rows),
            "mqtt_event_rejections_persisted": mqtt_event_rejections_persisted,
            "mqtt_event_rejections_expired": mqtt_event_rejections_expired,
            "from_utc": str(from_utc),
            "to_utc": str(to_utc),
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        }
    )
