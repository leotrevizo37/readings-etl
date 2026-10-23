from datetime import datetime, timezone
from typing import Optional, Dict, Any

import os
import pyodbc
from dagster import resource


def build_dim_telemetry_prod_dimensions_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.dimTelemetryProdDimensions AS target
USING (
    SELECT
        SensorId,
        SensorTyId,
        DeviceId,
        PhysicalIdentifier,
        PerEvent,
        ModelId,
        SensorHubId,
        sensor_name,
        sensor_type,
        model_name,
        hub_model_name,
        hub_name,
        DeviceTyId,
        SubLocationId,
        Active,
        device_name,
        device_type,
        SubLocationTyId,
        sublocation_name,
        location_name,
        city_name,
        CityId,
        TimeZone,
        LocationId,
        sublocation_type
    FROM stg.dimTelemetryProdDimensions
    WHERE RunId = ?
) AS source
    ON target.SensorId = source.SensorId
WHEN MATCHED AND EXISTS (
    SELECT
        source.SensorTyId,
        source.DeviceId,
        source.PhysicalIdentifier,
        source.PerEvent,
        source.ModelId,
        source.SensorHubId,
        source.sensor_name,
        source.sensor_type,
        source.model_name,
        source.hub_model_name,
        source.hub_name,
        source.DeviceTyId,
        source.SubLocationId,
        source.Active,
        source.device_name,
        source.device_type,
        source.SubLocationTyId,
        source.sublocation_name,
        source.location_name,
        source.city_name,
        source.CityId,
        source.TimeZone,
        source.LocationId,
        source.sublocation_type
    EXCEPT
    SELECT
        target.SensorTyId,
        target.DeviceId,
        target.PhysicalIdentifier,
        target.PerEvent,
        target.ModelId,
        target.SensorHubId,
        target.sensor_name,
        target.sensor_type,
        target.model_name,
        target.hub_model_name,
        target.hub_name,
        target.DeviceTyId,
        target.SubLocationId,
        target.Active,
        target.device_name,
        target.device_type,
        target.SubLocationTyId,
        target.sublocation_name,
        target.location_name,
        target.city_name,
        target.CityId,
        target.TimeZone,
        target.LocationId,
        target.sublocation_type
) THEN
    UPDATE SET
        target.SensorTyId = source.SensorTyId,
        target.DeviceId = source.DeviceId,
        target.PhysicalIdentifier = source.PhysicalIdentifier,
        target.PerEvent = source.PerEvent,
        target.ModelId = source.ModelId,
        target.SensorHubId = source.SensorHubId,
        target.sensor_name = source.sensor_name,
        target.sensor_type = source.sensor_type,
        target.model_name = source.model_name,
        target.hub_model_name = source.hub_model_name,
        target.hub_name = source.hub_name,
        target.DeviceTyId = source.DeviceTyId,
        target.SubLocationId = source.SubLocationId,
        target.Active = source.Active,
        target.device_name = source.device_name,
        target.device_type = source.device_type,
        target.SubLocationTyId = source.SubLocationTyId,
        target.sublocation_name = source.sublocation_name,
        target.location_name = source.location_name,
        target.city_name = source.city_name,
        target.CityId = source.CityId,
        target.TimeZone = source.TimeZone,
        target.LocationId = source.LocationId,
        target.sublocation_type = source.sublocation_type,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        SensorId,
        SensorTyId,
        DeviceId,
        PhysicalIdentifier,
        PerEvent,
        ModelId,
        SensorHubId,
        sensor_name,
        sensor_type,
        model_name,
        hub_model_name,
        hub_name,
        DeviceTyId,
        SubLocationId,
        Active,
        device_name,
        device_type,
        SubLocationTyId,
        sublocation_name,
        location_name,
        city_name,
        CityId,
        TimeZone,
        LocationId,
        sublocation_type
    )
    VALUES (
        source.SensorId,
        source.SensorTyId,
        source.DeviceId,
        source.PhysicalIdentifier,
        source.PerEvent,
        source.ModelId,
        source.SensorHubId,
        source.sensor_name,
        source.sensor_type,
        source.model_name,
        source.hub_model_name,
        source.hub_name,
        source.DeviceTyId,
        source.SubLocationId,
        source.Active,
        source.device_name,
        source.device_type,
        source.SubLocationTyId,
        source.sublocation_name,
        source.location_name,
        source.city_name,
        source.CityId,
        source.TimeZone,
        source.LocationId,
        source.sublocation_type
    )
WHEN NOT MATCHED BY SOURCE AND target.Active = 1 THEN
    UPDATE SET
        target.Active = 0,
        target.ModifiedAt = SYSUTCDATETIME()
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_dim_aspnet_users_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.dimAspNetUsers AS target
USING (
    SELECT
        Id,
        UserName,
        Name,
        LastName,
        Position
    FROM stg.dimAspNetUsers
    WHERE RunId = ?
) AS source
    ON target.Id = source.Id
WHEN MATCHED AND EXISTS (
    SELECT
        source.UserName,
        source.Name,
        source.LastName,
        source.Position
    EXCEPT
    SELECT
        target.UserName,
        target.Name,
        target.LastName,
        target.Position
) THEN
    UPDATE SET
        target.UserName = source.UserName,
        target.Name = source.Name,
        target.LastName = source.LastName,
        target.Position = source.Position,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,
        UserName,
        Name,
        LastName,
        Position
    )
    VALUES (
        source.Id,
        source.UserName,
        source.Name,
        source.LastName,
        source.Position
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_telemetry_user_usage_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factTelemetryUserUsage AS target
USING (
    SELECT
        UserId,
        [Date],
        HasConnected,
        MadeCompleteInteraction,
        TimeConnected,
        AvgLatency,
        Latency95thPercentile
    FROM stg.factTelemetryUserUsage
    WHERE RunId = ?
) AS source
    ON target.[Date] = source.[Date]
    AND target.UserId = source.UserId
WHEN MATCHED AND EXISTS (
    SELECT
        source.HasConnected,
        source.MadeCompleteInteraction,
        source.TimeConnected,
        source.AvgLatency,
        source.Latency95thPercentile
    EXCEPT
    SELECT
        target.HasConnected,
        target.MadeCompleteInteraction,
        target.TimeConnected,
        target.AvgLatency,
        target.Latency95thPercentile
) THEN
    UPDATE SET
        target.HasConnected = source.HasConnected,
        target.MadeCompleteInteraction = source.MadeCompleteInteraction,
        target.TimeConnected = source.TimeConnected,
        target.AvgLatency = source.AvgLatency,
        target.Latency95thPercentile = source.Latency95thPercentile,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        UserId,
        [Date],
        HasConnected,
        MadeCompleteInteraction,
        TimeConnected,
        AvgLatency,
        Latency95thPercentile
    )
    VALUES (
        source.UserId,
        source.[Date],
        source.HasConnected,
        source.MadeCompleteInteraction,
        source.TimeConnected,
        source.AvgLatency,
        source.Latency95thPercentile
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_readings_measurement_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factReadingsMeasurement AS target
USING (
    SELECT
        OperationId,
        SensorId,
        SensorTyId,
        DeviceId,
        LocationId,
        DeviceType,
        SensorType,
        TimeSpan,
        LocalTimeSpan,
        MeasurementValue,
        MeasurementStdDev,
        [Min],
        [Max],
        SensorName,
        Anomalies,
        HasAnomalies,
        CorrectReadingsJson,
        AnomalousReadingsJson,
        ReadingsCount
    FROM stg.factReadingsMeasurement
    WHERE RunId = ?
) AS source
    ON target.OperationId = source.OperationId
WHEN MATCHED THEN
    UPDATE SET
        target.SensorId = source.SensorId,
        target.SensorTyId = source.SensorTyId,
        target.DeviceId = source.DeviceId,
        target.LocationId = source.LocationId,
        target.DeviceType = source.DeviceType,
        target.SensorType = source.SensorType,
        target.TimeSpan = source.TimeSpan,
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.MeasurementValue = source.MeasurementValue,
        target.MeasurementStdDev = source.MeasurementStdDev,
        target.[Min] = source.[Min],
        target.[Max] = source.[Max],
        target.SensorName = source.SensorName,
        target.Anomalies = source.Anomalies,
        target.HasAnomalies = source.HasAnomalies,
        target.CorrectReadingsJson = source.CorrectReadingsJson,
        target.AnomalousReadingsJson = source.AnomalousReadingsJson,
        target.ReadingsCount = source.ReadingsCount,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        OperationId,
        SensorId,
        SensorTyId,
        DeviceId,
        LocationId,
        DeviceType,
        SensorType,
        TimeSpan,
        LocalTimeSpan,
        MeasurementValue,
        MeasurementStdDev,
        [Min],
        [Max],
        SensorName,
        Anomalies,
        HasAnomalies,
        CorrectReadingsJson,
        AnomalousReadingsJson,
        ReadingsCount
    )
    VALUES (
        source.OperationId,
        source.SensorId,
        source.SensorTyId,
        source.DeviceId,
        source.LocationId,
        source.DeviceType,
        source.SensorType,
        source.TimeSpan,
        source.LocalTimeSpan,
        source.MeasurementValue,
        source.MeasurementStdDev,
        source.[Min],
        source.[Max],
        source.SensorName,
        source.Anomalies,
        source.HasAnomalies,
        source.CorrectReadingsJson,
        source.AnomalousReadingsJson,
        source.ReadingsCount
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_sensor_events_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @RunId NVARCHAR(100) = ?;
DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factSensorEvents AS target
USING (
    SELECT
        EventId,
        ReadId,
        SensorId,
        DeviceId,
        LocationId,
        LocationName,
        DeviceType,
        SensorType,
        SensorName,
        TimeSpan,
        LocalTimeSpan,
        Value,
        EventMinutes,
        IsEventCompleted
    FROM stg.factSensorEvents
    WHERE RunId = @RunId
) AS source
    ON target.EventId = source.EventId
WHEN MATCHED THEN
    UPDATE SET
        target.ReadId = source.ReadId,
        target.SensorId = source.SensorId,
        target.DeviceId = source.DeviceId,
        target.LocationId = source.LocationId,
        target.LocationName = source.LocationName,
        target.DeviceType = source.DeviceType,
        target.SensorType = source.SensorType,
        target.SensorName = source.SensorName,
        target.TimeSpan = source.TimeSpan,
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.Value = source.Value,
        target.EventMinutes = source.EventMinutes,
        target.IsEventCompleted = source.IsEventCompleted,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        EventId,
        ReadId,
        SensorId,
        DeviceId,
        LocationId,
        LocationName,
        DeviceType,
        SensorType,
        SensorName,
        TimeSpan,
        LocalTimeSpan,
        Value,
        EventMinutes,
        IsEventCompleted
    )
    VALUES (
        source.EventId,
        source.ReadId,
        source.SensorId,
        source.DeviceId,
        source.LocationId,
        source.LocationName,
        source.DeviceType,
        source.SensorType,
        source.SensorName,
        source.TimeSpan,
        source.LocalTimeSpan,
        source.Value,
        source.EventMinutes,
        source.IsEventCompleted
    )
OUTPUT $action INTO @MergeActions;

DELETE target
FROM dwh.factSensorEvents AS target
INNER JOIN stg.factSensorEvents AS source
    ON target.SensorId = source.SensorId
    AND target.LocalTimeSpan = source.LocalTimeSpan
WHERE
    source.RunId = @RunId
    AND target.TimeSpan IS NULL;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


FACT_REDINGS_AUDITS_MERGE_SQL = """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factRedingsAudits AS target
USING (
    SELECT
        SensorId,
        TimeSpan,
        LocalTimeSpan,
        ReadingsCount,
        AverageGapMinutes,
        MaximumGapMinutes,
        MinimumGapMinutes,
        ReadingsCoveragePercentage,
        CoveredReadingBlocks,
        HasLateReadings,
        IsConnectionLost,
        LastReadingAt,
        ConnectionLostAt,
        MinutesWithoutReadings
    FROM stg.factRedingsAudits
    WHERE RunId = ?
) AS source
    ON target.SensorId = source.SensorId
    AND target.TimeSpan = source.TimeSpan
WHEN MATCHED THEN
    UPDATE SET
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.ReadingsCount = source.ReadingsCount,
        target.AverageGapMinutes = source.AverageGapMinutes,
        target.MaximumGapMinutes = source.MaximumGapMinutes,
        target.MinimumGapMinutes = source.MinimumGapMinutes,
        target.ReadingsCoveragePercentage = source.ReadingsCoveragePercentage,
        target.CoveredReadingBlocks = source.CoveredReadingBlocks,
        target.HasLateReadings = CASE
            WHEN target.HasLateReadings = 1 OR source.HasLateReadings = 1 THEN 1
            ELSE 0
        END,
        target.IsConnectionLost = source.IsConnectionLost,
        target.LastReadingAt = source.LastReadingAt,
        target.ConnectionLostAt = source.ConnectionLostAt,
        target.MinutesWithoutReadings = source.MinutesWithoutReadings,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        SensorId,
        TimeSpan,
        LocalTimeSpan,
        ReadingsCount,
        AverageGapMinutes,
        MaximumGapMinutes,
        MinimumGapMinutes,
        ReadingsCoveragePercentage,
        CoveredReadingBlocks,
        HasLateReadings,
        IsConnectionLost,
        LastReadingAt,
        ConnectionLostAt,
        MinutesWithoutReadings
    )
    VALUES (
        source.SensorId,
        source.TimeSpan,
        source.LocalTimeSpan,
        source.ReadingsCount,
        source.AverageGapMinutes,
        source.MaximumGapMinutes,
        source.MinimumGapMinutes,
        source.ReadingsCoveragePercentage,
        source.CoveredReadingBlocks,
        source.HasLateReadings,
        source.IsConnectionLost,
        source.LastReadingAt,
        source.ConnectionLostAt,
        source.MinutesWithoutReadings
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


FACT_CHIRPSTACK_DEVICE_METRICS_MERGE_SQL = """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factChirpStackDeviceMetrics WITH (HOLDLOCK) AS target
USING (
    SELECT
        DevEui,
        TimeSpan,
        TenantId,
        ApplicationId,
        DeviceProfileId,
        UplinkIntervalSeconds,
        ExpectedUplinkCount,
        ReceivedUplinkCount,
        AverageRssi,
        AverageSnr,
        LinkErrorCount,
        LinkErrorCountsJson,
        BatteryLevel,
        ExternalPowerSource,
        BatteryLevelSource,
        BatteryObservedAt,
        MetricsAvailable,
        ExtractedAt
    FROM stg.factChirpStackDeviceMetrics
    WHERE RunId = ?
) AS source
    ON target.DevEui = source.DevEui
    AND target.TimeSpan = source.TimeSpan
WHEN MATCHED AND (
    target.MetricsAvailable = 0
    OR source.MetricsAvailable = 1
) THEN
    UPDATE SET
        target.TenantId = source.TenantId,
        target.ApplicationId = source.ApplicationId,
        target.DeviceProfileId = source.DeviceProfileId,
        target.UplinkIntervalSeconds = source.UplinkIntervalSeconds,
        target.ExpectedUplinkCount = source.ExpectedUplinkCount,
        target.ReceivedUplinkCount = CASE
            WHEN target.ReceivedUplinkCount IS NULL
                OR source.ReceivedUplinkCount > target.ReceivedUplinkCount
                THEN source.ReceivedUplinkCount
            ELSE target.ReceivedUplinkCount
        END,
        target.AverageRssi = CASE
            WHEN target.ReceivedUplinkCount IS NULL
                OR source.ReceivedUplinkCount > target.ReceivedUplinkCount
                THEN source.AverageRssi
            ELSE target.AverageRssi
        END,
        target.AverageSnr = CASE
            WHEN target.ReceivedUplinkCount IS NULL
                OR source.ReceivedUplinkCount > target.ReceivedUplinkCount
                THEN source.AverageSnr
            ELSE target.AverageSnr
        END,
        target.LinkErrorCount = CASE
            WHEN target.LinkErrorCount IS NULL
                OR source.LinkErrorCount > target.LinkErrorCount
                THEN source.LinkErrorCount
            ELSE target.LinkErrorCount
        END,
        target.LinkErrorCountsJson = CASE
            WHEN target.LinkErrorCount IS NULL
                OR source.LinkErrorCount > target.LinkErrorCount
                THEN source.LinkErrorCountsJson
            ELSE target.LinkErrorCountsJson
        END,
        target.BatteryLevel = CASE
            WHEN target.BatteryObservedAt IS NULL
                OR source.BatteryObservedAt > target.BatteryObservedAt
                THEN source.BatteryLevel
            ELSE target.BatteryLevel
        END,
        target.ExternalPowerSource = CASE
            WHEN target.BatteryObservedAt IS NULL
                OR source.BatteryObservedAt > target.BatteryObservedAt
                THEN source.ExternalPowerSource
            ELSE target.ExternalPowerSource
        END,
        target.BatteryLevelSource = CASE
            WHEN target.BatteryObservedAt IS NULL
                OR source.BatteryObservedAt > target.BatteryObservedAt
                THEN source.BatteryLevelSource
            ELSE target.BatteryLevelSource
        END,
        target.BatteryObservedAt = CASE
            WHEN target.BatteryObservedAt IS NULL
                OR source.BatteryObservedAt > target.BatteryObservedAt
                THEN source.BatteryObservedAt
            ELSE target.BatteryObservedAt
        END,
        target.MetricsAvailable = CASE
            WHEN target.MetricsAvailable = 1 OR source.MetricsAvailable = 1 THEN 1
            ELSE 0
        END,
        target.ExtractedAt = source.ExtractedAt,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        DevEui,
        TimeSpan,
        TenantId,
        ApplicationId,
        DeviceProfileId,
        UplinkIntervalSeconds,
        ExpectedUplinkCount,
        ReceivedUplinkCount,
        AverageRssi,
        AverageSnr,
        LinkErrorCount,
        LinkErrorCountsJson,
        BatteryLevel,
        ExternalPowerSource,
        BatteryLevelSource,
        BatteryObservedAt,
        MetricsAvailable,
        ExtractedAt
    )
    VALUES (
        source.DevEui,
        source.TimeSpan,
        source.TenantId,
        source.ApplicationId,
        source.DeviceProfileId,
        source.UplinkIntervalSeconds,
        source.ExpectedUplinkCount,
        source.ReceivedUplinkCount,
        source.AverageRssi,
        source.AverageSnr,
        source.LinkErrorCount,
        source.LinkErrorCountsJson,
        source.BatteryLevel,
        source.ExternalPowerSource,
        source.BatteryLevelSource,
        source.BatteryObservedAt,
        source.MetricsAvailable,
        source.ExtractedAt
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


FACT_CHIRPSTACK_GATEWAY_METRICS_MERGE_SQL = """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factChirpStackGatewayMetrics WITH (HOLDLOCK) AS target
USING (
    SELECT
        GatewayId,
        TimeSpan,
        TenantId,
        GatewayName,
        ReceivedPacketCount,
        TransmittedPacketCount,
        TxOkCount,
        TxErrorCount,
        TxPacketStatusCountsJson,
        GatewayState,
        GatewayLastSeenAt,
        StatsIntervalSeconds,
        MetricsAvailable,
        ExtractedAt
    FROM stg.factChirpStackGatewayMetrics
    WHERE RunId = ?
) AS source
    ON target.GatewayId = source.GatewayId
    AND target.TimeSpan = source.TimeSpan
WHEN MATCHED AND (
    target.MetricsAvailable = 0
    OR source.MetricsAvailable = 1
) THEN
    UPDATE SET
        target.TenantId = source.TenantId,
        target.GatewayName = source.GatewayName,
        target.ReceivedPacketCount = CASE
            WHEN target.ReceivedPacketCount IS NULL
                OR source.ReceivedPacketCount > target.ReceivedPacketCount
                THEN source.ReceivedPacketCount
            ELSE target.ReceivedPacketCount
        END,
        target.TransmittedPacketCount = CASE
            WHEN target.TransmittedPacketCount IS NULL
                OR source.TransmittedPacketCount > target.TransmittedPacketCount
                THEN source.TransmittedPacketCount
            ELSE target.TransmittedPacketCount
        END,
        target.TxOkCount = CASE
            WHEN target.TransmittedPacketCount IS NULL
                OR source.TransmittedPacketCount > target.TransmittedPacketCount
                THEN source.TxOkCount
            ELSE target.TxOkCount
        END,
        target.TxErrorCount = CASE
            WHEN target.TransmittedPacketCount IS NULL
                OR source.TransmittedPacketCount > target.TransmittedPacketCount
                THEN source.TxErrorCount
            ELSE target.TxErrorCount
        END,
        target.TxPacketStatusCountsJson = CASE
            WHEN target.TransmittedPacketCount IS NULL
                OR source.TransmittedPacketCount > target.TransmittedPacketCount
                THEN source.TxPacketStatusCountsJson
            ELSE target.TxPacketStatusCountsJson
        END,
        target.GatewayState = CASE
            WHEN source.GatewayLastSeenAt IS NOT NULL THEN source.GatewayState
            WHEN source.GatewayState = N'NEVER_SEEN' THEN source.GatewayState
            WHEN
                ISNULL(target.ReceivedPacketCount, 0) > 0
                OR ISNULL(source.ReceivedPacketCount, 0) > 0
                OR ISNULL(target.TransmittedPacketCount, 0) > 0
                OR ISNULL(source.TransmittedPacketCount, 0) > 0
                OR ISNULL(target.TxOkCount, 0) > 0
                OR ISNULL(source.TxOkCount, 0) > 0
                OR ISNULL(target.TxErrorCount, 0) > 0
                OR ISNULL(source.TxErrorCount, 0) > 0
                THEN N'ONLINE'
            ELSE N'UNKNOWN'
        END,
        target.GatewayLastSeenAt = source.GatewayLastSeenAt,
        target.StatsIntervalSeconds = source.StatsIntervalSeconds,
        target.MetricsAvailable = CASE
            WHEN target.MetricsAvailable = 1 OR source.MetricsAvailable = 1 THEN 1
            ELSE 0
        END,
        target.ExtractedAt = source.ExtractedAt,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        GatewayId,
        TimeSpan,
        TenantId,
        GatewayName,
        ReceivedPacketCount,
        TransmittedPacketCount,
        TxOkCount,
        TxErrorCount,
        TxPacketStatusCountsJson,
        GatewayState,
        GatewayLastSeenAt,
        StatsIntervalSeconds,
        MetricsAvailable,
        ExtractedAt
    )
    VALUES (
        source.GatewayId,
        source.TimeSpan,
        source.TenantId,
        source.GatewayName,
        source.ReceivedPacketCount,
        source.TransmittedPacketCount,
        source.TxOkCount,
        source.TxErrorCount,
        source.TxPacketStatusCountsJson,
        source.GatewayState,
        source.GatewayLastSeenAt,
        source.StatsIntervalSeconds,
        source.MetricsAvailable,
        source.ExtractedAt
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


FACT_CHIRPSTACK_DEVICE_EVENT_METRICS_MERGE_SQL = """
SET NOCOUNT ON;

INSERT INTO #ChirpStackMqttAccepted
SELECT source.*
FROM stg.factChirpStackDeviceEventMetrics AS source
LEFT JOIN observability.factChirpStackDeviceEventMetrics AS target WITH (UPDLOCK, HOLDLOCK)
    ON target.DevEui = source.DevEui
    AND target.TimeSpan = source.TimeSpan
WHERE source.RunId = ?
    AND (
        target.DevEui IS NULL
        OR (
            source.CapturedUplinkCount >= target.CapturedUplinkCount
            AND source.ObservedGatewayCount >= target.ObservedGatewayCount
            AND source.MultiGatewayUplinkCount >= target.MultiGatewayUplinkCount
            AND NOT EXISTS (
                SELECT 1
                FROM OPENJSON(target.RadioFieldQualityJson) AS previousQuality
                LEFT JOIN OPENJSON(source.RadioFieldQualityJson) AS incomingQuality
                    ON incomingQuality.[key] = previousQuality.[key]
                WHERE
                    incomingQuality.[key] IS NULL
                    OR CONVERT(BIGINT, JSON_VALUE(incomingQuality.[value], '$.valid'))
                        < CONVERT(BIGINT, JSON_VALUE(previousQuality.[value], '$.valid'))
            )
            AND NOT EXISTS (
                SELECT 1
                FROM OPENJSON(target.EventLogCountsJson) AS targetLog
                LEFT JOIN OPENJSON(source.EventLogCountsJson) AS sourceLog
                    ON sourceLog.[key] = targetLog.[key]
                WHERE
                    sourceLog.[key] IS NULL
                    OR CONVERT(BIGINT, sourceLog.[value]) < CONVERT(BIGINT, targetLog.[value])
            )
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM observability.factChirpStackDeviceGatewayMetrics AS existing WITH (UPDLOCK, HOLDLOCK)
        LEFT JOIN stg.factChirpStackDeviceGatewayMetrics AS candidate
            ON candidate.RunId = source.RunId
            AND candidate.DevEui = existing.DevEui
            AND candidate.GatewayId = existing.GatewayId
            AND candidate.TimeSpan = existing.TimeSpan
        WHERE
            existing.DevEui = source.DevEui
            AND existing.TimeSpan = source.TimeSpan
            AND (
                candidate.GatewayId IS NULL
                OR candidate.GatewayReceptionCount < existing.GatewayReceptionCount
                OR EXISTS (
                    SELECT 1
                    FROM OPENJSON(existing.ReceptionFieldQualityJson) AS previousQuality
                    LEFT JOIN OPENJSON(candidate.ReceptionFieldQualityJson) AS incomingQuality
                        ON incomingQuality.[key] = previousQuality.[key]
                    WHERE
                        incomingQuality.[key] IS NULL
                        OR CONVERT(BIGINT, JSON_VALUE(incomingQuality.[value], '$.valid'))
                            < CONVERT(BIGINT, JSON_VALUE(previousQuality.[value], '$.valid'))
                )
                OR (
                    candidate.ReceptionFieldQualityJson IS NOT NULL
                    AND (
                        (existing.AverageRssi IS NOT NULL AND candidate.AverageRssi IS NULL)
                        OR (existing.AverageSnr IS NOT NULL AND candidate.AverageSnr IS NULL)
                    )
                )
            )
    );

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factChirpStackDeviceEventMetrics WITH (HOLDLOCK) AS target
USING (
    SELECT
        DevEui,
        TimeSpan,
        TenantId,
        CapturedUplinkCount,
        ObservedGatewayCount,
        MultiGatewayUplinkCount,
        EventLogCountsJson,
        RadioProfileCountsJson,
        UplinkContextCountsJson,
        RadioFieldQualityJson,
        FrameCounterSummaryJson,
        ExtractedAt
    FROM #ChirpStackMqttAccepted
) AS source
    ON target.DevEui = source.DevEui
    AND target.TimeSpan = source.TimeSpan
WHEN MATCHED AND
    source.CapturedUplinkCount >= target.CapturedUplinkCount
    AND source.ObservedGatewayCount >= target.ObservedGatewayCount
    AND source.MultiGatewayUplinkCount >= target.MultiGatewayUplinkCount
    AND NOT EXISTS (
        SELECT 1
        FROM OPENJSON(target.EventLogCountsJson) AS targetLog
        LEFT JOIN OPENJSON(source.EventLogCountsJson) AS sourceLog
            ON sourceLog.[key] = targetLog.[key]
        WHERE
            sourceLog.[key] IS NULL
            OR CONVERT(BIGINT, sourceLog.[value]) < CONVERT(BIGINT, targetLog.[value])
    )
    AND (
        source.CapturedUplinkCount > target.CapturedUplinkCount
        OR source.ObservedGatewayCount > target.ObservedGatewayCount
        OR source.MultiGatewayUplinkCount > target.MultiGatewayUplinkCount
        OR ISNULL(source.RadioProfileCountsJson, N'') <> ISNULL(target.RadioProfileCountsJson, N'')
        OR ISNULL(source.UplinkContextCountsJson, N'') <> ISNULL(target.UplinkContextCountsJson, N'')
        OR ISNULL(source.RadioFieldQualityJson, N'') <> ISNULL(target.RadioFieldQualityJson, N'')
        OR ISNULL(source.FrameCounterSummaryJson, N'') <> ISNULL(target.FrameCounterSummaryJson, N'')
        OR EXISTS (
            SELECT 1
            FROM OPENJSON(source.EventLogCountsJson) AS sourceLog
            LEFT JOIN OPENJSON(target.EventLogCountsJson) AS targetLog
                ON targetLog.[key] = sourceLog.[key]
            WHERE
                targetLog.[key] IS NULL
                OR CONVERT(BIGINT, sourceLog.[value]) > CONVERT(BIGINT, targetLog.[value])
        )
    ) THEN
    UPDATE SET
        target.TenantId = source.TenantId,
        target.CapturedUplinkCount = source.CapturedUplinkCount,
        target.ObservedGatewayCount = source.ObservedGatewayCount,
        target.MultiGatewayUplinkCount = source.MultiGatewayUplinkCount,
        target.EventLogCountsJson = source.EventLogCountsJson,
        target.RadioProfileCountsJson = source.RadioProfileCountsJson,
        target.UplinkContextCountsJson = source.UplinkContextCountsJson,
        target.RadioFieldQualityJson = source.RadioFieldQualityJson,
        target.FrameCounterSummaryJson = source.FrameCounterSummaryJson,
        target.ExtractedAt = source.ExtractedAt,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        DevEui,
        TimeSpan,
        TenantId,
        CapturedUplinkCount,
        ObservedGatewayCount,
        MultiGatewayUplinkCount,
        EventLogCountsJson,
        RadioProfileCountsJson,
        UplinkContextCountsJson,
        RadioFieldQualityJson,
        FrameCounterSummaryJson,
        ExtractedAt
    )
    VALUES (
        source.DevEui,
        source.TimeSpan,
        source.TenantId,
        source.CapturedUplinkCount,
        source.ObservedGatewayCount,
        source.MultiGatewayUplinkCount,
        source.EventLogCountsJson,
        source.RadioProfileCountsJson,
        source.UplinkContextCountsJson,
        source.RadioFieldQualityJson,
        source.FrameCounterSummaryJson,
        source.ExtractedAt
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


FACT_CHIRPSTACK_DEVICE_GATEWAY_METRICS_MERGE_SQL = """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factChirpStackDeviceGatewayMetrics WITH (HOLDLOCK) AS target
USING (
    SELECT
        DevEui,
        GatewayId,
        TimeSpan,
        TenantId,
        GatewayReceptionCount,
        ReceptionShare,
        AverageRssi,
        MinRssi,
        MaxRssi,
        AverageSnr,
        MinSnr,
        MaxSnr,
        RadioReceptionStatsJson,
        ReceptionFieldQualityJson,
        FirstReceivedAt,
        LastReceivedAt,
        ExtractedAt,
        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM observability.factChirpStackDeviceGatewayMetrics AS existing
                LEFT JOIN stg.factChirpStackDeviceGatewayMetrics AS candidate
                    ON candidate.RunId = stage.RunId
                    AND candidate.DevEui = existing.DevEui
                    AND candidate.GatewayId = existing.GatewayId
                    AND candidate.TimeSpan = existing.TimeSpan
                WHERE
                    existing.DevEui = stage.DevEui
                    AND existing.TimeSpan = stage.TimeSpan
                    AND (
                        candidate.GatewayId IS NULL
                        OR candidate.GatewayReceptionCount < existing.GatewayReceptionCount
                    )
            ) THEN 1
            ELSE 0
        END AS IsNonDecreasing
    FROM stg.factChirpStackDeviceGatewayMetrics AS stage
    WHERE RunId = ?
        AND EXISTS (
            SELECT 1
            FROM #ChirpStackMqttAccepted AS accepted
            WHERE accepted.RunId = stage.RunId
                AND accepted.DevEui = stage.DevEui
                AND accepted.TimeSpan = stage.TimeSpan
        )
) AS source
    ON target.DevEui = source.DevEui
    AND target.GatewayId = source.GatewayId
    AND target.TimeSpan = source.TimeSpan
WHEN MATCHED AND source.IsNonDecreasing = 1 THEN
    UPDATE SET
        target.TenantId = source.TenantId,
        target.GatewayReceptionCount = source.GatewayReceptionCount,
        target.ReceptionShare = source.ReceptionShare,
        target.AverageRssi = CASE
            WHEN source.ReceptionFieldQualityJson IS NOT NULL THEN source.AverageRssi
            WHEN target.AverageRssi IS NOT NULL AND target.MinRssi IS NOT NULL AND target.MaxRssi IS NOT NULL
                AND (
                    source.AverageRssi IS NULL OR source.MinRssi IS NULL OR source.MaxRssi IS NULL
                    OR (source.AverageRssi = 0 AND source.MinRssi = 0 AND source.MaxRssi = 0)
                ) THEN target.AverageRssi
            ELSE source.AverageRssi
        END,
        target.MinRssi = CASE
            WHEN source.ReceptionFieldQualityJson IS NOT NULL THEN source.MinRssi
            WHEN target.AverageRssi IS NOT NULL AND target.MinRssi IS NOT NULL AND target.MaxRssi IS NOT NULL
                AND (
                    source.AverageRssi IS NULL OR source.MinRssi IS NULL OR source.MaxRssi IS NULL
                    OR (source.AverageRssi = 0 AND source.MinRssi = 0 AND source.MaxRssi = 0)
                ) THEN target.MinRssi
            ELSE source.MinRssi
        END,
        target.MaxRssi = CASE
            WHEN source.ReceptionFieldQualityJson IS NOT NULL THEN source.MaxRssi
            WHEN target.AverageRssi IS NOT NULL AND target.MinRssi IS NOT NULL AND target.MaxRssi IS NOT NULL
                AND (
                    source.AverageRssi IS NULL OR source.MinRssi IS NULL OR source.MaxRssi IS NULL
                    OR (source.AverageRssi = 0 AND source.MinRssi = 0 AND source.MaxRssi = 0)
                ) THEN target.MaxRssi
            ELSE source.MaxRssi
        END,
        target.AverageSnr = CASE
            WHEN source.ReceptionFieldQualityJson IS NOT NULL THEN source.AverageSnr
            WHEN target.AverageSnr IS NOT NULL AND target.MinSnr IS NOT NULL AND target.MaxSnr IS NOT NULL
                AND (
                    source.AverageSnr IS NULL OR source.MinSnr IS NULL OR source.MaxSnr IS NULL
                    OR (source.AverageSnr = 0 AND source.MinSnr = 0 AND source.MaxSnr = 0)
                ) THEN target.AverageSnr
            ELSE source.AverageSnr
        END,
        target.MinSnr = CASE
            WHEN source.ReceptionFieldQualityJson IS NOT NULL THEN source.MinSnr
            WHEN target.AverageSnr IS NOT NULL AND target.MinSnr IS NOT NULL AND target.MaxSnr IS NOT NULL
                AND (
                    source.AverageSnr IS NULL OR source.MinSnr IS NULL OR source.MaxSnr IS NULL
                    OR (source.AverageSnr = 0 AND source.MinSnr = 0 AND source.MaxSnr = 0)
                ) THEN target.MinSnr
            ELSE source.MinSnr
        END,
        target.MaxSnr = CASE
            WHEN source.ReceptionFieldQualityJson IS NOT NULL THEN source.MaxSnr
            WHEN target.AverageSnr IS NOT NULL AND target.MinSnr IS NOT NULL AND target.MaxSnr IS NOT NULL
                AND (
                    source.AverageSnr IS NULL OR source.MinSnr IS NULL OR source.MaxSnr IS NULL
                    OR (source.AverageSnr = 0 AND source.MinSnr = 0 AND source.MaxSnr = 0)
                ) THEN target.MaxSnr
            ELSE source.MaxSnr
        END,
        target.RadioReceptionStatsJson = source.RadioReceptionStatsJson,
        target.ReceptionFieldQualityJson = source.ReceptionFieldQualityJson,
        target.FirstReceivedAt = source.FirstReceivedAt,
        target.LastReceivedAt = source.LastReceivedAt,
        target.ExtractedAt = source.ExtractedAt,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET AND source.IsNonDecreasing = 1 THEN
    INSERT (
        DevEui,
        GatewayId,
        TimeSpan,
        TenantId,
        GatewayReceptionCount,
        ReceptionShare,
        AverageRssi,
        MinRssi,
        MaxRssi,
        AverageSnr,
        MinSnr,
        MaxSnr,
        RadioReceptionStatsJson,
        ReceptionFieldQualityJson,
        FirstReceivedAt,
        LastReceivedAt,
        ExtractedAt
    )
    VALUES (
        source.DevEui,
        source.GatewayId,
        source.TimeSpan,
        source.TenantId,
        source.GatewayReceptionCount,
        source.ReceptionShare,
        source.AverageRssi,
        source.MinRssi,
        source.MaxRssi,
        source.AverageSnr,
        source.MinSnr,
        source.MaxSnr,
        source.RadioReceptionStatsJson,
        source.ReceptionFieldQualityJson,
        source.FirstReceivedAt,
        source.LastReceivedAt,
        source.ExtractedAt
    )
OUTPUT $action INTO @MergeActions;

DROP TABLE #ChirpStackMqttAccepted;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""
















class SqlServerResource:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        connect_timeout: int,
        command_timeout: int,
        driver: str,
        retries: int,
        retry_interval: int,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self.driver = driver
        self.retries = retries
        self.retry_interval = retry_interval

    def _get_connection(self) -> pyodbc.Connection:
        conn_str = (
            "DRIVER={"
            + self.driver
            + "};SERVER="
            + self.host
            + ","
            + str(self.port)
            + ";DATABASE="
            + self.database
            + ";UID="
            + self.user
            + ";PWD="
            + self.password
            + ";TrustServerCertificate=yes"
            + ";Connection Timeout="
            + str(self.connect_timeout)
            + ";ConnectRetryCount="
            + str(self.retries)
            + ";ConnectRetryInterval="
            + str(self.retry_interval)
        )
        return pyodbc.connect(conn_str, timeout=self.connect_timeout)

    def get_watermark(self, ingestion_name: str) -> Optional[datetime]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        "EXEC ctl.usp_IngestionControl_GetWatermark @IngestionName = ?",
                        ingestion_name,
                    )
                    row = cursor.fetchone()
                except pyodbc.Error:
                    return None
                if row is None:
                    return None
                return row.LastLoadedAt

    def get_min_open_sensor_event_time(
        self,
        sensor_type: Optional[str] = None,
    ) -> Optional[datetime]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    if sensor_type is None:
                        cursor.execute(
                            "SELECT MIN(COALESCE(TimeSpan, LocalTimeSpan)) AS OpenEventFrom "
                            "FROM dwh.factSensorEvents "
                            "WHERE IsEventCompleted = 0"
                        )
                    else:
                        cursor.execute(
                            "SELECT MIN(COALESCE(TimeSpan, LocalTimeSpan)) AS OpenEventFrom "
                            "FROM dwh.factSensorEvents "
                            "WHERE IsEventCompleted = 0 "
                            "AND LOWER(SensorType) LIKE LOWER(?)",
                            sensor_type,
                        )
                    row = cursor.fetchone()
                except pyodbc.Error:
                    return None
                if row is None:
                    return None
                return row.OpenEventFrom

    def delete_sensor_events_range(
        self,
        sensor_type: str,
        from_utc: datetime,
        to_utc: datetime,
    ) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM dwh.factSensorEvents "
                    "WHERE SensorType = ? "
                    "AND LocalTimeSpan >= ? "
                    "AND LocalTimeSpan <= ?",
                    sensor_type,
                    from_utc,
                    to_utc,
                )
                deleted = cursor.rowcount
                conn.commit()
                if deleted is None or deleted < 0:
                    return 0
                return int(deleted)

    def mark_run_started(self, ingestion_name: str) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_MarkRunStarted @IngestionName = ?",
                    ingestion_name,
                )
                conn.commit()

    def execute_query(self, query: str, *args) -> Optional[int]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query, *args)
                    conn.commit()
                    return cursor.rowcount
                except pyodbc.Error as e:
                    return e

    def fetch_scalar(self, query: str, *args):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, *args)
                row = cursor.fetchone()
                return row[0] if row is not None else None

    def delete_range(self,
                     schema_name: str,
                     table_name: str,
                     time_column: str,
                     from_utc: datetime,
                     to_utc: Optional[datetime] = None) -> int:
        if to_utc is None:
            to_utc = datetime.now(timezone.utc)
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_DeleteRange "
                    "@SchemaName = ?, "
                    "@TableName = ?, "
                    "@TimeColumn = ?, "
                    "@FromUtc = ?, "
                    "@ToUtc = ?",
                    schema_name,
                    table_name,
                    time_column,
                    from_utc,
                    to_utc,
                )
                rows = cursor.fetchone()
                conn.commit()
                if rows is None:
                    return 0
                deleted = rows.DeletedRowCount
                if deleted is None:
                    return 0
                return int(deleted)

    def mark_run_completed(
        self,
        ingestion_name: str,
        new_last_loaded_at: Optional[datetime],
        succeeded: bool,
        error_message: Optional[str],
    ) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_MarkRunCompleted "
                    "@IngestionName = ?, @NewLastLoadedAt = ?, @Succeeded = ?, @ErrorMessage = ?",
                    ingestion_name,
                    new_last_loaded_at,
                    1 if succeeded else 0,
                    error_message,
                )
                conn.commit()

    def clear_dim_telemetry_prod_dimensions_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.dimTelemetryProdDimensions WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_dim_telemetry_prod_dimensions(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_dim_telemetry_prod_dimensions_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_dim_aspnet_users_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.dimAspNetUsers WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_dim_aspnet_users(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_dim_aspnet_users_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_telemetry_user_usage_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factTelemetryUserUsage WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_telemetry_user_usage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_telemetry_user_usage_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_sensor_events_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factSensorEvents WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_sensor_events(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_sensor_events_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_redings_audits_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factRedingsAudits WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_redings_audits(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(FACT_REDINGS_AUDITS_MERGE_SQL, run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_chirpstack_metrics_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factChirpStackDeviceMetrics WHERE RunId = ?",
                    run_id,
                )
                deleted_devices = int(cursor.rowcount or 0)
                cursor.execute(
                    "DELETE FROM stg.factChirpStackGatewayMetrics WHERE RunId = ?",
                    run_id,
                )
                deleted_gateways = int(cursor.rowcount or 0)
                conn.commit()
                return deleted_devices + deleted_gateways

    def clear_chirpstack_mqtt_metrics_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factChirpStackDeviceEventMetrics WHERE RunId = ?",
                    run_id,
                )
                deleted_device_events = int(cursor.rowcount or 0)
                cursor.execute(
                    "DELETE FROM stg.factChirpStackDeviceGatewayMetrics WHERE RunId = ?",
                    run_id,
                )
                deleted_device_gateways = int(cursor.rowcount or 0)
                conn.commit()
                return deleted_device_events + deleted_device_gateways

    def write_mqtt_event_history_rejected_rows(
        self,
        rows: list[dict],
        run_id: str,
        retention_days: int,
    ) -> tuple[int, int]:
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
DELETE FROM raw.MqttEventHistoryRejectedRow
WHERE CreatedAt < DATEADD(DAY, -?, SYSUTCDATETIME());
""",
                        retention_days,
                    )
                    deleted_rows = max(int(cursor.rowcount or 0), 0)
                    if rows:
                        cursor.executemany(
                            """
INSERT INTO raw.MqttEventHistoryRejectedRow (
    RunId,
    Tenant,
    FechaInicio,
    FechaFin,
    RequestedLimit,
    FailureReason,
    RowJson,
    FetchedAt
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
""",
                            [
                                (
                                    run_id,
                                    row["Tenant"],
                                    row["FechaInicio"],
                                    row["FechaFin"],
                                    row["RequestedLimit"],
                                    row["FailureReason"],
                                    row["RowJson"],
                                    row["FetchedAt"],
                                )
                                for row in rows
                            ],
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return len(rows), deleted_rows

    def merge_chirpstack_metrics(
        self,
        run_id: str,
    ) -> tuple[int, int]:
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        FACT_CHIRPSTACK_DEVICE_METRICS_MERGE_SQL,
                        run_id,
                    )
                    device_row = cursor.fetchone()
                    cursor.execute(
                        FACT_CHIRPSTACK_GATEWAY_METRICS_MERGE_SQL,
                        run_id,
                    )
                    gateway_row = cursor.fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        device_rows = (
            0
            if device_row is None or device_row.AffectedRows is None
            else int(device_row.AffectedRows)
        )
        gateway_rows = (
            0
            if gateway_row is None or gateway_row.AffectedRows is None
            else int(gateway_row.AffectedRows)
        )
        return device_rows, gateway_rows

    def merge_chirpstack_mqtt_metrics(
        self,
        run_id: str,
    ) -> tuple[int, int]:
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT TOP (0) * INTO #ChirpStackMqttAccepted "
                        "FROM stg.factChirpStackDeviceEventMetrics"
                    )
                    cursor.execute(
                        FACT_CHIRPSTACK_DEVICE_EVENT_METRICS_MERGE_SQL,
                        run_id,
                    )
                    device_event_row = cursor.fetchone()
                    cursor.execute(
                        FACT_CHIRPSTACK_DEVICE_GATEWAY_METRICS_MERGE_SQL,
                        run_id,
                    )
                    device_gateway_row = cursor.fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        device_event_rows = (
            0
            if device_event_row is None or device_event_row.AffectedRows is None
            else int(device_event_row.AffectedRows)
        )
        device_gateway_rows = (
            0
            if device_gateway_row is None or device_gateway_row.AffectedRows is None
            else int(device_gateway_row.AffectedRows)
        )
        return device_event_rows, device_gateway_rows

    def clear_fact_readings_measurement_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factReadingsMeasurement WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_readings_measurement(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_readings_measurement_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)


















    def get_basic_options(self) -> Dict[str, Any]:
        url = (
                "jdbc:sqlserver://"
                + self.host
                + ":"
                + str(self.port)
                + ";databaseName="
                + self.database
                + ";encrypt=true;trustServerCertificate=true"
                + ";Connection Timeout="
                + str(self.connect_timeout)
        )
        return {
            "url": url,
            "user": self.user,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

@resource
def sqlserver_resource(_):
    host = os.getenv("MSSQL_HOST", "localhost")
    port = int(os.getenv("MSSQL_PORT", "1433"))
    database = os.getenv("APP_DB", "YourDatabaseName")
    user = os.getenv("MSSQL_USER", "app_ingestion_user")
    password = os.getenv("MSSQL_PASSWORD", "")
    connect_timeout = int(os.getenv("MSSQL_CONNECT_TIMEOUT", "300"))
    command_timeout = int(os.getenv("MSSQL_COMMAND_TIMEOUT", "4500"))
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    connect_retry_count = int(os.getenv("MSSQL_CONNECT_RETRY_COUNT", "3"))
    connect_retry_interval = int(os.getenv("MSSQL_CONNECT_RETRY_INTERVAL", "60"))
    return SqlServerResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
        driver=driver,
        retries=connect_retry_count,
        retry_interval=connect_retry_interval,
    )
