CREATE OR ALTER VIEW serving.vwSensorSensorCommunicationHealthHourly
AS
SELECT
    audit.SensorId,
    device.DevEui,
    audit.TimeSpan,
    audit.LocalTimeSpan,
    dimensions.sensor_name AS SensorName,
    dimensions.PerEvent,
    audit.ReadingsCount,
    audit.ReadingsCoveragePercentage AS DataCoveragePercentage,
    audit.HasLateReadings,
    audit.IsConnectionLost,
    audit.LastReadingAt,
    device.UplinkIntervalSeconds,
    device.ExpectedUplinkCount,
    device.ReceivedUplinkCount,
    device.AverageRssi,
    device.AverageSnr,
    device.LinkErrorCount,
    device.BatteryLevel,
    device.ExternalPowerSource,
    device.BatteryLevelSource,
    device.BatteryObservedAt,
    gateway.GatewayCount,
    gateway.OnlineGatewayCount,
    gateway.OfflineGatewayCount,
    gateway.NeverSeenGatewayCount,
    gateway.GatewaysWithMetricsCount,
    gateway.ReceivedPacketCount AS GatewayReceivedPacketCount,
    gateway.TransmittedPacketCount AS GatewayTransmittedPacketCount,
    gateway.TxOkCount AS GatewayTxOkCount,
    gateway.TxErrorCount AS GatewayTxErrorCount,
    gateway.LatestGatewaySeenAt,
    CASE
        WHEN device.ExpectedUplinkCount > 0 AND device.ReceivedUplinkCount IS NOT NULL
            THEN CONVERT(
                DECIMAL(18, 6),
                device.ReceivedUplinkCount * CONVERT(DECIMAL(18, 6), 100)
                / NULLIF(device.ExpectedUplinkCount, 0)
            )
    END AS ChirpStackReceiptCoveragePercentage,
    CASE
        WHEN device.ReceivedUplinkCount > 0
            THEN CONVERT(
                DECIMAL(18, 6),
                audit.ReadingsCount * CONVERT(DECIMAL(18, 6), 1)
                / NULLIF(device.ReceivedUplinkCount, 0)
            )
    END AS DataReadingsPerReceivedUplink,
    ISNULL(device.MetricsAvailable, 0) AS ChirpStackMetricsAvailable,
    CASE
        WHEN audit.ReadingsCount > 0 THEN N'OK'
        WHEN dimensions.PerEvent IS NULL OR dimensions.PerEvent = 1 THEN N'UNKNOWN'
        WHEN device.MetricsAvailable IS NULL OR device.MetricsAvailable = 0 THEN N'UNKNOWN'
        WHEN device.ReceivedUplinkCount > 0 THEN N'DATA_INGESTION_PROBLEM'
        WHEN device.ExpectedUplinkCount IS NULL OR device.ExpectedUplinkCount <= 0 THEN N'UNKNOWN'
        WHEN device.ReceivedUplinkCount = 0 AND gateway.OnlineGatewayCount > 0
            THEN N'DEVICE_NOT_COMMUNICATING'
        WHEN
            device.ReceivedUplinkCount = 0
            AND gateway.GatewayCount > 0
            AND gateway.OnlineGatewayCount = 0
            AND gateway.OfflineGatewayCount > 0
            THEN N'GATEWAY_PROBLEM'
        ELSE N'UNKNOWN'
    END AS CommunicationStatus
FROM observability.factRedingsAudits AS audit
INNER JOIN dwh.dimTelemetryProdDimensions AS dimensions
    ON audit.SensorId = dimensions.SensorId
LEFT JOIN observability.factChirpStackDeviceMetrics AS device
    ON
        LOWER(LTRIM(RTRIM(dimensions.PhysicalIdentifier))) = device.DevEui
        AND audit.TimeSpan = device.TimeSpan
LEFT JOIN observability.vwChirpStackTenantGatewayHealthHourly AS gateway
    ON
        device.TenantId = gateway.TenantId
        AND audit.TimeSpan = gateway.TimeSpan;
GO

CREATE OR ALTER VIEW serving.vwSensorSensorCommunicationHealthDaily
AS
WITH Ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY SensorId, DevEui, CONVERT(DATE, LocalTimeSpan)
            ORDER BY
                CASE WHEN BatteryLevel <> 0 THEN 0 WHEN BatteryLevel = 0 THEN 1 ELSE 2 END,
                BatteryObservedAt DESC,
                TimeSpan DESC
        ) AS BatteryLevelOrder
    FROM serving.vwSensorSensorCommunicationHealthHourly
)

SELECT
    SensorId,
    DevEui,
    CONVERT(DATE, LocalTimeSpan) AS LocalDate,
    SUM(CONVERT(BIGINT, ReadingsCount)) AS ReadingsCount,
    SUM(ReceivedUplinkCount) AS ReceivedUplinkCount,
    SUM(ExpectedUplinkCount) AS ExpectedUplinkCount,
    SUM(DataCoveragePercentage) / NULLIF(COUNT_BIG(*), 0) AS DataCoveragePercentage,
    CASE
        WHEN SUM(ExpectedUplinkCount) > 0
            THEN CONVERT(
                DECIMAL(18, 6),
                SUM(ReceivedUplinkCount) * CONVERT(DECIMAL(18, 6), 100)
                / NULLIF(SUM(ExpectedUplinkCount), 0)
            )
    END AS ChirpStackReceiptCoveragePercentage,
    CASE
        WHEN SUM(ReceivedUplinkCount) > 0
            THEN CONVERT(
                DECIMAL(18, 6),
                SUM(CONVERT(BIGINT, ReadingsCount)) * CONVERT(DECIMAL(18, 6), 1)
                / NULLIF(SUM(ReceivedUplinkCount), 0)
            )
    END AS DataReadingsPerReceivedUplink,
    SUM(CONVERT(INT, CASE WHEN ReadingsCount > 0 THEN 1 ELSE 0 END)) AS HoursWithData,
    SUM(CONVERT(INT, CASE WHEN ReceivedUplinkCount > 0 THEN 1 ELSE 0 END)) AS HoursWithChirpStackUplinks,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'DATA_INGESTION_PROBLEM' THEN 1 ELSE 0 END)) AS DataIngestionProblemHours,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'DEVICE_NOT_COMMUNICATING' THEN 1 ELSE 0 END)) AS DeviceNotCommunicatingHours,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'GATEWAY_PROBLEM' THEN 1 ELSE 0 END)) AS GatewayProblemHours,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'UNKNOWN' THEN 1 ELSE 0 END)) AS UnknownHours,
    MAX(CASE WHEN BatteryLevelOrder = 1 THEN BatteryLevel END) AS BatteryLevel
FROM Ranked
GROUP BY
    SensorId,
    DevEui,
    CONVERT(DATE, LocalTimeSpan);
GO

CREATE OR ALTER VIEW serving.vwSensorSensorCommunicationHealthWeekly
AS
WITH Ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
            SensorId,
            DevEui,
            DATEADD(
                DAY,
                -(DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), LocalTimeSpan) % 7),
                CONVERT(DATE, LocalTimeSpan)
            )
            ORDER BY
                CASE WHEN BatteryLevel <> 0 THEN 0 WHEN BatteryLevel = 0 THEN 1 ELSE 2 END,
                BatteryObservedAt DESC,
                TimeSpan DESC
        ) AS BatteryLevelOrder
    FROM serving.vwSensorSensorCommunicationHealthHourly
)

SELECT
    SensorId,
    DevEui,
    DATEADD(
        DAY,
        -(DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), LocalTimeSpan) % 7),
        CONVERT(DATE, LocalTimeSpan)
    ) AS WeekStartDate,
    DATEADD(
        DAY,
        6 - (DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), LocalTimeSpan) % 7),
        CONVERT(DATE, LocalTimeSpan)
    ) AS WeekEndDate,
    SUM(CONVERT(BIGINT, ReadingsCount)) AS ReadingsCount,
    SUM(ReceivedUplinkCount) AS ReceivedUplinkCount,
    SUM(ExpectedUplinkCount) AS ExpectedUplinkCount,
    SUM(DataCoveragePercentage) / NULLIF(COUNT_BIG(*), 0) AS DataCoveragePercentage,
    CASE
        WHEN SUM(ExpectedUplinkCount) > 0
            THEN CONVERT(
                DECIMAL(18, 6),
                SUM(ReceivedUplinkCount) * CONVERT(DECIMAL(18, 6), 100)
                / NULLIF(SUM(ExpectedUplinkCount), 0)
            )
    END AS ChirpStackReceiptCoveragePercentage,
    CASE
        WHEN SUM(ReceivedUplinkCount) > 0
            THEN CONVERT(
                DECIMAL(18, 6),
                SUM(CONVERT(BIGINT, ReadingsCount)) * CONVERT(DECIMAL(18, 6), 1)
                / NULLIF(SUM(ReceivedUplinkCount), 0)
            )
    END AS DataReadingsPerReceivedUplink,
    SUM(CONVERT(INT, CASE WHEN ReadingsCount > 0 THEN 1 ELSE 0 END)) AS HoursWithData,
    SUM(CONVERT(INT, CASE WHEN ReceivedUplinkCount > 0 THEN 1 ELSE 0 END)) AS HoursWithChirpStackUplinks,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'DATA_INGESTION_PROBLEM' THEN 1 ELSE 0 END)) AS DataIngestionProblemHours,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'DEVICE_NOT_COMMUNICATING' THEN 1 ELSE 0 END)) AS DeviceNotCommunicatingHours,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'GATEWAY_PROBLEM' THEN 1 ELSE 0 END)) AS GatewayProblemHours,
    SUM(CONVERT(INT, CASE WHEN CommunicationStatus = N'UNKNOWN' THEN 1 ELSE 0 END)) AS UnknownHours,
    MAX(CASE WHEN BatteryLevelOrder = 1 THEN BatteryLevel END) AS BatteryLevel
FROM Ranked
GROUP BY
    SensorId,
    DevEui,
    DATEADD(
        DAY,
        -(DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), LocalTimeSpan) % 7),
        CONVERT(DATE, LocalTimeSpan)
    ),
    DATEADD(
        DAY,
        6 - (DATEDIFF(DAY, CONVERT(DATE, '19000101', 112), LocalTimeSpan) % 7),
        CONVERT(DATE, LocalTimeSpan)
    );
GO
