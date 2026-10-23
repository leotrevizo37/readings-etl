CREATE OR ALTER VIEW observability.vwChirpStackTenantGatewayHealthHourly
AS
SELECT
    TenantId,
    TimeSpan,
    COUNT_BIG(*) AS GatewayCount,
    SUM(CONVERT(BIGINT, CASE WHEN GatewayState = N'ONLINE' THEN 1 ELSE 0 END)) AS OnlineGatewayCount,
    SUM(CONVERT(BIGINT, CASE WHEN GatewayState = N'OFFLINE' THEN 1 ELSE 0 END)) AS OfflineGatewayCount,
    SUM(CONVERT(BIGINT, CASE WHEN GatewayState = N'NEVER_SEEN' THEN 1 ELSE 0 END)) AS NeverSeenGatewayCount,
    SUM(CONVERT(BIGINT, CASE WHEN GatewayState = N'UNKNOWN' THEN 1 ELSE 0 END)) AS UnknownGatewayCount,
    SUM(CONVERT(BIGINT, CASE WHEN MetricsAvailable = 1 THEN 1 ELSE 0 END)) AS GatewaysWithMetricsCount,
    SUM(ReceivedPacketCount) AS ReceivedPacketCount,
    SUM(TransmittedPacketCount) AS TransmittedPacketCount,
    SUM(TxOkCount) AS TxOkCount,
    SUM(TxErrorCount) AS TxErrorCount,
    MAX(GatewayLastSeenAt) AS LatestGatewaySeenAt
FROM observability.factChirpStackGatewayMetrics
GROUP BY
    TenantId,
    TimeSpan;
GO

CREATE OR ALTER VIEW serving.vwSensorGatewayHealthHourly
AS
SELECT
    GatewayId,
    TenantId,
    GatewayName,
    TimeSpan,
    GatewayState,
    GatewayLastSeenAt,
    StatsIntervalSeconds,
    ReceivedPacketCount,
    TransmittedPacketCount,
    TxOkCount,
    TxErrorCount,
    MetricsAvailable,
    ExtractedAt
FROM observability.factChirpStackGatewayMetrics;
GO
