IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'observability'
    )
    BEGIN
        EXEC ('CREATE SCHEMA observability');
    END;
GO

CREATE OR ALTER VIEW observability.vwSensorDevEuiUplinkReadingsDaily
AS
WITH DeviceHourly AS (
    SELECT
        DevEui,
        TimeSpan,
        CONVERT(DATE, LocalTimeSpan) AS LocalDate,
        MAX(ReceivedUplinkCount) AS ReceivedUplinkCount,
        SUM(CONVERT(BIGINT, ReadingsCount)) AS ReadingsCount
    FROM serving.vwSensorSensorCommunicationHealthHourly
    WHERE DevEui IS NOT NULL
    GROUP BY
        DevEui,
        CONVERT(DATE, LocalTimeSpan),
        TimeSpan
),

DeviceSensors AS (
    SELECT
        LOWER(LTRIM(RTRIM(PhysicalIdentifier))) AS DevEui,
        COUNT_BIG(*) AS SensorIdCount
    FROM dwh.dimTelemetryProdDimensions
    WHERE PhysicalIdentifier IS NOT NULL
    GROUP BY LOWER(LTRIM(RTRIM(PhysicalIdentifier)))
)

SELECT
    hourly.DevEui,
    hourly.LocalDate,
    sensors.SensorIdCount,
    SUM(hourly.ReceivedUplinkCount) AS ReceivedUplinkCount,
    SUM(hourly.ReadingsCount) AS ReadingsCount
FROM DeviceHourly AS hourly
INNER JOIN DeviceSensors AS sensors
    ON hourly.DevEui = sensors.DevEui
GROUP BY
    hourly.DevEui,
    hourly.LocalDate,
    sensors.SensorIdCount;
GO
