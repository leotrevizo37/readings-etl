IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'dwh'
    )
    BEGIN
        EXEC ('CREATE SCHEMA dwh');
    END;
GO

CREATE OR ALTER VIEW dwh.vwFactReadingsMeasurementDevice
AS
SELECT
    device_hour.DeviceId,
    device_hour.LocationId,
    device_hour.LocationName,
    device_hour.SubLocationName,
    device_hour.DeviceType,
    device_hour.DeviceName,
    device_hour.LocalTimeSpan,
    (
        SELECT
            sensor_dim.sensor_type AS SensorType,
            sensor_dim.sensor_name AS SensorName,
            sensor_fact.MeasurementValue AS [Value],
            sensor_fact.ReadingsCount
        FROM dwh.factReadingsMeasurement AS sensor_fact
        INNER JOIN dwh.dimTelemetryProdDimensions AS sensor_dim
            ON sensor_fact.SensorId = sensor_dim.SensorId
        WHERE
            sensor_fact.DeviceId = device_hour.DeviceId
            AND sensor_fact.LocationId = device_hour.LocationId
            AND sensor_dim.SubLocationId = device_hour.SubLocationId
            AND sensor_fact.LocalTimeSpan = device_hour.LocalTimeSpan
        ORDER BY
            sensor_dim.sensor_type,
            sensor_dim.sensor_name,
            sensor_fact.SensorId
        FOR JSON PATH, INCLUDE_NULL_VALUES
    ) AS SensorsJson
FROM (
    SELECT
        fact.DeviceId,
        fact.LocationId,
        dim.SubLocationId,
        dim.sublocation_name AS SubLocationName,
        dim.location_name AS LocationName,
        dim.device_type AS DeviceType,
        dim.device_name AS DeviceName,
        fact.LocalTimeSpan
    FROM dwh.factReadingsMeasurement AS fact
    INNER JOIN dwh.dimTelemetryProdDimensions AS dim
        ON fact.SensorId = dim.SensorId
    GROUP BY
        fact.DeviceId,
        fact.LocationId,
        dim.SubLocationId,
        dim.sublocation_name,
        dim.location_name,
        dim.device_type,
        dim.device_name,
        fact.LocalTimeSpan
) AS device_hour;
GO
