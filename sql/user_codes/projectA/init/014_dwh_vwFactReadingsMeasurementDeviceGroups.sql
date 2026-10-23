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

CREATE OR ALTER VIEW dwh.vwFactReadingsMeasurementDeviceGroups
AS
WITH fact_dim AS (
    SELECT
        fact.LocalTimeSpan,
        fact.DeviceId,
        fact.LocationId,
        dim.SubLocationId,
        dim.sublocation_name AS SubLocationName,
        dim.location_name AS LocationName,
        dim.device_type AS DeviceType,
        dim.device_name AS DeviceName,
        dim.sensor_type AS SensorType,
        dim.sensor_name AS SensorName,
        fact.MeasurementValue,
        fact.ReadingsCount,
        fact.SensorId,
        CASE
            WHEN dim.device_type LIKE N'Refrigerador%' THEN N'Refrigeradores'
            WHEN dim.device_type LIKE N'Congelador%' THEN N'Congeladores'
            WHEN dim.device_type LIKE N'Cámara congelación%' THEN N'Congeladores'
            WHEN dim.device_type LIKE N'Camara congelacion%' THEN N'Congeladores'
            WHEN dim.device_type LIKE N'Cámara fría%' THEN N'Congeladores'
            WHEN dim.device_type LIKE N'Camara fria%' THEN N'Congeladores'
            WHEN dim.device_type LIKE N'Monitoreo camara%' THEN N'Presencia'
            WHEN dim.device_type LIKE N'Monitoreo cámara%' THEN N'Presencia'
            WHEN dim.device_type LIKE N'Nivel de cisterna%' THEN N'Cisterna'
            ELSE dim.device_type
        END AS DeviceGroup
    FROM dwh.factReadingsMeasurement AS fact
    INNER JOIN dwh.dimTelemetryProdDimensions AS dim
        ON fact.SensorId = dim.SensorId
),

group_hour AS (
    SELECT
        fact_dim.DeviceGroup,
        fact_dim.LocalTimeSpan,
        fact_dim.LocationId,
        fact_dim.LocationName
    FROM fact_dim
    GROUP BY
        fact_dim.DeviceGroup,
        fact_dim.LocalTimeSpan,
        fact_dim.LocationId,
        fact_dim.LocationName
),

device_hour AS (
    SELECT
        fact_dim.DeviceGroup,
        fact_dim.LocalTimeSpan,
        fact_dim.DeviceId,
        fact_dim.LocationId,
        fact_dim.SubLocationId,
        fact_dim.SubLocationName,
        fact_dim.LocationName,
        fact_dim.DeviceType,
        fact_dim.DeviceName
    FROM fact_dim
    GROUP BY
        fact_dim.DeviceGroup,
        fact_dim.LocalTimeSpan,
        fact_dim.DeviceId,
        fact_dim.LocationId,
        fact_dim.SubLocationId,
        fact_dim.SubLocationName,
        fact_dim.LocationName,
        fact_dim.DeviceType,
        fact_dim.DeviceName
)

SELECT
    group_hour.LocationId,
    group_hour.LocationName,
    group_hour.DeviceGroup,
    group_hour.LocalTimeSpan,
    (
        SELECT
            device_hour.DeviceId,
            device_hour.SubLocationName,
            device_hour.DeviceType,
            device_hour.DeviceName,
            JSON_QUERY((
                SELECT
                    sensor_fact.SensorType,
                    sensor_fact.SensorName,
                    sensor_fact.MeasurementValue AS [Value],
                    sensor_fact.ReadingsCount
                FROM fact_dim AS sensor_fact
                WHERE
                    sensor_fact.DeviceGroup = device_hour.DeviceGroup
                    AND sensor_fact.LocalTimeSpan = device_hour.LocalTimeSpan
                    AND sensor_fact.DeviceId = device_hour.DeviceId
                    AND sensor_fact.LocationId = device_hour.LocationId
                    AND sensor_fact.SubLocationId = device_hour.SubLocationId
                ORDER BY
                    sensor_fact.SensorType,
                    sensor_fact.SensorName,
                    sensor_fact.SensorId
                FOR JSON PATH, INCLUDE_NULL_VALUES
            )) AS Sensors
        FROM device_hour
        WHERE
            device_hour.DeviceGroup = group_hour.DeviceGroup
            AND device_hour.LocalTimeSpan = group_hour.LocalTimeSpan
            AND device_hour.LocationId = group_hour.LocationId
        ORDER BY
            device_hour.SubLocationName,
            device_hour.DeviceType,
            device_hour.DeviceName,
            device_hour.DeviceId
        FOR JSON PATH, INCLUDE_NULL_VALUES
    ) AS DevicesJson
FROM group_hour;
GO
