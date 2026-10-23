IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'serving'
    )
    BEGIN
        EXEC ('CREATE SCHEMA serving');
    END;
GO

CREATE OR ALTER VIEW serving.vwSensorSensors
AS
SELECT
    SensorId,
    PhysicalIdentifier,
    sensor_name AS SensorName,
    sensor_type AS SensorType,
    model_name AS ModelName,
    hub_model_name AS HubModelName,
    hub_name AS HubName,
    device_name AS DeviceName,
    device_type AS DeviceType,
    sublocation_name AS SubLocationName,
    sublocation_type AS SubLocationType,
    location_name AS LocationName,
    city_name AS CityName
FROM dwh.dimTelemetryProdDimensions
WHERE Active = 1;
GO
