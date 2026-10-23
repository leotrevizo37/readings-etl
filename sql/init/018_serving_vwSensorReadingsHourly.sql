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

CREATE OR ALTER VIEW serving.vwSensorReadingsHourly
AS
SELECT
    SensorId,
    TimeSpan,
    LocalTimeSpan,
    Anomalies AS AnomaliesSum,
    MeasurementValue AS AverageValue
FROM dwh.factReadingsMeasurement;
GO
