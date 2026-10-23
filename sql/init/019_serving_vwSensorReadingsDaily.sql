SET NUMERIC_ROUNDABORT OFF;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

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

IF OBJECT_ID(N'dwh.vwFactReadingsMeasurementDaily', N'V') IS NULL
    BEGIN
        EXEC (N'
            CREATE VIEW dwh.vwFactReadingsMeasurementDaily
            WITH SCHEMABINDING
            AS
            SELECT
                SensorId,
                CONVERT(DATE, LocalTimeSpan) AS LocalDate,
                SUM(ISNULL(MeasurementValue, CONVERT(DECIMAL(9, 4), 0))) AS MeasurementValueSum,
                COUNT_BIG(MeasurementValue) AS MeasurementValueCount,
                SUM(ISNULL(Anomalies, 0)) AS AnomaliesSum,
                COUNT_BIG(*) AS [RowCount]
            FROM dwh.factReadingsMeasurement
            GROUP BY
                SensorId,
                CONVERT(DATE, LocalTimeSpan);
        ');
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'dwh.vwFactReadingsMeasurementDaily')
            AND name = N'ix_vwFactReadingsMeasurementDaily_SensorId_LocalDate'
    )
    BEGIN
        CREATE UNIQUE CLUSTERED INDEX ix_vwFactReadingsMeasurementDaily_SensorId_LocalDate
            ON dwh.vwFactReadingsMeasurementDaily (SensorId, LocalDate);
    END;
GO

CREATE OR ALTER VIEW serving.vwSensorReadingsDaily
AS
SELECT
    SensorId,
    LocalDate,
    AnomaliesSum,
    MeasurementValueSum / NULLIF(MeasurementValueCount, 0) AS AverageValue
FROM dwh.vwFactReadingsMeasurementDaily WITH (NOEXPAND);
GO

IF OBJECT_ID(N'dwh.vwFactReadingsMeasurementWeekly', N'V') IS NULL
    BEGIN
        EXEC (N'
            CREATE VIEW dwh.vwFactReadingsMeasurementWeekly
            WITH SCHEMABINDING
            AS
            SELECT
                SensorId,
                DATEADD(
                    DAY,
                    -(DATEDIFF(DAY, CONVERT(DATE, ''19000101'', 112), LocalTimeSpan) % 7),
                    CONVERT(DATE, LocalTimeSpan)
                ) AS WeekStartDate,
                SUM(ISNULL(MeasurementValue, CONVERT(DECIMAL(9, 4), 0))) AS MeasurementValueSum,
                COUNT_BIG(MeasurementValue) AS MeasurementValueCount,
                SUM(ISNULL(Anomalies, 0)) AS AnomaliesSum,
                COUNT_BIG(*) AS [RowCount]
            FROM dwh.factReadingsMeasurement
            GROUP BY
                SensorId,
                DATEADD(
                    DAY,
                    -(DATEDIFF(DAY, CONVERT(DATE, ''19000101'', 112), LocalTimeSpan) % 7),
                    CONVERT(DATE, LocalTimeSpan)
                );
        ');
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'dwh.vwFactReadingsMeasurementWeekly')
            AND name = N'ix_vwFactReadingsMeasurementWeekly_SensorId_WeekStartDate'
    )
    BEGIN
        CREATE UNIQUE CLUSTERED INDEX ix_vwFactReadingsMeasurementWeekly_SensorId_WeekStartDate
            ON dwh.vwFactReadingsMeasurementWeekly (SensorId, WeekStartDate);
    END;
GO

CREATE OR ALTER VIEW serving.vwSensorReadingsWeekly
AS
SELECT
    SensorId,
    WeekStartDate,
    AnomaliesSum,
    DATEADD(DAY, 6, WeekStartDate) AS WeekEndDate,
    MeasurementValueSum / NULLIF(MeasurementValueCount, 0) AS AverageValue
FROM dwh.vwFactReadingsMeasurementWeekly WITH (NOEXPAND);
GO
