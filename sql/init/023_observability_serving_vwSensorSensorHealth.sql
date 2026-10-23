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

IF OBJECT_ID(N'observability.vwFactRedingsAuditsDaily', N'V') IS NULL
    BEGIN
        EXEC (N'
            CREATE VIEW observability.vwFactRedingsAuditsDaily
            WITH SCHEMABINDING
            AS
            SELECT
                SensorId,
                CONVERT(DATE, LocalTimeSpan) AS LocalDate,
                SUM(ISNULL(AverageGapMinutes, CONVERT(DECIMAL(18, 6), 0))) AS AverageGapMinutesSum,
                SUM(CONVERT(BIGINT, CASE WHEN AverageGapMinutes IS NULL THEN 0 ELSE 1 END)) AS AverageGapMinutesCount,
                SUM(ReadingsCoveragePercentage) AS ReadingsCoveragePercentageSum,
                SUM(CoveredReadingBlocks) AS CoveredReadingBlocks,
                SUM(ReadingsCount) AS ReadingsCount,
                COUNT_BIG(*) AS [RowCount]
            FROM observability.factRedingsAudits
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
            object_id = OBJECT_ID(N'observability.vwFactRedingsAuditsDaily')
            AND name = N'ix_vwFactRedingsAuditsDaily_SensorId_LocalDate'
    )
    BEGIN
        CREATE UNIQUE CLUSTERED INDEX ix_vwFactRedingsAuditsDaily_SensorId_LocalDate
            ON observability.vwFactRedingsAuditsDaily (SensorId, LocalDate);
    END;
GO

IF OBJECT_ID(N'observability.vwFactRedingsAuditsWeekly', N'V') IS NULL
    BEGIN
        EXEC (N'
            CREATE VIEW observability.vwFactRedingsAuditsWeekly
            WITH SCHEMABINDING
            AS
            SELECT
                SensorId,
                DATEADD(
                    DAY,
                    -(DATEDIFF(DAY, CONVERT(DATE, ''19000101'', 112), LocalTimeSpan) % 7),
                    CONVERT(DATE, LocalTimeSpan)
                ) AS WeekStartDate,
                SUM(ISNULL(AverageGapMinutes, CONVERT(DECIMAL(18, 6), 0))) AS AverageGapMinutesSum,
                SUM(CONVERT(BIGINT, CASE WHEN AverageGapMinutes IS NULL THEN 0 ELSE 1 END)) AS AverageGapMinutesCount,
                SUM(ReadingsCoveragePercentage) AS ReadingsCoveragePercentageSum,
                SUM(CoveredReadingBlocks) AS CoveredReadingBlocks,
                SUM(ReadingsCount) AS ReadingsCount,
                COUNT_BIG(*) AS [RowCount]
            FROM observability.factRedingsAudits
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
            object_id = OBJECT_ID(N'observability.vwFactRedingsAuditsWeekly')
            AND name = N'ix_vwFactRedingsAuditsWeekly_SensorId_WeekStartDate'
    )
    BEGIN
        CREATE UNIQUE CLUSTERED INDEX ix_vwFactRedingsAuditsWeekly_SensorId_WeekStartDate
            ON observability.vwFactRedingsAuditsWeekly (SensorId, WeekStartDate);
    END;
GO

CREATE OR ALTER VIEW serving.vwSensorSensorCoverageHourly
AS
SELECT
    SensorId,
    TimeSpan,
    LocalTimeSpan,
    ReadingsCount,
    AverageGapMinutes,
    ReadingsCoveragePercentage AS CoveragePercentage
FROM observability.factRedingsAudits;
GO

CREATE OR ALTER VIEW serving.vwSensorSensorCoverageDaily
AS
SELECT
    SensorId,
    LocalDate,
    ReadingsCount,
    AverageGapMinutesSum / NULLIF(AverageGapMinutesCount, 0) AS AverageGapMinutes,
    ReadingsCoveragePercentageSum / NULLIF([RowCount], 0) AS CoveragePercentage
FROM observability.vwFactRedingsAuditsDaily WITH (NOEXPAND);
GO

CREATE OR ALTER VIEW serving.vwSensorSensorCoverageWeekly
AS
SELECT
    SensorId,
    WeekStartDate,
    ReadingsCount,
    DATEADD(DAY, 6, WeekStartDate) AS WeekEndDate,
    AverageGapMinutesSum / NULLIF(AverageGapMinutesCount, 0) AS AverageGapMinutes,
    ReadingsCoveragePercentageSum / NULLIF([RowCount], 0) AS CoveragePercentage
FROM observability.vwFactRedingsAuditsWeekly WITH (NOEXPAND);
GO

IF OBJECT_ID(N'serving.vwSensorSensorHealthDaily', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSensorSensorHealthDaily;
    END;
GO

IF OBJECT_ID(N'serving.vwSensorSensorHealthWeekly', N'V') IS NOT NULL
    BEGIN
        DROP VIEW serving.vwSensorSensorHealthWeekly;
    END;
GO
