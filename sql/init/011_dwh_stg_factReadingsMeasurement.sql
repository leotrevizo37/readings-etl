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

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'stg'
    )
    BEGIN
        EXEC ('CREATE SCHEMA stg');
    END;
GO

IF OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factReadingsMeasurement
        (
            OperationId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            DeviceType VARCHAR(255) NOT NULL,
            SensorType VARCHAR(255) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            MeasurementValue DECIMAL(9, 4) NULL,
            MeasurementStdDev DECIMAL(9, 4) NULL,
            [Min] DECIMAL(9, 4) NULL,
            [Max] DECIMAL(9, 4) NULL,
            SensorName NVARCHAR(255) NULL,
            Anomalies INT NULL,
            HasAnomalies BIT NULL,
            CorrectReadingsJson NVARCHAR(MAX) NULL,
            AnomalousReadingsJson NVARCHAR(MAX) NULL,
            ReadingsCount INT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factReadingsMeasurement_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factReadingsMeasurement
            PRIMARY KEY CLUSTERED (OperationId)
            WITH (IGNORE_DUP_KEY = ON)
        );
    END;
GO

IF
    OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factReadingsMeasurement', N'TimeSpan') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD TimeSpan DATETIME2(0) NULL;
    END;
GO

IF OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factReadingsMeasurement
        (
            RunId NVARCHAR(100) NOT NULL,
            OperationId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            DeviceType VARCHAR(255) NOT NULL,
            SensorType VARCHAR(255) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            MeasurementValue DECIMAL(9, 4) NULL,
            MeasurementStdDev DECIMAL(9, 4) NULL,
            [Min] DECIMAL(9, 4) NULL,
            [Max] DECIMAL(9, 4) NULL,
            SensorName NVARCHAR(255) NULL,
            Anomalies INT NULL,
            HasAnomalies BIT NULL,
            CorrectReadingsJson NVARCHAR(MAX) NULL,
            AnomalousReadingsJson NVARCHAR(MAX) NULL,
            ReadingsCount INT NULL
        );
    END;
GO

IF
    OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factReadingsMeasurement', N'TimeSpan') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD TimeSpan DATETIME2(0) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factReadingsMeasurement', N'Min') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD [Min] DECIMAL(9, 4) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factReadingsMeasurement', N'Max') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD [Max] DECIMAL(9, 4) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factReadingsMeasurement', N'SensorName') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD SensorName NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factReadingsMeasurement', N'HasAnomalies') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD HasAnomalies BIT NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factReadingsMeasurement', N'CorrectReadingsJson') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD CorrectReadingsJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factReadingsMeasurement', N'AnomalousReadingsJson') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD AnomalousReadingsJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factReadingsMeasurement', N'Min') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD [Min] DECIMAL(9, 4) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factReadingsMeasurement', N'Max') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD [Max] DECIMAL(9, 4) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factReadingsMeasurement', N'SensorName') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD SensorName NVARCHAR(255) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factReadingsMeasurement', N'HasAnomalies') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD HasAnomalies BIT NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factReadingsMeasurement', N'CorrectReadingsJson') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD CorrectReadingsJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factReadingsMeasurement', N'AnomalousReadingsJson') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD AnomalousReadingsJson NVARCHAR(MAX) NULL;
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'dwh'
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_hasAnomalies'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_hasAnomalies
            ON dwh.factReadingsMeasurement (
                HasAnomalies,
                TimeSpan DESC,
                SensorId
            );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'dwh'
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_OperationId_SensorId'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_OperationId_SensorId
            ON dwh.factReadingsMeasurement (OperationId, SensorId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'dwh'
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_OperationId_SensorTyId'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_OperationId_SensorTyId
            ON dwh.factReadingsMeasurement (OperationId, SensorTyId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'dwh'
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_timespan_SensorId'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_timespan_sensorid
            ON dwh.factReadingsMeasurement (LocalTimeSpan, SensorId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'stg'
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_stg_factReadingsMeasurement_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factReadingsMeasurement_run
            ON stg.factReadingsMeasurement (RunId, OperationId);
    END;
GO
