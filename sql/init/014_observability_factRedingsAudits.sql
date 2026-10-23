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

IF OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factRedingsAudits
        (
            SensorId UNIQUEIDENTIFIER NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            ReadingsCount INT NOT NULL,
            AverageGapMinutes DECIMAL(18, 6) NULL,
            MaximumGapMinutes DECIMAL(18, 6) NULL,
            MinimumGapMinutes DECIMAL(18, 6) NULL,
            ReadingsCoveragePercentage DECIMAL(7, 4) NOT NULL,
            CoveredReadingBlocks INT NOT NULL,
            HasLateReadings BIT NOT NULL,
            IsConnectionLost BIT NOT NULL,
            LastReadingAt DATETIME2(0) NULL,
            ConnectionLostAt DATETIME2(0) NULL,
            MinutesWithoutReadings INT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factRedingsAudits_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factRedingsAudits
            PRIMARY KEY CLUSTERED (SensorId, TimeSpan)
        );
    END;
GO

IF OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factRedingsAudits
        (
            RunId NVARCHAR(100) NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            ReadingsCount INT NOT NULL,
            AverageGapMinutes DECIMAL(18, 6) NULL,
            MaximumGapMinutes DECIMAL(18, 6) NULL,
            MinimumGapMinutes DECIMAL(18, 6) NULL,
            ReadingsCoveragePercentage DECIMAL(7, 4) NOT NULL,
            CoveredReadingBlocks INT NOT NULL,
            HasLateReadings BIT NOT NULL,
            IsConnectionLost BIT NOT NULL,
            LastReadingAt DATETIME2(0) NULL,
            ConnectionLostAt DATETIME2(0) NULL,
            MinutesWithoutReadings INT NULL
        );
    END;
GO

IF
    OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'observability.factRedingsAudits', N'AverageGapMinutes') IS NULL
    BEGIN
        ALTER TABLE observability.factRedingsAudits
        ADD AverageGapMinutes DECIMAL(18, 6) NULL;
    END;
GO

IF
    OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'observability.factRedingsAudits', N'MaximumGapMinutes') IS NULL
    BEGIN
        ALTER TABLE observability.factRedingsAudits
        ADD MaximumGapMinutes DECIMAL(18, 6) NULL;
    END;
GO

IF
    OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'observability.factRedingsAudits', N'MinimumGapMinutes') IS NULL
    BEGIN
        ALTER TABLE observability.factRedingsAudits
        ADD MinimumGapMinutes DECIMAL(18, 6) NULL;
    END;
GO

IF
    OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'observability.factRedingsAudits', N'ReadingsCoveragePercentage') IS NULL
    BEGIN
        ALTER TABLE observability.factRedingsAudits
        ADD ReadingsCoveragePercentage DECIMAL(7, 4) NOT NULL
        CONSTRAINT DF_factRedingsAudits_ReadingsCoveragePercentage
        DEFAULT (0);
    END;
GO

IF
    OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'observability.factRedingsAudits', N'CoveredReadingBlocks') IS NULL
    BEGIN
        ALTER TABLE observability.factRedingsAudits
        ADD CoveredReadingBlocks INT NOT NULL
        CONSTRAINT DF_factRedingsAudits_CoveredReadingBlocks
        DEFAULT (0);
    END;
GO

IF
    OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factRedingsAudits', N'AverageGapMinutes') IS NULL
    BEGIN
        ALTER TABLE stg.factRedingsAudits
        ADD AverageGapMinutes DECIMAL(18, 6) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factRedingsAudits', N'MaximumGapMinutes') IS NULL
    BEGIN
        ALTER TABLE stg.factRedingsAudits
        ADD MaximumGapMinutes DECIMAL(18, 6) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factRedingsAudits', N'MinimumGapMinutes') IS NULL
    BEGIN
        ALTER TABLE stg.factRedingsAudits
        ADD MinimumGapMinutes DECIMAL(18, 6) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factRedingsAudits', N'ReadingsCoveragePercentage') IS NULL
    BEGIN
        ALTER TABLE stg.factRedingsAudits
        ADD ReadingsCoveragePercentage DECIMAL(7, 4) NOT NULL
        CONSTRAINT DF_stg_factRedingsAudits_ReadingsCoveragePercentage
        DEFAULT (0);
    END;
GO

IF
    OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factRedingsAudits', N'CoveredReadingBlocks') IS NULL
    BEGIN
        ALTER TABLE stg.factRedingsAudits
        ADD CoveredReadingBlocks INT NOT NULL
        CONSTRAINT DF_stg_factRedingsAudits_CoveredReadingBlocks
        DEFAULT (0);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'observability'
            AND o.name = N'factRedingsAudits'
            AND i.name = N'ix_factRedingsAudits_timeSpan'
    )
    BEGIN
        CREATE INDEX ix_factRedingsAudits_timeSpan
            ON observability.factRedingsAudits (TimeSpan DESC, SensorId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'observability'
            AND o.name = N'factRedingsAudits'
            AND i.name = N'ix_factRedingsAudits_localtimeSpan'
    )
    BEGIN
        CREATE INDEX ix_factRedingsAudits_localtimeSpan
            ON observability.factRedingsAudits (LocalTimeSpan DESC, SensorId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'stg'
            AND o.name = N'factRedingsAudits'
            AND i.name = N'ix_stg_factRedingsAudits_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factRedingsAudits_run
            ON stg.factRedingsAudits (RunId, SensorId, TimeSpan);
    END;
GO
