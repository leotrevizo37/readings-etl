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

IF OBJECT_ID(N'observability.factTelemetryUserUsage', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factTelemetryUserUsage
        (
            UserId UNIQUEIDENTIFIER NOT NULL,
            [Date] DATE NOT NULL,
            HasConnected BIT NOT NULL,
            MadeCompleteInteraction BIT NOT NULL,
            TimeConnected BIGINT NOT NULL,
            AvgLatency BIGINT NOT NULL,
            Latency95thPercentile DECIMAL(18, 2) NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factTelemetryUserUsage_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factTelemetryUserUsage
            PRIMARY KEY CLUSTERED ([Date], UserId)
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
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'observability'
            AND o.name = N'factTelemetryUserUsage'
            AND i.name = N'ix_factTelemetryUserUsage_date'
    )
    BEGIN
        CREATE INDEX ix_factTelemetryUserUsage_date
            ON observability.factTelemetryUserUsage ([Date]);
    END;
GO

IF OBJECT_ID(N'stg.factTelemetryUserUsage', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factTelemetryUserUsage
        (
            RunId NVARCHAR(100) NOT NULL,
            UserId UNIQUEIDENTIFIER NOT NULL,
            [Date] DATE NOT NULL,
            HasConnected BIT NOT NULL,
            MadeCompleteInteraction BIT NOT NULL,
            TimeConnected BIGINT NOT NULL,
            AvgLatency BIGINT NOT NULL,
            Latency95thPercentile DECIMAL(18, 2) NULL
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
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'stg'
            AND o.name = N'factTelemetryUserUsage'
            AND i.name = N'ix_stg_factTelemetryUserUsage_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factTelemetryUserUsage_run
            ON stg.factTelemetryUserUsage (RunId, [Date], UserId);
    END;
GO
