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

IF OBJECT_ID(N'dwh.factAudiencia', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factAudiencia
        (
            EventId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            PhysicalIdentifier NVARCHAR(255) NULL,
            TimeSpan DATETIME2(7) NOT NULL,
            LocalTimeSpan DATETIME2(7) NOT NULL,
            Age INT NOT NULL,
            AgeGroup NVARCHAR(255) NULL,
            IsKid BIT NOT NULL,
            Gender NVARCHAR(255) NULL,
            EmotionRaw NVARCHAR(255) NULL,
            Emotion NVARCHAR(255) NULL,
            Sentiment NVARCHAR(255) NULL,
            CONSTRAINT PK_FactAudiencia
            PRIMARY KEY CLUSTERED (EventId)
            WITH (IGNORE_DUP_KEY = ON)
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
            s.name = N'dwh'
            AND o.name = N'factAudiencia'
            AND i.name = N'ix_factaudiencia_sensor_time_span'
    )
    BEGIN
        CREATE INDEX ix_factaudiencia_sensor_time_span
            ON dwh.factAudiencia (SensorId, TimeSpan);
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
            s.name = N'dwh'
            AND o.name = N'factAudiencia'
            AND i.name = N'ix_factAudiencia_localTimeSpan_desc'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factAudiencia_localTimeSpan_desc
            ON dwh.factAudiencia (LocalTimeSpan DESC);
    END;
GO
