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

IF OBJECT_ID(N'dwh.factSensorEvents', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factSensorEvents
        (
            ReadId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            LocationName VARCHAR(255) NOT NULL,
            DeviceType VARCHAR(255) NOT NULL,
            SensorType VARCHAR(255) NOT NULL,
            SensorName VARCHAR(255) NOT NULL,
            LocalTimeSpan DATETIME2(0) NULL,
            Value DECIMAL(19, 9) NOT NULL,
            EventMinutes DECIMAL(19, 9) NOT NULL,
            IsEventCompleted BIT NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factSensorEvents_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factSensorEvents
            PRIMARY KEY CLUSTERED (ReadId)
            WITH (IGNORE_DUP_KEY = ON)
        );
    END;
GO

IF OBJECT_ID(N'stg.factSensorEvents', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factSensorEvents
        (
            RunId NVARCHAR(100) NOT NULL,
            ReadId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            LocationName VARCHAR(255) NOT NULL,
            DeviceType VARCHAR(255) NOT NULL,
            SensorType VARCHAR(255) NOT NULL,
            SensorName VARCHAR(255) NOT NULL,
            LocalTimeSpan DATETIME2(0) NULL,
            Value DECIMAL(19, 9) NOT NULL,
            EventMinutes DECIMAL(19, 9) NOT NULL,
            IsEventCompleted BIT NOT NULL
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
            AND o.name = N'factSensorEvents'
            AND i.name = N'ix_factSensorEvents_sensor_time_span'
    )
    BEGIN
        CREATE INDEX ix_factSensorEvents_sensor_time_span
            ON dwh.factSensorEvents (SensorId, LocalTimeSpan);
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
            AND o.name = N'factSensorEvents'
            AND i.name = N'ix_factSensorEvents_localTimeSpan_desc'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factSensorEvents_localTimeSpan_desc
            ON dwh.factSensorEvents (LocalTimeSpan DESC);
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
            AND o.name = N'factSensorEvents'
            AND i.name = N'ix_stg_factSensorEvents_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factSensorEvents_run
            ON stg.factSensorEvents (RunId, ReadId);
    END;
GO
