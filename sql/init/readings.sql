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

IF OBJECT_ID(N'dwh.readings', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.readings
        (
            ReadId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NULL,
            Value DECIMAL(19, 9) NOT NULL,
            CONSTRAINT PK_readings
            PRIMARY KEY CLUSTERED (ReadId)
            WITH (IGNORE_DUP_KEY = ON) -- noqa: PRS
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
            AND o.name = N'readings'
            AND i.name = N'ix_readings_sensor_time_span'
    )
    BEGIN
        CREATE INDEX ix_readings_sensor_time_span
            ON dwh.readings (SensorId, TimeSpan);
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
            AND o.name = N'readings'
            AND i.name = N'ix_readings_localTimeSpan_desc'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_readings_localTimeSpan_desc
            ON dwh.readings (LocalTimeSpan DESC);
    END;
GO
