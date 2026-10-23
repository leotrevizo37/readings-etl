IF
    OBJECT_ID(N'dwh.factSensorEvents', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factSensorEvents', N'EventId') IS NULL
    AND COL_LENGTH(N'dwh.factSensorEvents', N'ReadId') IS NOT NULL
    BEGIN
        EXEC sys.sp_rename
            N'dwh.factSensorEvents.ReadId',
            N'EventId',
            N'COLUMN';
    END;
GO

IF
    OBJECT_ID(N'dwh.factSensorEvents', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factSensorEvents', N'ReadId') IS NULL
    BEGIN
        ALTER TABLE dwh.factSensorEvents
        ADD ReadId UNIQUEIDENTIFIER NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factSensorEvents', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factSensorEvents', N'EventId') IS NULL
    AND COL_LENGTH(N'stg.factSensorEvents', N'ReadId') IS NOT NULL
    BEGIN
        EXEC sys.sp_rename
            N'stg.factSensorEvents.ReadId',
            N'EventId',
            N'COLUMN';
    END;
GO

IF
    OBJECT_ID(N'stg.factSensorEvents', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factSensorEvents', N'ReadId') IS NULL
    BEGIN
        ALTER TABLE stg.factSensorEvents
        ADD ReadId UNIQUEIDENTIFIER NULL;
    END;
GO
