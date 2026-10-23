IF
    OBJECT_ID(N'dwh.factSensorEvents', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.factSensorEvents', N'TimeSpan') IS NULL
    BEGIN
        ALTER TABLE dwh.factSensorEvents
        ADD TimeSpan DATETIME2(0) NULL;
    END;
GO

IF
    OBJECT_ID(N'stg.factSensorEvents', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.factSensorEvents', N'TimeSpan') IS NULL
    BEGIN
        ALTER TABLE stg.factSensorEvents
        ADD TimeSpan DATETIME2(0) NULL;
    END;
GO
