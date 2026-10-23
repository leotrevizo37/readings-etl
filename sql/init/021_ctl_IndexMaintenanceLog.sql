IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'ctl'
    )
    BEGIN
        EXEC ('CREATE SCHEMA ctl');
    END;
GO

IF OBJECT_ID(N'ctl.IndexMaintenanceLog', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.IndexMaintenanceLog
        (
            IndexMaintenanceLogId BIGINT IDENTITY (1, 1) NOT NULL
            CONSTRAINT PK_IndexMaintenanceLog PRIMARY KEY,
            RunId UNIQUEIDENTIFIER NOT NULL,
            SchemaName SYSNAME NOT NULL,
            TableName SYSNAME NOT NULL,
            IndexName SYSNAME NOT NULL,
            IndexId INT NOT NULL,
            PageCount BIGINT NOT NULL,
            AvgFragmentationInPercent DECIMAL(9, 4) NOT NULL,
            ActionTaken VARCHAR(20) NOT NULL,
            Succeeded BIT NOT NULL,
            ErrorMessage NVARCHAR(4000) NULL,
            StartedAt DATETIME2(3) NOT NULL,
            FinishedAt DATETIME2(3) NOT NULL
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
            s.name = N'ctl'
            AND o.name = N'IndexMaintenanceLog'
            AND i.name = N'ix_IndexMaintenanceLog_RunId'
    )
    BEGIN
        CREATE INDEX ix_IndexMaintenanceLog_RunId
            ON ctl.IndexMaintenanceLog (RunId, StartedAt);
    END;
GO
