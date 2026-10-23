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

IF OBJECT_ID(N'ctl.IngestionControl', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.IngestionControl (
            ControlIngestionId INT IDENTITY (1, 1) NOT NULL
            CONSTRAINT PK_IngestionControl PRIMARY KEY,
            IngestionName SYSNAME NOT NULL
            CONSTRAINT UQ_IngestionControl_IngestionName UNIQUE,
            LastLoadedAt DATETIME2(3) NULL,
            LastRunStatus VARCHAR(20) NOT NULL
            CONSTRAINT DF_IngestionControl_LastRunStatus
            DEFAULT ('NEVER_RUN'),
            LastRunStartedAt DATETIME2(3) NULL,
            LastRunFinishedAt DATETIME2(3) NULL,
            LastRunError NVARCHAR(4000) NULL,
            IsActive BIT NOT NULL
            CONSTRAINT DF_IngestionControl_IsActive
            DEFAULT (1),
            CreatedAt DATETIME2(3) NOT NULL
            CONSTRAINT DF_IngestionControl_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            UpdatedAt DATETIME2(3) NOT NULL
            CONSTRAINT DF_IngestionControl_UpdatedAt
            DEFAULT (SYSUTCDATETIME())
        );
    END;
GO
