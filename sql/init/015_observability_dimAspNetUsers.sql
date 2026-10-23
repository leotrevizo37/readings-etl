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

IF OBJECT_ID(N'observability.dimAspNetUsers', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.dimAspNetUsers
        (
            Id UNIQUEIDENTIFIER NOT NULL,
            UserName NVARCHAR(256) NULL,
            Name NVARCHAR(50) NULL,
            LastName NVARCHAR(50) NULL,
            Position NVARCHAR(100) NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimAspNetUsers_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimAspNetUsers_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimAspNetUsers
            PRIMARY KEY CLUSTERED (Id)
        );
    END;
GO

IF OBJECT_ID(N'stg.dimAspNetUsers', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.dimAspNetUsers
        (
            RunId NVARCHAR(100) NOT NULL,
            Id UNIQUEIDENTIFIER NOT NULL,
            UserName NVARCHAR(256) NULL,
            Name NVARCHAR(50) NULL,
            LastName NVARCHAR(50) NULL,
            Position NVARCHAR(100) NULL
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
            AND o.name = N'dimAspNetUsers'
            AND i.name = N'ix_stg_dimAspNetUsers_run'
    )
    BEGIN
        CREATE INDEX ix_stg_dimAspNetUsers_run
            ON stg.dimAspNetUsers (RunId, Id);
    END;
GO
