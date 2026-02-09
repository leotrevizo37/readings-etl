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

IF OBJECT_ID(N'dwh.dimDerivedStatus', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimDerivedStatus
        (
            StatusId UNIQUEIDENTIFIER NOT NULL,
            StatusCode SMALLINT IDENTITY (1, 1) PRIMARY KEY,
            Description NVARCHAR(20) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimDerivedStatus_ModifiedAt
            DEFAULT (SYSUTCDATETIME())
        );

        SET IDENTITY_INSERT dwh.dimDerivedStatus ON;

        INSERT INTO dwh.dimDerivedStatus (StatusId, StatusCode, Description)
        VALUES
        (NEWID(), 1, N'Funcionando OK'),
        (NEWID(), 2, N'Baja presión'),
        (NEWID(), 3, N'Sin funcionar'),
        (NEWID(), 4, N'Temp crítica');

        SET IDENTITY_INSERT dwh.dimDerivedStatus OFF;
    END;
GO
