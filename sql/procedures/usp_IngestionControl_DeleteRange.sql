SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ctl.usp_IngestionControl_DeleteRange
    (
        @SchemaName SYSNAME,
        @TableName SYSNAME,
        @TimeColumn SYSNAME,
        @FromUtc DATETIME2(0),
        @ToUtc DATETIME2(0)
    )
AS
BEGIN
    SET NOCOUNT ON;

    IF @SchemaName IS NULL OR @TableName IS NULL OR @TimeColumn IS NULL
        BEGIN
            RAISERROR (
                'usp_IngestionControl_DeleteRange: schema, table, and time column must not be NULL.',
                16,
                1
            );
            RETURN;
        END;

    IF @FromUtc IS NULL OR @ToUtc IS NULL
        BEGIN
            RAISERROR (
                'usp_IngestionControl_DeleteRange: @FromUtc and @ToUtc must not be NULL.',
                16,
                1
            );
            RETURN;
        END;

    IF @FromUtc >= @ToUtc
        BEGIN
            RAISERROR (
                'usp_IngestionControl_DeleteRange: @FromUtc must be < @ToUtc.',
                16,
                1
            );
            RETURN;
        END;

    DECLARE
        @obj_id INT
        = OBJECT_ID(
            QUOTENAME(@SchemaName)
            + N'.' + QUOTENAME(@TableName), N'U'
        );

    IF @obj_id IS NULL
        BEGIN
            RAISERROR (
                'usp_IngestionControl_DeleteRange: target table not found.',
                16,
                1
            );
            RETURN;
        END;

    IF
        NOT EXISTS (
            SELECT 1
            FROM sys.columns AS c
            INNER JOIN sys.types AS t
                ON c.user_type_id = t.user_type_id
            WHERE
                c.object_id = @obj_id
                AND c.name = @TimeColumn
                AND t.name IN (N'datetime', N'datetime2', N'smalldatetime', N'date')
        )
        BEGIN
            RAISERROR ('usp_IngestionControl_DeleteRange: time column not found or not a datetime type.', 16, 1);
            RETURN;
        END;

    DECLARE
        @sql NVARCHAR(MAX)
        = N'DELETE FROM '
        + QUOTENAME(@SchemaName)
        + N'.' + QUOTENAME(@TableName)
        + N' WITH (ROWLOCK) '
        +
        N'WHERE '
        + QUOTENAME(@TimeColumn)
        + N' >= @FromUtc AND '
        + QUOTENAME(@TimeColumn)
        + N' < @ToUtc;'
        +
        N'SELECT CAST(@@ROWCOUNT AS INT) AS DeletedRowCount;';

    EXEC sp_executesql
        @sql,
        N'@FromUtc DATETIME2(0), @ToUtc DATETIME2(0)',
        @FromUtc = @FromUtc,
        @ToUtc = @ToUtc;
END;
GO
