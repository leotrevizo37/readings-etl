SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ctl.usp_IndexMaintenance_Run
    (
        @SchemaName SYSNAME = NULL,
        @MinPageCount INT = 1000,
        @ReorganizeThreshold DECIMAL(9, 4) = 30.0,
        @RebuildThreshold DECIMAL(9, 4) = 80.0,
        @MaxDop INT = 1,
        @UseOnlineRebuild BIT = 0
    )
AS
BEGIN
    SET NOCOUNT ON;
    SET ANSI_PADDING ON;
    SET ANSI_WARNINGS ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET ARITHABORT ON;
    SET NUMERIC_ROUNDABORT OFF;

    IF @MinPageCount IS NULL OR @MinPageCount < 0
        BEGIN
            RAISERROR ('usp_IndexMaintenance_Run: @MinPageCount must be >= 0.', 16, 1);
            RETURN;
        END;

    IF @ReorganizeThreshold IS NULL OR @RebuildThreshold IS NULL OR @ReorganizeThreshold < 0 OR @RebuildThreshold <= @ReorganizeThreshold
        BEGIN
            RAISERROR ('usp_IndexMaintenance_Run: thresholds must be non-negative and rebuild threshold must be greater than reorganize threshold.', 16, 1);
            RETURN;
        END;

    IF @MaxDop IS NULL OR @MaxDop < 0
        BEGIN
            RAISERROR ('usp_IndexMaintenance_Run: @MaxDop must be >= 0.', 16, 1);
            RETURN;
        END;

    IF @SchemaName IS NOT NULL AND SCHEMA_ID(@SchemaName) IS NULL
        BEGIN
            RAISERROR ('usp_IndexMaintenance_Run: target schema not found.', 16, 1);
            RETURN;
        END;

    DECLARE @RunId UNIQUEIDENTIFIER = NEWID();

    DECLARE
        @Indexes TABLE
        (
            RowId INT IDENTITY (1, 1) NOT NULL,
            ObjectId INT NOT NULL,
            IndexId INT NOT NULL,
            SchemaName SYSNAME NOT NULL,
            TableName SYSNAME NOT NULL,
            IndexName SYSNAME NOT NULL,
            PageCount BIGINT NOT NULL,
            AvgFragmentationInPercent DECIMAL(9, 4) NOT NULL,
            ActionTaken VARCHAR(20) NOT NULL
        );

    WITH index_stats AS (
        SELECT
            ips.object_id,
            ips.index_id,
            CAST(
                SUM(
                    CAST(ips.avg_fragmentation_in_percent AS DECIMAL(19, 4))
                    * CAST(ips.page_count AS DECIMAL(19, 4))
                )
                / NULLIF(SUM(CAST(ips.page_count AS DECIMAL(19, 4))), 0)
                AS DECIMAL(9, 4)
            ) AS avg_fragmentation_in_percent,
            SUM(CAST(ips.page_count AS BIGINT)) AS page_count
        FROM
            sys.dm_db_index_physical_stats(
                DB_ID(),
                NULL,
                NULL,
                NULL,
                'LIMITED'
            ) AS ips
        WHERE ips.index_id > 0
        GROUP BY
            ips.object_id,
            ips.index_id
    )

    INSERT INTO @Indexes
    (
        ObjectId,
        IndexId,
        SchemaName,
        TableName,
        IndexName,
        PageCount,
        AvgFragmentationInPercent,
        ActionTaken
    )
    SELECT
        stats.object_id,
        stats.index_id,
        s.name AS SchemaName,
        t.name AS TableName,
        i.name AS IndexName,
        stats.page_count,
        stats.avg_fragmentation_in_percent,
        CASE
            WHEN stats.avg_fragmentation_in_percent >= @RebuildThreshold THEN 'REBUILD'
            WHEN stats.avg_fragmentation_in_percent >= @ReorganizeThreshold THEN 'REORGANIZE'
            ELSE 'SKIP'
        END AS ActionTaken
    FROM index_stats AS stats
    INNER JOIN sys.indexes AS i
        ON
            stats.object_id = i.object_id
            AND stats.index_id = i.index_id
    INNER JOIN sys.objects AS t
        ON stats.object_id = t.object_id
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    WHERE
        (@SchemaName IS NULL OR s.name = @SchemaName)
        AND t.type IN ('U', 'V')
        AND t.is_ms_shipped = 0
        AND i.type IN (1, 2)
        AND i.is_disabled = 0
        AND i.is_hypothetical = 0
        AND stats.page_count >= @MinPageCount;

    DECLARE
        @RowId INT = 0,
        @ObjectId INT,
        @IndexId INT,
        @CurrentSchemaName SYSNAME,
        @TableName SYSNAME,
        @IndexName SYSNAME,
        @PageCount BIGINT,
        @AvgFragmentationInPercent DECIMAL(9, 4),
        @ActionTaken VARCHAR(20),
        @StartedAt DATETIME2(3),
        @FinishedAt DATETIME2(3),
        @ErrorMessage NVARCHAR(4000),
        @Sql NVARCHAR(MAX),
        @QualifiedTableName NVARCHAR(517);

    WHILE 1 = 1
        BEGIN
            SELECT TOP (1)
                @RowId = RowId,
                @ObjectId = ObjectId,
                @IndexId = IndexId,
                @CurrentSchemaName = SchemaName,
                @TableName = TableName,
                @IndexName = IndexName,
                @PageCount = PageCount,
                @AvgFragmentationInPercent = AvgFragmentationInPercent,
                @ActionTaken = ActionTaken
            FROM @Indexes
            WHERE RowId > @RowId
            ORDER BY RowId;

            IF @@ROWCOUNT = 0
                BREAK;

            SET @StartedAt = SYSUTCDATETIME();
            SET @ErrorMessage = NULL;
            SET @QualifiedTableName = QUOTENAME(@CurrentSchemaName) + N'.' + QUOTENAME(@TableName);

            BEGIN TRY
                IF @ActionTaken = 'REORGANIZE'
                    BEGIN
                        SET
                            @Sql = N'ALTER INDEX ' + QUOTENAME(@IndexName)
                            + N' ON ' + @QualifiedTableName
                            + N' REORGANIZE;';
                        EXEC sp_executesql @Sql;

                        SET
                            @Sql = N'UPDATE STATISTICS ' + @QualifiedTableName
                            + N' ' + QUOTENAME(@IndexName) + N';';
                        EXEC sp_executesql @Sql;
                    END;
                ELSE IF @ActionTaken = 'REBUILD'
                    BEGIN
                        SET
                            @Sql = N'ALTER INDEX ' + QUOTENAME(@IndexName)
                            + N' ON ' + @QualifiedTableName
                            + N' REBUILD WITH (MAXDOP = ' + CAST(@MaxDop AS NVARCHAR(10));

                        IF @UseOnlineRebuild = 1
                            BEGIN
                                SET
                                    @Sql = @Sql
                                    + N', ONLINE = ON (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 5 MINUTES, ABORT_AFTER_WAIT = SELF))';
                            END;

                        SET @Sql = @Sql + N');';
                        EXEC sp_executesql @Sql;
                    END;

                SET @FinishedAt = SYSUTCDATETIME();

                INSERT INTO ctl.IndexMaintenanceLog
                (
                    RunId,
                    SchemaName,
                    TableName,
                    IndexName,
                    IndexId,
                    PageCount,
                    AvgFragmentationInPercent,
                    ActionTaken,
                    Succeeded,
                    ErrorMessage,
                    StartedAt,
                    FinishedAt
                )
                VALUES
                (
                    @RunId,
                    @CurrentSchemaName,
                    @TableName,
                    @IndexName,
                    @IndexId,
                    @PageCount,
                    @AvgFragmentationInPercent,
                    @ActionTaken,
                    1,
                    NULL,
                    @StartedAt,
                    @FinishedAt
                );
            END TRY
            BEGIN CATCH
                SET @FinishedAt = SYSUTCDATETIME();
                SET @ErrorMessage = ERROR_MESSAGE();

                INSERT INTO ctl.IndexMaintenanceLog
                (
                    RunId,
                    SchemaName,
                    TableName,
                    IndexName,
                    IndexId,
                    PageCount,
                    AvgFragmentationInPercent,
                    ActionTaken,
                    Succeeded,
                    ErrorMessage,
                    StartedAt,
                    FinishedAt
                )
                VALUES
                (
                    @RunId,
                    @CurrentSchemaName,
                    @TableName,
                    @IndexName,
                    @IndexId,
                    @PageCount,
                    @AvgFragmentationInPercent,
                    @ActionTaken,
                    0,
                    @ErrorMessage,
                    @StartedAt,
                    @FinishedAt
                );
            END CATCH;
        END;
END;
GO
