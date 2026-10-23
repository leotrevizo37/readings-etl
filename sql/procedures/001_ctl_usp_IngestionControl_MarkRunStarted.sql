IF OBJECT_ID(N'ctl.usp_IngestionControl_MarkRunStarted', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_IngestionControl_MarkRunStarted;
GO

CREATE PROCEDURE ctl.usp_IngestionControl_MarkRunStarted
    (
        @IngestionName SYSNAME
    )
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();

    UPDATE ctl.ingestionControl WITH (ROWLOCK, UPDLOCK)
    SET
        LastRunStatus = 'RUNNING',
        LastRunStartedAt = @now,
        LastRunFinishedAt = NULL,
        LastRunError = NULL,
        UpdatedAt = @now
    WHERE
        IngestionName = @IngestionName
        AND IsActive = 1;

    IF @@ROWCOUNT = 0
        BEGIN
            RAISERROR (
                'ingestionControl: no row found for IngestionName = %s or it is inactive.',
                16,
                1,
                @IngestionName
            );
            RETURN;
        END;
END;
GO
