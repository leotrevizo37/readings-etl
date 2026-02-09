IF OBJECT_ID(N'ctl.usp_IngestionControl_MarkRunCompleted', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_IngestionControl_MarkRunCompleted;
GO

CREATE PROCEDURE ctl.usp_IngestionControl_MarkRunCompleted
    (
        @IngestionName SYSNAME,
        @NewLastLoadedAt DATETIME2(3),
        @Succeeded BIT,
        @ErrorMessage NVARCHAR(4000) = NULL
    )
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE
        @status VARCHAR(20)
        = CASE
            WHEN @Succeeded = 1 THEN 'SUCCESS'
            ELSE 'FAILED'
        END;

    UPDATE ctl.IngestionControl WITH (ROWLOCK, UPDLOCK)
    SET
        LastLoadedAt =
        CASE
            WHEN @Succeeded = 1 THEN @NewLastLoadedAt
            ELSE LastLoadedAt
        END,
        LastRunStatus = @status,
        LastRunFinishedAt = @now,
        LastRunError = @ErrorMessage,
        UpdatedAt = @now
    WHERE
        IngestionName = @IngestionName
        AND IsActive = 1;

    IF @@ROWCOUNT = 0
        BEGIN
            RAISERROR (
                'IngestionControl: no row found for IngestionName = %s or it is inactive.',
                16,
                1,
                @IngestionName
            );
            RETURN;
        END;
END;
GO
