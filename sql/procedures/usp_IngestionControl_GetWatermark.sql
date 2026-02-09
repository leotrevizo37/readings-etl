IF OBJECT_ID(N'ctl.usp_IngestionControl_GetWatermark', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_IngestionControl_GetWatermark;
GO

CREATE PROCEDURE ctl.usp_IngestionControl_GetWatermark
    (
        @IngestionName SYSNAME
    )
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        ControlIngestionId,
        IngestionName,
        LastLoadedAt,
        LastRunStatus,
        LastRunStartedAt,
        LastRunFinishedAt,
        LastRunError,
        IsActive,
        CreatedAt,
        UpdatedAt
    FROM ctl.IngestionControl WITH (READCOMMITTED)
    WHERE
        IngestionName = @IngestionName
        AND IsActive = 1;

    IF @@ROWCOUNT = 0
        BEGIN
            DECLARE @now DATETIME2(3) = SYSUTCDATETIME();

            INSERT INTO ctl.IngestionControl
            (
                IngestionName,
                LastRunStatus,
                LastRunStartedAt,
                LastRunFinishedAt,
                LastRunError,
                UpdatedAt
            )
            VALUES
            (
                @IngestionName,
                'RUNNING',
                @now,
                NULL,
                'IngestionControl: no row found for IngestionName = %s or it is inactive. New one created',
                @now
            )
        END;
END;
GO
