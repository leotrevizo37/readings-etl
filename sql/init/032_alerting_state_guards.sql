SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_config_guard
ON alerting.alert_config
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS current_row
            LEFT JOIN deleted AS previous_row
                ON current_row.alert_config_id = previous_row.alert_config_id
            INNER JOIN alerting.dim_config_status AS current_status
                ON current_row.config_status_id = current_status.config_status_id
            WHERE
                previous_row.alert_config_id IS NULL
                AND current_status.code <> N'DRAFT'
        )
        THROW 51024, 'New alert configurations must start as drafts.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            INNER JOIN inserted AS current_row
                ON previous_row.alert_config_id = current_row.alert_config_id
            WHERE
                current_row.user_code <> previous_row.user_code
                OR current_row.version_number <> previous_row.version_number
                OR current_row.created_at <> previous_row.created_at
        )
        THROW 51025, 'Alert configuration identity is immutable.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            INNER JOIN inserted AS current_row
                ON previous_row.alert_config_id = current_row.alert_config_id
            INNER JOIN alerting.dim_config_status AS previous_status
                ON previous_row.config_status_id = previous_status.config_status_id
            INNER JOIN alerting.dim_config_status AS current_status
                ON current_row.config_status_id = current_status.config_status_id
            WHERE
                previous_status.code = N'DRAFT'
                AND current_status.code NOT IN (N'DRAFT', N'ACTIVE')
        )
        THROW 51026, 'Draft alert configurations can only become active.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS current_row
            INNER JOIN alerting.dim_config_status AS current_status
                ON current_row.config_status_id = current_status.config_status_id
            WHERE
                current_row.updated_at < current_row.created_at
                OR current_row.activated_at < current_row.created_at
                OR current_row.retired_at < current_row.activated_at
                OR (
                    current_status.code = N'DRAFT'
                    AND (
                        current_row.activated_at IS NOT NULL
                        OR current_row.retired_at IS NOT NULL
                    )
                )
                OR (
                    current_status.code = N'ACTIVE'
                    AND (
                        current_row.activated_at IS NULL
                        OR current_row.retired_at IS NOT NULL
                    )
                )
                OR (
                    current_status.code = N'RETIRED'
                    AND (
                        current_row.activated_at IS NULL
                        OR current_row.retired_at IS NULL
                    )
                )
        )
        THROW 51027, 'Alert configuration status timestamps are invalid.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.alert_config_id = current_row.alert_config_id
            INNER JOIN alerting.dim_config_status AS previous_status
                ON previous_row.config_status_id = previous_status.config_status_id
            WHERE
                current_row.alert_config_id IS NULL
                AND previous_status.code <> N'DRAFT'
        )
        THROW 51009, 'Active and retired alert configurations cannot be deleted.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS current_row
            INNER JOIN alerting.dim_config_status AS status
                ON current_row.config_status_id = status.config_status_id
            INNER JOIN serving.vwValidacionConfiguracionesAlerta AS validation
                ON current_row.alert_config_id = validation.ConfiguracionAlertaId
            WHERE
                status.code = N'ACTIVE'
                AND validation.EsBloqueante = 1
        )
        THROW 51010, 'An invalid alert configuration cannot be activated.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            INNER JOIN inserted AS current_row
                ON previous_row.alert_config_id = current_row.alert_config_id
            INNER JOIN alerting.dim_config_status AS previous_status
                ON previous_row.config_status_id = previous_status.config_status_id
            WHERE previous_status.code = N'RETIRED'
        )
        THROW 51011, 'Retired alert configurations are immutable.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            INNER JOIN inserted AS current_row
                ON previous_row.alert_config_id = current_row.alert_config_id
            INNER JOIN alerting.dim_config_status AS previous_status
                ON previous_row.config_status_id = previous_status.config_status_id
            INNER JOIN alerting.dim_config_status AS current_status
                ON current_row.config_status_id = current_status.config_status_id
            WHERE
                previous_status.code = N'ACTIVE'
                AND (
                    current_status.code <> N'RETIRED'
                    OR current_row.user_code <> previous_row.user_code
                    OR current_row.version_number <> previous_row.version_number
                )
        )
        THROW 51012, 'Active alert configurations can only be retired.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_signal_event_append_only
ON alerting.alert_signal_event
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51021, 'Alert signal events are append-only.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_workflow_event_append_only
ON alerting.alert_workflow_event
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51022, 'Alert workflow events are append-only.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_action_event_append_only
ON alerting.alert_action_event
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51023, 'Alert action events are append-only.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_guard
ON alerting.alert_rule
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM (
                SELECT alert_config_id FROM inserted
                UNION
                SELECT alert_config_id FROM deleted
            ) AS changed
            INNER JOIN alerting.alert_config AS config
                ON changed.alert_config_id = config.alert_config_id
            INNER JOIN alerting.dim_config_status AS status
                ON config.config_status_id = status.config_status_id
            WHERE status.code <> N'DRAFT'
        )
        THROW 51013, 'Rules outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_sensor_guard
ON alerting.alert_rule
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS current_row
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON current_row.alert_type_id = alert_type.alert_type_id
            WHERE
                alert_type.code IN (N'connection_lost', N'late_readings')
                AND current_row.entity_key IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM dwh.dimTelemetryProdDimensions AS dimension
                    WHERE
                        current_row.entity_key
                        = LOWER(CAST(dimension.SensorId AS NVARCHAR(36)))
                        COLLATE Latin1_General_100_BIN2
                        AND DATALENGTH(current_row.entity_key) = 72
                        AND dimension.Active = 1
                )
        )
        THROW 51028, 'Sensor rules must reference an existing active SensorId using its canonical lowercase GUID.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_ingestion_guard
ON alerting.alert_rule
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS current_row
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON current_row.alert_type_id = alert_type.alert_type_id
            WHERE
                alert_type.code IN (N'pipeline_failure', N'pipeline_stale')
                AND current_row.entity_key IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM ctl.IngestionControl AS ingestion
                    WHERE
                        current_row.entity_key
                        = ingestion.IngestionName COLLATE Latin1_General_100_BIN2
                        AND DATALENGTH(current_row.entity_key)
                        = DATALENGTH(ingestion.IngestionName)
                        AND ingestion.IsActive = 1
                )
        )
        THROW 51029, 'Pipeline rules must reference an existing active IngestionName.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_no_readings_guard
ON alerting.alert_rule_no_readings
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM (
                SELECT alert_rule_id FROM inserted
                UNION
                SELECT alert_rule_id FROM deleted
            ) AS changed
            INNER JOIN alerting.alert_rule AS alert_rule
                ON changed.alert_rule_id = alert_rule.alert_rule_id
            INNER JOIN alerting.alert_config AS config
                ON alert_rule.alert_config_id = config.alert_config_id
            INNER JOIN alerting.dim_config_status AS status
                ON config.config_status_id = status.config_status_id
            WHERE status.code <> N'DRAFT'
        )
        THROW 51014, 'Rule details outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_pipeline_stale_guard
ON alerting.alert_rule_pipeline_stale
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM (
                SELECT alert_rule_id FROM inserted
                UNION
                SELECT alert_rule_id FROM deleted
            ) AS changed
            INNER JOIN alerting.alert_rule AS alert_rule
                ON changed.alert_rule_id = alert_rule.alert_rule_id
            INNER JOIN alerting.alert_config AS config
                ON alert_rule.alert_config_id = config.alert_config_id
            INNER JOIN alerting.dim_config_status AS status
                ON config.config_status_id = status.config_status_id
            WHERE status.code <> N'DRAFT'
        )
        THROW 51015, 'Rule details outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_rule_row_count_trend_guard
ON alerting.alert_rule_row_count_trend
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM (
                SELECT alert_rule_id FROM inserted
                UNION
                SELECT alert_rule_id FROM deleted
            ) AS changed
            INNER JOIN alerting.alert_rule AS alert_rule
                ON changed.alert_rule_id = alert_rule.alert_rule_id
            INNER JOIN alerting.alert_config AS config
                ON alert_rule.alert_config_id = config.alert_config_id
            INNER JOIN alerting.dim_config_status AS status
                ON config.config_status_id = status.config_status_id
            WHERE status.code <> N'DRAFT'
        )
        THROW 51016, 'Rule details outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_detector_operational_config_guard
ON alerting.alert_detector_operational_config
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM (
                SELECT alert_config_id FROM inserted
                UNION
                SELECT alert_config_id FROM deleted
            ) AS changed
            INNER JOIN alerting.alert_config AS config
                ON changed.alert_config_id = config.alert_config_id
            INNER JOIN alerting.dim_config_status AS status
                ON config.config_status_id = status.config_status_id
            WHERE status.code <> N'DRAFT'
        )
        THROW 51017, 'Detector settings outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_detector_row_count_config_guard
ON alerting.alert_detector_row_count_config
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM (
                SELECT alert_config_id FROM inserted
                UNION
                SELECT alert_config_id FROM deleted
            ) AS changed
            INNER JOIN alerting.alert_config AS config
                ON changed.alert_config_id = config.alert_config_id
            INNER JOIN alerting.dim_config_status AS status
                ON config.config_status_id = status.config_status_id
            WHERE status.code <> N'DRAFT'
        )
        THROW 51018, 'Detector settings outside draft configurations are immutable.', 1;
END;
GO

CREATE OR ALTER PROCEDURE alerting.usp_AlertConfig_Activate
    @alert_config_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @user_code NVARCHAR(100);
    DECLARE @active_status_id UNIQUEIDENTIFIER;
    DECLARE @retired_status_id UNIQUEIDENTIFIER;

    SELECT @active_status_id = config_status_id
    FROM alerting.dim_config_status
    WHERE code = N'ACTIVE';

    SELECT @retired_status_id = config_status_id
    FROM alerting.dim_config_status
    WHERE code = N'RETIRED';

    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT @user_code = config.user_code
        FROM alerting.alert_config AS config WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN alerting.dim_config_status AS status
            ON config.config_status_id = status.config_status_id
        WHERE
            config.alert_config_id = @alert_config_id
            AND status.code = N'DRAFT';

        IF @user_code IS NULL
            THROW 51019, 'The alert configuration does not exist or is not a draft.', 1;

        IF
            EXISTS (
                SELECT 1
                FROM serving.vwValidacionConfiguracionesAlerta
                WHERE
                    ConfiguracionAlertaId = @alert_config_id
                    AND EsBloqueante = 1
            )
            THROW 51020, 'The alert configuration has blocking validation errors.', 1;

        UPDATE alerting.alert_config
        SET
            config_status_id = @retired_status_id,
            retired_at = SYSUTCDATETIME(),
            updated_at = SYSUTCDATETIME()
        WHERE
            user_code = @user_code
            AND config_status_id = @active_status_id;

        UPDATE alerting.alert_config
        SET
            config_status_id = @active_status_id,
            activated_at = SYSUTCDATETIME(),
            retired_at = NULL,
            updated_at = SYSUTCDATETIME()
        WHERE alert_config_id = @alert_config_id;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO
