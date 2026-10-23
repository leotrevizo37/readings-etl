SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_alert_type_stable
ON alerting.dim_alert_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.alert_type_id = current_row.alert_type_id
            WHERE
                current_row.alert_type_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
                OR current_row.is_continuous <> previous_row.is_continuous
        )
        THROW 51030, 'Alert type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_signal_type_stable
ON alerting.dim_signal_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.signal_type_id = current_row.signal_type_id
            WHERE
                current_row.signal_type_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51031, 'Signal type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_severity_stable
ON alerting.dim_severity
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.severity_id = current_row.severity_id
            WHERE
                current_row.severity_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51032, 'Severity identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_trigger_source_stable
ON alerting.dim_trigger_source
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.trigger_source_id = current_row.trigger_source_id
            WHERE
                current_row.trigger_source_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51033, 'Trigger source identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_config_status_stable
ON alerting.dim_config_status
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.config_status_id = current_row.config_status_id
            WHERE
                current_row.config_status_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51034, 'Configuration status identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_workflow_status_stable
ON alerting.dim_workflow_status
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.workflow_status_id = current_row.workflow_status_id
            WHERE
                current_row.workflow_status_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51035, 'Workflow status identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_closure_type_stable
ON alerting.dim_closure_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.closure_type_id = current_row.closure_type_id
            WHERE
                current_row.closure_type_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51036, 'Closure type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_action_type_stable
ON alerting.dim_action_type
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.action_type_id = current_row.action_type_id
            WHERE
                current_row.action_type_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51037, 'Action type identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_action_operation_stable
ON alerting.dim_action_operation
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.action_operation_id = current_row.action_operation_id
            WHERE
                current_row.action_operation_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51038, 'Action operation identifiers and codes are stable.', 1;
END;
GO

CREATE OR ALTER TRIGGER alerting.tr_dim_action_status_stable
ON alerting.dim_action_status
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF
        EXISTS (
            SELECT 1
            FROM deleted AS previous_row
            LEFT JOIN inserted AS current_row
                ON previous_row.action_status_id = current_row.action_status_id
            WHERE
                current_row.action_status_id IS NULL
                OR current_row.code <> previous_row.code
                OR current_row.sort_order <> previous_row.sort_order
        )
        THROW 51039, 'Action status identifiers and codes are stable.', 1;
END;
GO
