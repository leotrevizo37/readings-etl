SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'alerting.dim_email_recipient', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_email_recipient (
            recipient_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_email_recipient PRIMARY KEY,
            display_name NVARCHAR(250) NULL,
            email_address NVARCHAR(320) COLLATE Latin1_General_100_BIN2 NOT NULL,
            is_active BIT NOT NULL
            CONSTRAINT DF_dim_email_recipient_active DEFAULT (0),
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_dim_email_recipient_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_dim_email_recipient_updated DEFAULT (SYSUTCDATETIME()),
            version ROWVERSION,
            CONSTRAINT UQ_dim_email_recipient_address UNIQUE (email_address),
            CONSTRAINT CK_dim_email_recipient_address CHECK (
                email_address LIKE N'%_@_%'
                AND LEN(email_address) - LEN(REPLACE(email_address, N'@', N'')) = 1
                AND email_address NOT LIKE N'%[ ,;<>]%'
                AND CHARINDEX(NCHAR(9), email_address) = 0
                AND CHARINDEX(NCHAR(10), email_address) = 0
                AND CHARINDEX(NCHAR(13), email_address) = 0
                AND SUBSTRING(email_address, CHARINDEX(N'@', email_address) + 1, 320)
                = LOWER(SUBSTRING(email_address, CHARINDEX(N'@', email_address) + 1, 320))
            ),
            CONSTRAINT CK_dim_email_recipient_dates CHECK (updated_at >= created_at)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_email_route', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_email_route (
            email_route_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_email_route PRIMARY KEY,
            alert_type_id UNIQUEIDENTIFIER NOT NULL,
            severity_id UNIQUEIDENTIFIER NULL,
            entity_key NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NULL,
            is_enabled BIT NOT NULL
            CONSTRAINT DF_alert_email_route_enabled DEFAULT (0),
            send_recovery BIT NOT NULL
            CONSTRAINT DF_alert_email_route_recovery DEFAULT (0),
            stop_on_acknowledged BIT NOT NULL
            CONSTRAINT DF_alert_email_route_acknowledged DEFAULT (1),
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_email_route_created DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_email_route_updated DEFAULT (SYSUTCDATETIME()),
            version ROWVERSION,
            CONSTRAINT UQ_alert_email_route_scope
            UNIQUE (alert_type_id, severity_id, entity_key),
            CONSTRAINT FK_alert_email_route_type FOREIGN KEY (alert_type_id)
            REFERENCES alerting.dim_alert_type (alert_type_id),
            CONSTRAINT FK_alert_email_route_severity FOREIGN KEY (severity_id)
            REFERENCES alerting.dim_severity (severity_id),
            CONSTRAINT CK_alert_email_route_entity
            CHECK (entity_key IS NULL OR LEN(entity_key) > 0),
            CONSTRAINT CK_alert_email_route_dates CHECK (updated_at >= created_at)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_email_route_step', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_email_route_step (
            email_route_step_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_email_route_step PRIMARY KEY,
            email_route_id UNIQUEIDENTIFIER NOT NULL,
            step_number SMALLINT NOT NULL,
            delay_minutes INT NOT NULL,
            is_enabled BIT NOT NULL
            CONSTRAINT DF_alert_email_route_step_enabled DEFAULT (0),
            CONSTRAINT UQ_alert_email_route_step_number
            UNIQUE (email_route_id, step_number),
            CONSTRAINT UQ_alert_email_route_step_route
            UNIQUE (email_route_step_id, email_route_id),
            CONSTRAINT FK_alert_email_route_step_route FOREIGN KEY (email_route_id)
            REFERENCES alerting.alert_email_route (email_route_id),
            CONSTRAINT CK_alert_email_route_step_delay CHECK (
                (step_number = 0 AND delay_minutes = 0)
                OR (step_number > 0 AND delay_minutes > 0)
            )
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_email_route_recipient', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_email_route_recipient (
            email_route_step_id UNIQUEIDENTIFIER NOT NULL,
            recipient_id UNIQUEIDENTIFIER NOT NULL,
            CONSTRAINT PK_alert_email_route_recipient
            PRIMARY KEY (email_route_step_id, recipient_id),
            CONSTRAINT FK_alert_email_route_recipient_step
            FOREIGN KEY (email_route_step_id)
            REFERENCES alerting.alert_email_route_step (email_route_step_id),
            CONSTRAINT FK_alert_email_route_recipient_recipient
            FOREIGN KEY (recipient_id)
            REFERENCES alerting.dim_email_recipient (recipient_id)
        );
    END;
GO

IF OBJECT_ID(N'alerting.alert_email_action', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_email_action (
            action_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_alert_email_action PRIMARY KEY,
            alert_id UNIQUEIDENTIFIER NOT NULL,
            source_signal_event_id UNIQUEIDENTIFIER NOT NULL,
            recipient_id UNIQUEIDENTIFIER NOT NULL,
            email_address NVARCHAR(320) COLLATE Latin1_General_100_BIN2 NOT NULL,
            purpose NVARCHAR(20) COLLATE Latin1_General_100_BIN2 NOT NULL,
            email_route_id UNIQUEIDENTIFIER NULL,
            email_route_step_id UNIQUEIDENTIFIER NULL,
            route_version BINARY(8) NULL,
            parent_action_id UNIQUEIDENTIFIER NULL,
            idempotency_key NVARCHAR(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
            not_before DATETIME2(3) NOT NULL,
            requested_by NVARCHAR(250) NULL,
            request_reason NVARCHAR(1000) NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_email_action_created DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT UQ_alert_email_action_idempotency UNIQUE (idempotency_key),
            CONSTRAINT UQ_alert_email_action_incident UNIQUE (action_id, alert_id),
            CONSTRAINT FK_alert_email_action_alert FOREIGN KEY (alert_id)
            REFERENCES alerting.alert_incident (alert_id),
            CONSTRAINT FK_alert_email_action_signal FOREIGN KEY (source_signal_event_id)
            REFERENCES alerting.alert_signal_event (event_id),
            CONSTRAINT FK_alert_email_action_recipient FOREIGN KEY (recipient_id)
            REFERENCES alerting.dim_email_recipient (recipient_id),
            CONSTRAINT FK_alert_email_action_step
            FOREIGN KEY (email_route_step_id, email_route_id)
            REFERENCES alerting.alert_email_route_step (email_route_step_id, email_route_id),
            CONSTRAINT FK_alert_email_action_parent
            FOREIGN KEY (parent_action_id, alert_id)
            REFERENCES alerting.alert_email_action (action_id, alert_id),
            CONSTRAINT CK_alert_email_action_purpose
            CHECK (purpose IN (N'DETECTION', N'RECOVERY', N'ESCALATION', N'FORWARD')),
            CONSTRAINT CK_alert_email_action_route CHECK (
                (email_route_id IS NOT NULL AND email_route_step_id IS NOT NULL AND route_version IS NOT NULL AND purpose <> N'FORWARD')
                OR (email_route_id IS NULL AND email_route_step_id IS NULL AND route_version IS NULL AND purpose IN (N'FORWARD', N'RECOVERY'))
            ),
            CONSTRAINT CK_alert_email_action_parent CHECK (
                (purpose IN (N'RECOVERY', N'FORWARD') AND parent_action_id IS NOT NULL AND parent_action_id <> action_id)
                OR (purpose IN (N'DETECTION', N'ESCALATION') AND parent_action_id IS NULL)
            ),
            CONSTRAINT CK_alert_email_action_request CHECK (
                purpose <> N'FORWARD'
                OR (requested_by IS NOT NULL AND LEN(requested_by) > 0 AND request_reason IS NOT NULL AND LEN(request_reason) > 0)
            ),
            CONSTRAINT CK_alert_email_action_idempotency CHECK (LEN(idempotency_key) > 0)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ix_alert_email_action_incident'
            AND object_id = OBJECT_ID(N'alerting.alert_email_action')
    )
    BEGIN
        CREATE INDEX ix_alert_email_action_incident
            ON alerting.alert_email_action (alert_id, recipient_id, purpose);
    END;
GO

CREATE OR ALTER TRIGGER alerting.tr_alert_email_action_guard
ON alerting.alert_email_action
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM deleted)
        THROW 51040, 'Email action snapshots are immutable.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS email_action
            INNER JOIN alerting.alert_signal_event AS signal_event
                ON email_action.source_signal_event_id = signal_event.event_id
            INNER JOIN alerting.dim_signal_type AS signal_type
                ON signal_event.signal_type_id = signal_type.signal_type_id
            WHERE
                signal_event.alert_id <> email_action.alert_id
                OR (email_action.purpose IN (N'DETECTION', N'ESCALATION') AND signal_type.code <> N'DETECTED')
                OR (email_action.purpose = N'RECOVERY' AND signal_type.code <> N'CLEARED')
        )
        THROW 51041, 'Email action signal does not match its incident or purpose.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS email_action
            INNER JOIN alerting.dim_email_recipient AS recipient
                ON email_action.recipient_id = recipient.recipient_id
            WHERE
                email_action.email_address <> recipient.email_address
                OR DATALENGTH(email_action.email_address) <> DATALENGTH(recipient.email_address)
                OR recipient.is_active = 0
        )
        THROW 51042, 'Email action requires an active recipient with the same address.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS email_action
            INNER JOIN alerting.alert_email_action AS parent_action
                ON email_action.parent_action_id = parent_action.action_id
            INNER JOIN alerting.alert_signal_event AS parent_signal
                ON parent_action.source_signal_event_id = parent_signal.event_id
            INNER JOIN alerting.dim_signal_type AS parent_signal_type
                ON parent_signal.signal_type_id = parent_signal_type.signal_type_id
            WHERE
                (email_action.purpose = N'FORWARD' AND email_action.source_signal_event_id <> parent_action.source_signal_event_id)
                OR (
                    email_action.purpose = N'RECOVERY'
                    AND (
                        parent_signal_type.code <> N'DETECTED'
                        OR email_action.recipient_id <> parent_action.recipient_id
                        OR email_action.email_address <> parent_action.email_address
                        OR EXISTS (
                            SELECT
                                email_action.email_route_id,
                                email_action.email_route_step_id,
                                email_action.route_version
                            EXCEPT
                            SELECT
                                parent_action.email_route_id,
                                parent_action.email_route_step_id,
                                parent_action.route_version
                        )
                    )
                )
        )
        THROW 51043, 'Email action parent does not match its recipient, signal or route snapshot.', 1;

    IF
        EXISTS (
            SELECT 1
            FROM inserted AS email_action
            INNER JOIN alerting.alert_email_route AS email_route
                ON email_action.email_route_id = email_route.email_route_id
            INNER JOIN alerting.alert_email_route_step AS route_step
                ON email_action.email_route_step_id = route_step.email_route_step_id
            INNER JOIN alerting.alert_incident AS incident
                ON email_action.alert_id = incident.alert_id
            WHERE
                email_action.purpose IN (N'DETECTION', N'ESCALATION')
                AND (
                    email_route.alert_type_id <> incident.alert_type_id
                    OR (email_route.severity_id IS NOT NULL AND (incident.severity_id IS NULL OR email_route.severity_id <> incident.severity_id))
                    OR (email_route.entity_key IS NOT NULL AND (incident.entity_key IS NULL OR email_route.entity_key <> incident.entity_key))
                    OR email_route.is_enabled = 0
                    OR route_step.is_enabled = 0
                    OR email_action.route_version <> email_route.version
                    OR (email_action.purpose = N'DETECTION' AND route_step.step_number <> 0)
                    OR (email_action.purpose = N'ESCALATION' AND route_step.step_number = 0)
                    OR NOT EXISTS (
                        SELECT 1
                        FROM alerting.alert_email_route_recipient AS assigned_recipient
                        WHERE
                            assigned_recipient.email_route_step_id = email_action.email_route_step_id
                            AND assigned_recipient.recipient_id = email_action.recipient_id
                    )
                )
        )
        THROW 51044, 'Email action requires a current enabled route and an assigned recipient matching its incident and stage.', 1;
END;
GO
