SET QUOTED_IDENTIFIER ON;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'alerting'
    )
    BEGIN
        EXEC ('CREATE SCHEMA alerting');
    END;
GO

IF
    OBJECT_ID(N'alerting.alert_workflow_state', N'U') IS NULL
    AND OBJECT_ID(N'alerting.alert_workflow_state_legacy', N'U') IS NULL
    AND OBJECT_ID(N'alerting.alert_incident', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.alert_workflow_state (
            alert_key NVARCHAR(450) NOT NULL
            CONSTRAINT PK_alert_workflow_state PRIMARY KEY,
            trigger_source NVARCHAR(100) NOT NULL,
            alert_type NVARCHAR(50) NOT NULL,
            entity_id NVARCHAR(250) NOT NULL,
            severity NVARCHAR(20) NULL,
            state NVARCHAR(30) NOT NULL
            CONSTRAINT DF_alert_workflow_state_state DEFAULT (N'OPEN'),
            notification_type NVARCHAR(20) NOT NULL
            CONSTRAINT DF_alert_workflow_state_notification_type
            DEFAULT (N'OPEN'),
            notification_status NVARCHAR(30) NOT NULL
            CONSTRAINT DF_alert_workflow_state_notification_status
            DEFAULT (N'PENDING'),
            details NVARCHAR(MAX) NULL,
            metadata_json NVARCHAR(MAX) NOT NULL
            CONSTRAINT DF_alert_workflow_state_metadata_json DEFAULT (N'{}'),
            ticket_id NVARCHAR(250) NULL,
            ticket_created_at DATETIME2(3) NULL,
            first_detected_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_workflow_state_first_detected_at
            DEFAULT (SYSUTCDATETIME()),
            last_detected_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_workflow_state_last_detected_at
            DEFAULT (SYSUTCDATETIME()),
            notification_queued_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_workflow_state_notification_queued_at
            DEFAULT (SYSUTCDATETIME()),
            last_notification_attempt_at DATETIME2(3) NULL,
            last_notified_at DATETIME2(3) NULL,
            next_retry_at DATETIME2(3) NULL,
            resolved_at DATETIME2(3) NULL,
            notification_count INT NOT NULL
            CONSTRAINT DF_alert_workflow_state_notification_count
            DEFAULT (0),
            notification_attempt_count INT NOT NULL
            CONSTRAINT DF_alert_workflow_state_attempt_count DEFAULT (0),
            max_notification_attempts SMALLINT NOT NULL
            CONSTRAINT DF_alert_workflow_state_max_attempts DEFAULT (3),
            last_error_code NVARCHAR(100) NULL,
            last_error_message NVARCHAR(MAX) NULL,
            last_error_at DATETIME2(3) NULL,
            created_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_workflow_state_created_at
            DEFAULT (SYSUTCDATETIME()),
            updated_at DATETIME2(3) NOT NULL
            CONSTRAINT DF_alert_workflow_state_updated_at
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT CK_alert_workflow_state_metadata_json
            CHECK (ISJSON(metadata_json) = 1),
            CONSTRAINT CK_alert_workflow_state_state
            CHECK (state IN (N'OPEN', N'RESOLVED_PENDING', N'RESOLVED')),
            CONSTRAINT CK_alert_workflow_state_notification_type
            CHECK (notification_type IN (N'OPEN', N'REMINDER', N'RESOLUTION')),
            CONSTRAINT CK_alert_workflow_state_notification_status
            CHECK (notification_status IN (
                N'PENDING', N'PROCESSING', N'SENT', N'RETRY_PENDING',
                N'FAILED', N'CANCELLED'
            )),
            CONSTRAINT CK_alert_workflow_state_notification_count
            CHECK (notification_count >= 0),
            CONSTRAINT CK_alert_workflow_state_attempts
            CHECK (
                notification_attempt_count >= 0
                AND max_notification_attempts > 0
            )
        );
    END;
GO

IF
    OBJECT_ID(N'alerting.alert_workflow_state', N'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ix_alert_workflow_state_active'
            AND object_id = OBJECT_ID(N'alerting.alert_workflow_state')
    )
    BEGIN
        CREATE INDEX ix_alert_workflow_state_active
            ON alerting.alert_workflow_state (state)
            WHERE state IN ('OPEN', 'RESOLVED_PENDING');
    END;
GO

IF
    OBJECT_ID(N'alerting.alert_workflow_state', N'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ux_alert_workflow_state_active_condition'
            AND object_id = OBJECT_ID(N'alerting.alert_workflow_state')
    )
    BEGIN
        CREATE UNIQUE INDEX ux_alert_workflow_state_active_condition
            ON alerting.alert_workflow_state
            (trigger_source, alert_type, entity_id)
            WHERE state = 'OPEN';
    END;
GO

IF
    OBJECT_ID(N'alerting.alert_workflow_state', N'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            name = N'ix_alert_workflow_state_notification_ready'
            AND object_id = OBJECT_ID(N'alerting.alert_workflow_state')
    )
    BEGIN
        CREATE INDEX ix_alert_workflow_state_notification_ready
            ON alerting.alert_workflow_state
            (notification_status, next_retry_at, notification_queued_at)
            WHERE state IN ('OPEN', 'RESOLVED_PENDING');
    END;
GO
