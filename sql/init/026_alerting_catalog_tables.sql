SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;
GO

IF
    NOT EXISTS (
        SELECT 1 FROM sys.schemas
        WHERE name = N'alerting'
    )
    BEGIN
        EXEC ('CREATE SCHEMA alerting');
    END;
GO

IF OBJECT_ID(N'alerting.alert_workflow_state', N'U') IS NOT NULL
    BEGIN
        IF OBJECT_ID(N'alerting.alert_workflow_state_legacy', N'U') IS NOT NULL
            THROW 51000, 'alert_workflow_state_legacy already exists.', 1;

        EXEC sys.sp_rename
            N'alerting.alert_workflow_state',
            N'alert_workflow_state_legacy';
    END;
GO

IF
    OBJECT_ID(N'alerting.alert_config', N'U') IS NOT NULL
    AND COL_LENGTH(N'alerting.alert_config', N'alert_config_id') IS NULL
    BEGIN
        IF OBJECT_ID(N'alerting.alert_config_legacy', N'U') IS NOT NULL
            THROW 51001, 'alert_config_legacy already exists.', 1;

        EXEC sys.sp_rename N'alerting.alert_config', N'alert_config_legacy';
    END;
GO

IF
    OBJECT_ID(N'alerting.alert_rule', N'U') IS NOT NULL
    AND COL_LENGTH(N'alerting.alert_rule', N'alert_rule_id') IS NULL
    BEGIN
        IF OBJECT_ID(N'alerting.alert_rule_legacy', N'U') IS NOT NULL
            THROW 51002, 'alert_rule_legacy already exists.', 1;

        EXEC sys.sp_rename N'alerting.alert_rule', N'alert_rule_legacy';
    END;
GO

IF OBJECT_ID(N'alerting.dim_alert_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_alert_type (
            alert_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_alert_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_continuous BIT NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_alert_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_signal_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_signal_type (
            signal_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_signal_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_signal_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_severity', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_severity (
            severity_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_severity PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_severity_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_trigger_source', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_trigger_source (
            trigger_source_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_trigger_source PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_trigger_source_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_config_status', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_config_status (
            config_status_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_config_status PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_config_status_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_workflow_status', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_workflow_status (
            workflow_status_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_workflow_status PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_workflow_status_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_closure_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_closure_type (
            closure_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_closure_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_closure_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_action_type', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_action_type (
            action_type_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_action_type PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_action_type_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_action_operation', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_action_operation (
            action_operation_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_action_operation PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_action_operation_code UNIQUE (code)
        );
    END;
GO

IF OBJECT_ID(N'alerting.dim_action_status', N'U') IS NULL
    BEGIN
        CREATE TABLE alerting.dim_action_status (
            action_status_id UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_dim_action_status PRIMARY KEY,
            code NVARCHAR(50) COLLATE Latin1_General_100_BIN2 NOT NULL,
            name_es NVARCHAR(100) NOT NULL,
            description_es NVARCHAR(500) NOT NULL,
            is_active BIT NOT NULL,
            sort_order SMALLINT NOT NULL,
            CONSTRAINT UQ_dim_action_status_code UNIQUE (code)
        );
    END;
GO
