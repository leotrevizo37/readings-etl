IF COL_LENGTH(N'dwh.factReadingsMeasurement', N'Sales') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD Sales INT NULL;
    END;
GO
