USE Students;
GO

IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'Students') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on database Students';
END
ELSE PRINT 'CDC already enabled on database Students';
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Students'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'Students',
        @role_name     = NULL,
        @supports_net_changes = 1;
    PRINT 'CDC enabled on dbo.Students';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Applicants'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'Applicants',
        @role_name     = NULL,
        @supports_net_changes = 1;
    PRINT 'CDC enabled on dbo.Applicants';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.CourseEnrollments'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'CourseEnrollments',
        @role_name     = NULL,
        @supports_net_changes = 1;
    PRINT 'CDC enabled on dbo.CourseEnrollments';
END
GO

SELECT name AS database_name, is_cdc_enabled FROM sys.databases WHERE name = 'Students';
SELECT capture_instance, source_schema, source_name, start_lsn FROM cdc.change_tables;
GO
