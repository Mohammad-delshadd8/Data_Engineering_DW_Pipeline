USE Students;
GO

-- ============================================================
-- 1. Enable CDC at the Database Level
-- ============================================================
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'Students') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT '✅ CDC enabled on database Students';
END
ELSE 
    PRINT 'ℹ️ CDC already enabled on database Students';
GO

-- ============================================================
-- 2. Enable CDC on Core SCD Tables (Type 2 Dimensions)
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Students'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Students', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Students';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Applicants'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Applicants', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Applicants';
END
GO

-- ============================================================
-- 3. Enable CDC on Fact Tables (CDC Pipeline)
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.CourseEnrollments'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'CourseEnrollments', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.CourseEnrollments';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.CourseOfferings'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'CourseOfferings', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.CourseOfferings';
END
GO

-- ============================================================
-- 4. Enable CDC on Lookup Dimensions (Type 1 Dimensions)
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Departments'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Departments', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Departments';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Courses'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Courses', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Courses';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Terms'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Terms', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Terms';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Sessions'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Sessions', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Sessions';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Degrees'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Degrees', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Degrees';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.DegreeTypes'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'DegreeTypes', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.DegreeTypes';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.DegreeLevels'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'DegreeLevels', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.DegreeLevels';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.ClassStandings'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ClassStandings', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.ClassStandings';
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.Grades'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Grades', @role_name = NULL, @supports_net_changes = 1;
    PRINT '✅ CDC enabled on dbo.Grades';
END
GO

-- ============================================================
-- 5. Correct Verification Query
-- ============================================================
PRINT '--- Active CDC Tables ---';
SELECT 
    ct.capture_instance, 
    s.name AS source_schema, 
    o.name AS source_name
FROM cdc.change_tables ct
JOIN sys.objects o ON ct.source_object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
ORDER BY o.name;
GO
