CREATE DATABASE IF NOT EXISTS StudentsDW;

-- ============================================================
--  LOOKUP DIMENSIONS (SCD Type 1 — ReplacingMergeTree)
-- ============================================================

CREATE TABLE IF NOT EXISTS StudentsDW.DimDepartment
(
    DepartmentKey  UInt64,
    DepartmentCode String,
    DepartmentName String,
    LoadDateTime   DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY DepartmentKey;

CREATE TABLE IF NOT EXISTS StudentsDW.DimCourse
(
    CourseKey          UInt64,
    DepartmentCode     String,
    DepartmentName     String,
    CourseNumber       Int32,
    CourseTitle        String,
    CourseDescription  String,
    Credits            Decimal(3,1),
    MaximumSectionSize Nullable(Int32),
    LoadDateTime       DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY CourseKey;

CREATE TABLE IF NOT EXISTS StudentsDW.DimTerm
(
    TermKey      UInt64,
    TermCode     String,
    SessionCode  String,
    SessionName  String,
    AcademicYear Int32,
    LoadDateTime DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY TermKey;

CREATE TABLE IF NOT EXISTS StudentsDW.DimGrade
(
    GradeKey     UInt64,
    GradeCode    String,
    GradePoints  Nullable(Decimal(2,1)),
    IncludeInGpa Bool,
    LoadDateTime DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY GradeKey;

CREATE TABLE IF NOT EXISTS StudentsDW.DimClassStanding
(
    ClassStandingKey  UInt64,
    ClassStandingCode String,
    ClassStandingName String,
    DegreeLevelCode   String,
    RequiredCredits   Int32,
    LoadDateTime      DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY ClassStandingKey;

CREATE TABLE IF NOT EXISTS StudentsDW.DimDegree
(
    DegreeKey       UInt64,
    DegreeId        Int32,
    DegreeName      String,
    DegreeTypeCode  String,
    DegreeTypeName  String,
    DegreeLevelCode String,
    DegreeLevelName String,
    DepartmentCode  String,
    DepartmentName  String,
    LoadDateTime    DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY DegreeKey;

CREATE TABLE IF NOT EXISTS StudentsDW.DimDate
(
    DateKey       Int32,
    FullDate      Date,
    DayNumber     UInt8,
    DayName       String,
    DayOfWeek     UInt8,
    WeekOfYear    UInt8,
    MonthNumber   UInt8,
    MonthName     String,
    QuarterNumber UInt8,
    QuarterName   String,
    YearNumber    Int16,
    IsWeekend     Bool,
    LoadDateTime  DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY DateKey;

-- ============================================================
--  SCD TYPE 2 DIMENSIONS (MergeTree + mutation)
-- ============================================================

CREATE TABLE IF NOT EXISTS StudentsDW.DimStudent
(
    StudentKey         UInt64,
    StudentId          Int32,
    FirstName          String,
    MiddleName         String,
    LastName           String,
    Gender             String,
    StreetAddress      String,
    City               String,
    State              String,
    ZipCode            String,
    Telephone          String,
    Email              String,
    DateOfBirth        Date,
    DegreeId           Int32,
    DegreeName         String,
    DegreeTypeCode     String,
    DegreeTypeName     String,
    DegreeLevelCode    String,
    DegreeLevelName    String,
    DepartmentCode     String,
    DepartmentName     String,
    ClassStandingCode  Nullable(String),
    ClassStandingName  Nullable(String),
    StudentStatus      String,
    EnrollmentTermCode String,
    EnrollmentYear     Nullable(Int32),
    EffectiveStartDate Date,
    EffectiveEndDate   Date,
    IsCurrent          Bool,
    RowHash            String,
    LoadDateTime       DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = MergeTree
ORDER BY (StudentId, EffectiveStartDate, LoadDateTime);

CREATE TABLE IF NOT EXISTS StudentsDW.DimApplicant
(
    ApplicantKey        UInt64,
    ApplicantId         Int32,
    FirstName           String,
    MiddleName          String,
    LastName            String,
    Gender              String,
    City                String,
    State               String,
    ZipCode             String,
    Telephone           String,
    Email               String,
    DateOfBirth         Date,
    ApplicationTypeCode String,
    EffectiveStartDate  Date,
    EffectiveEndDate    Date,
    IsCurrent           Bool,
    RowHash             String,
    LoadDateTime        DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = MergeTree
ORDER BY (ApplicantId, EffectiveStartDate, LoadDateTime);

-- ============================================================
--  FACT TABLES (ReplacingMergeTree = Type 1 upsert)
-- ============================================================

CREATE TABLE IF NOT EXISTS StudentsDW.FactApplication
(
    ApplicationFactKey         UInt64,
    ApplicationId              Int32,
    ApplicantKey               UInt64,
    ApplicationReceivedDateKey Int32,
    EntryTermStartDateKey      Int32,
    EntryTermKey               UInt64,
    ApplicationStatusCode      String,
    ApplicationTypeCode        String,
    GPA                        Nullable(Decimal(4,2)),
    SATMathScore               Nullable(Int32),
    SATReadingScore            Nullable(Int32),
    SATWritingScore            Nullable(Int32),
    EssayScore                 Nullable(Int32),
    AdmissionPoints            Nullable(Decimal(18,4)),
    ApplicationCount           Int32,
    LoadDateTime               DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY ApplicationId;

CREATE TABLE IF NOT EXISTS StudentsDW.FactEnrollment
(
    EnrollmentFactKey    UInt64,
    CourseEnrollmentId   Int32,
    StudentKey           UInt64,
    CourseKey            UInt64,
    DepartmentKey        UInt64,
    TermKey              UInt64,
    GradeKey             UInt64,
    DegreeKey            UInt64,
    ClassStandingKey     UInt64,
    EnrollmentDateKey    Int32,
    GradeAssignedDateKey Int32,
    EnrollmentCount      Int32,
    EarnedCredits        Nullable(Decimal(5,2)),
    GradePointsEarned    Nullable(Decimal(8,4)),
    LoadDateTime         DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY CourseEnrollmentId;

CREATE TABLE IF NOT EXISTS StudentsDW.FactCourseOffering
(
    CourseOfferingFactKey UInt64,
    CourseOfferingId      Int32,
    CourseKey             UInt64,
    DepartmentKey         UInt64,
    TermKey               UInt64,
    SectionNumber         Int32,
    Capacity              Int32,
    OfferingCount         Int32,
    LoadDateTime          DateTime64(0, 'UTC') DEFAULT now64(0)
) ENGINE = ReplacingMergeTree(LoadDateTime) ORDER BY CourseOfferingId;
