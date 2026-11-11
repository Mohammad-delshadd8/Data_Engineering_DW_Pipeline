-- ============================================================
--  Per-consumer watermark table
--  (allows SCD and Facts to consume the same CDC table independently)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.cdc_watermark (
    consumer    TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    last_lsn    TEXT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, table_name)
);

INSERT INTO etl.cdc_watermark (consumer, table_name, last_lsn) VALUES
    ('facts', 'CourseEnrollments', '0x00000000000000000000'),
    ('facts', 'Applicants',        '0x00000000000000000000'),
    ('scd',   'Students',          '0x00000000000000000000'),
    ('scd',   'Applicants',        '0x00000000000000000000')
ON CONFLICT (consumer, table_name) DO NOTHING;
