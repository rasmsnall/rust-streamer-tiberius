-- Every type crate::types maps, each one nullable, so the live tests exercise both the
-- value path and the NULL path against a real SQL Server rather than against assumptions
-- about how TDS encodes them. Row 1 carries values, row 2 carries NULLs.
USE tiberiusdelta_test;
GO
IF OBJECT_ID('dbo.type_zoo') IS NOT NULL DROP TABLE dbo.type_zoo;
CREATE TABLE dbo.type_zoo (
    id INT PRIMARY KEY,
    c_tinyint TINYINT NULL,
    c_smallint SMALLINT NULL,
    c_bigint BIGINT NULL,
    c_bit BIT NULL,
    c_real REAL NULL,
    c_float FLOAT NULL,
    c_money MONEY NULL,
    c_decimal DECIMAL(18,4) NULL,
    c_numeric NUMERIC(5,0) NULL,
    c_date DATE NULL,
    c_time TIME(7) NULL,
    c_datetime DATETIME NULL,
    c_datetime2 DATETIME2(7) NULL,
    c_smalldatetime SMALLDATETIME NULL,
    c_datetimeoffset DATETIMEOFFSET(7) NULL,
    c_varchar VARCHAR(50) NULL,
    c_nvarchar NVARCHAR(50) NULL,
    c_varbinary VARBINARY(16) NULL,
    c_uniqueidentifier UNIQUEIDENTIFIER NULL,
    c_xml XML NULL,
    updated_at DATETIME2 NOT NULL
);
GO
INSERT INTO dbo.type_zoo VALUES
    (1, 255, -32768, 9223372036854775807, 1, 1.5, 2.25, 12.3400, 12345.6789, 42,
     '2026-03-04', '13:45:30.1234567', '2026-03-04T13:45:30',
     '2026-03-04T13:45:30.1234567', '2026-03-04T13:45:00',
     '2026-03-04T13:45:30.1234567+02:00', 'ascii', N'unicode',
     0x0FA0, '6F9619FF-8B86-D011-B42D-00C04FC964FF', N'<a b="c"/>',
     '2026-01-01T00:00:00'),
    (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     '2026-01-02T00:00:00');
GO
