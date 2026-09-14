-- Every type crate::types maps, each one nullable, so the live tests exercise both the
-- value path and the NULL path against a real Firebird rather than against assumptions
-- about how the wire protocol and rsfbclient encode them. Row 1 carries values, row 2
-- carries NULLs. Mirrors tiberiusdelta's own .devtest/type_zoo.sql, which earned its
-- keep immediately by catching two real defects on its first live run; this file has
-- not yet had that chance (see CLAUDE.md's Open items: no live Firebird instance was
-- available in the session that wrote it), so treat the exact literal syntax below,
-- particularly the WITH TIME ZONE literals, as unverified until it has run once.
--
-- Requires Firebird 4.0 or later: INT128, DECFLOAT, and the WITH TIME ZONE types are
-- Firebird 4 additions and do not exist on Firebird 3.
--
-- Deliberately absent: an ARRAY column. crate::types::resolve does not special-case
-- Firebird's ARRAY feature (detected via RDB$RELATION_FIELDS.RDB$DIMENSIONS, not by
-- RDB$FIELD_TYPE at all), so this crate's behaviour against one is genuinely unknown;
-- see CLAUDE.md's Open items rather than assuming this fixture would catch it.

CREATE TABLE TYPE_ZOO (
    ID INTEGER NOT NULL PRIMARY KEY,
    C_SMALLINT SMALLINT,
    C_INTEGER INTEGER,
    C_BIGINT BIGINT,
    C_NUMERIC NUMERIC(18,4),
    C_DECIMAL DECIMAL(9,2),
    C_FLOAT FLOAT,
    C_DOUBLE DOUBLE PRECISION,
    C_BOOLEAN BOOLEAN,
    C_DATE DATE,
    C_TIME TIME,
    C_TIMESTAMP TIMESTAMP,
    C_VARCHAR VARCHAR(50),
    C_CHAR CHAR(10),
    C_BLOB_TEXT BLOB SUB_TYPE TEXT,
    C_BLOB_BINARY BLOB SUB_TYPE BINARY,
    C_INT128 INT128,
    C_DECFLOAT16 DECFLOAT(16),
    C_DECFLOAT34 DECFLOAT(34),
    C_TIME_TZ TIME WITH TIME ZONE,
    C_TIMESTAMP_TZ TIMESTAMP WITH TIME ZONE,
    UPDATED_AT TIMESTAMP NOT NULL
);
COMMIT;

INSERT INTO TYPE_ZOO VALUES (
    1,
    -32768,
    2147483647,
    9223372036854775807,
    12345.6789,
    1234567.89,
    1.5,
    2.25,
    TRUE,
    '2026-03-04',
    '13:45:30.1234',
    '2026-03-04 13:45:30',
    'ascii',
    'char10',
    'hello blob text',
    x'0FA0',
    123456789012345678901234567890,
    1234567890123.456,
    123456789012345678901234.56789012,
    TIME '13:45:30+02:00',
    TIMESTAMP '2026-03-04 13:45:30+02:00',
    '2026-01-01 00:00:00'
);
INSERT INTO TYPE_ZOO (ID, UPDATED_AT) VALUES (2, '2026-01-02 00:00:00');
COMMIT;
