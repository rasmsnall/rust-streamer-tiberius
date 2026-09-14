-- Baseline fixture for the live end-to-end tests (tests/firebird_live.rs) and for
-- tools/seed.py. Firebird has no CREATE DATABASE IF NOT EXISTS; the container this is
-- meant to run against (see CLAUDE.md's Environment notes) is created fresh each time,
-- so this file only builds objects inside it.
--
-- CUSTOMERS is the plain incremental-sync case: an autoincrement-ish INTEGER id and a
-- TIMESTAMP watermark that is never tied within this fixture (each row's UPDATED_AT is
-- a full second apart), so ordinary sync semantics apply without the tie caveat
-- CLAUDE.md's "What incremental sync does not do" section warns about.

CREATE TABLE CUSTOMERS (
    ID INTEGER NOT NULL PRIMARY KEY,
    NAME VARCHAR(100) NOT NULL,
    UPDATED_AT TIMESTAMP NOT NULL
);

INSERT INTO CUSTOMERS (ID, NAME, UPDATED_AT) VALUES (1, 'Alice',   '2026-01-01 10:00:00');
INSERT INTO CUSTOMERS (ID, NAME, UPDATED_AT) VALUES (2, 'Bob',     '2026-01-01 10:00:01');
INSERT INTO CUSTOMERS (ID, NAME, UPDATED_AT) VALUES (3, 'Carol',   '2026-01-01 10:00:02');
INSERT INTO CUSTOMERS (ID, NAME, UPDATED_AT) VALUES (4, 'Dave',    '2026-01-01 10:00:03');
INSERT INTO CUSTOMERS (ID, NAME, UPDATED_AT) VALUES (5, 'Eve',     '2026-01-01 10:00:04');
COMMIT;
