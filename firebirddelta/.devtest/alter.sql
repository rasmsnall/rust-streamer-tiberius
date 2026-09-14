-- Simulates a schema that grew a column between two runs, and adds two more customers
-- so the live incremental-resync test has genuinely new rows above the first sync's
-- checkpoint. Run after seed.sql, in its own statement batch (see tools/seed.py):
-- Firebird requires DDL committed before it can be referenced by DML in the same
-- connection.

ALTER TABLE CUSTOMERS ADD NOTES VARCHAR(50);
COMMIT;

INSERT INTO CUSTOMERS (ID, NAME, UPDATED_AT, NOTES) VALUES (6, 'Frank', '2026-01-01 10:00:05', NULL);
INSERT INTO CUSTOMERS (ID, NAME, UPDATED_AT, NOTES) VALUES (7, 'Grace', '2026-01-01 10:00:06', '');
COMMIT;
