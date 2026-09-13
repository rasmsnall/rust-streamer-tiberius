USE tiberiusdelta_test;
GO
ALTER TABLE dbo.customers ADD notes NVARCHAR(50) NULL;
GO
INSERT INTO dbo.customers (id, name, balance, is_active, updated_at, notes) VALUES
    (4, N'Dave', 5.00, 1, '2026-01-04T00:00:00', NULL),
    (5, N'Eve', 6.00, 1, '2026-01-05T00:00:00', N'');
GO
