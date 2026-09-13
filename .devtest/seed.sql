IF DB_ID('tiberiusdelta_test') IS NULL CREATE DATABASE tiberiusdelta_test;
GO
USE tiberiusdelta_test;
GO
IF OBJECT_ID('dbo.customers') IS NOT NULL DROP TABLE dbo.customers;
CREATE TABLE dbo.customers (
    id INT PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    balance DECIMAL(10,2) NOT NULL,
    is_active BIT NOT NULL,
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
INSERT INTO dbo.customers (id, name, balance, is_active, updated_at) VALUES
    (1, N'Alice', 123.45, 1, '2026-01-01T10:00:00'),
    (2, N'Bob', 0.00, 0, '2026-01-02T11:00:00'),
    (3, N'Carol', 9999.99, 1, '2026-01-03T12:00:00');
GO
