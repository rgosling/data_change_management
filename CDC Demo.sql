--============================================================================
--Change Data Capture Example
-- 
--From: https://www.mssqltips.com/sqlservertip/1474/using-change-data-capture-cdc-in-sql-server-2008/
--============================================================================

USE MASTER
GO
DROP DATABASE IF EXISTS DemoCDC
GO
CREATE DATABASE DemoCDC
GO

-- Enable CDC
USE DemoCDC
GO
--
DECLARE @rc INT

EXEC @rc = sys.sp_cdc_enable_db

SELECT @rc

-- new column added to sys.databases: is_cdc_enabled
SELECT name
	,is_cdc_enabled
FROM sys.databases

CREATE TABLE dbo.customer (
	id INT identity NOT NULL
	,name VARCHAR(50) NOT NULL
	,STATE VARCHAR(2) NOT NULL
	,CONSTRAINT pk_customer PRIMARY KEY CLUSTERED (id)
	)

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo'
	,@source_name = 'customer'
	,@role_name = 'CDCRole'
	,@supports_net_changes = 1

SELECT name
	,type
	,type_desc
	,is_tracked_by_cdc
FROM sys.tables

SELECT o.name
	,o.type
	,o.type_desc
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE s.name = 'cdc'

----
--Demo
INSERT customer
VALUES (
	'abc company'
	,'md'
	)

INSERT customer
VALUES (
	'xyz company'
	,'de'
	)

INSERT customer
VALUES (
	'xox company'
	,'va'
	)

UPDATE customer
SET STATE = 'pa'
WHERE id = 1

DELETE
FROM customer
WHERE id = 3

--
DECLARE @begin_lsn BINARY (10)
	,@end_lsn BINARY (10)

-- get the first LSN for customer changes
SELECT @begin_lsn = sys.fn_cdc_get_min_lsn('dbo_customer')

-- get the last LSN for customer changes
SELECT @end_lsn = sys.fn_cdc_get_max_lsn()

-- get net changes; group changes in the range by the pk
SELECT *
FROM cdc.fn_cdc_get_net_changes_dbo_customer(@begin_lsn, @end_lsn, 'all');

-- get individual changes in the range
SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_customer(@begin_lsn, @end_lsn, 'all');

--Let's Extend this to the LSN
CREATE TABLE dbo.customer_lsn (last_lsn BINARY (10))

CREATE FUNCTION dbo.get_last_customer_lsn ()
RETURNS BINARY (10)
AS
BEGIN
	DECLARE @last_lsn BINARY (10)

	SELECT @last_lsn = last_lsn
	FROM dbo.customer_lsn

	SELECT @last_lsn = isnull(@last_lsn, sys.fn_cdc_get_min_lsn('dbo_customer'))

	RETURN @last_lsn
END

DECLARE @begin_lsn BINARY (10)
	,@end_lsn BINARY (10)

-- get the next LSN for customer changes
SELECT @begin_lsn = dbo.get_last_customer_lsn()

-- get the last LSN for customer changes
SELECT @end_lsn = sys.fn_cdc_get_max_lsn()

-- get the net changes; group all changes in the range by the pk
SELECT *
FROM cdc.fn_cdc_get_net_changes_dbo_customer(@begin_lsn, @end_lsn, 'all');

-- get all individual changes in the range
SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_customer(@begin_lsn, @end_lsn, 'all');

-- save the end_lsn in the customer_lsn table
UPDATE dbo.customer_lsn
SET last_lsn = @end_lsn

IF @@ROWCOUNT = 0
	INSERT INTO dbo.customer_lsn
	VALUES (@end_lsn)