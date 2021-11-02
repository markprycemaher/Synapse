--- https://docs.microsoft.com/en-us/azure/stream-analytics/stream-analytics-quick-create-portal

-- setup streaming in ASA from storage accout to DB.
-- using the customer table from TPCH...
CREATE TABLE [dbo].[streaming_customer_copy_into]
(
	[c_custkey] [bigint]  NULL,
	[c_name] [varchar](25)  NULL,
	[c_address] [varchar](40)  NULL,
	[c_nationkey] [int]  NULL,
	[c_phone] [char](15)  NULL,
	[c_acctbal] [float]  NULL,
	[c_mktsegment] [char](10)  NULL,
	[c_comment] [varchar](117)  NULL,
	[EventProcessedUtcTime] [varchar](50) NULL,
	[PartitionId] [bigint] NULL,
	[BlobName] [varchar](255) NULL,
	[BlobLastModifiedUtcTime] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = round_robin,
	CLUSTERED COLUMNSTORE INDEX
)
GO


-- This code has nothing todo with streaming - but its a funky way
-- or creating files on storage.

/*
CREATE EXTERNAL FILE FORMAT [CSV2] WITH 
(FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (
FIELD_TERMINATOR = N',', STRING_DELIMITER = N'"',
USE_TYPE_DEFAULT = True, First_Row = 2 ))
GO

CREATE EXTERNAL FILE FORMAT parquet3  
WITH (  
    FORMAT_TYPE = PARQUET 
	, 
);     

create master key;

CREATE DATABASE SCOPED CREDENTIAL sak
WITH IDENTITY = 'sak'
     , SECRET = 'shhhhh.....' 


CREATE DATABASE SCOPED CREDENTIAL demo2
WITH IDENTITY = 'demo2'
     , SECRET = 'Shh!' 

CREATE EXTERNAL DATA SOURCE [AzureDataLakeStore] WITH 
(LOCATION = N'wasbs://streaming@.....blob.core.windows.net',
CREDENTIAL = [sak])
GO
drop EXTERNAL TABLE cust125_test
CREATE EXTERNAL TABLE cust125_test
WITH (
    LOCATION = 'cluster1/logs/2021-10-30',
    DATA_SOURCE = [AzureDataLakeStore],  
    FILE_FORMAT = [csv]
)  
AS
SELECT top 10000 * from [dbo].[customer]
print getdate()
*/
-- CUSTOMER, dwu1000 (2 nodes smallrc)
-- 54 seconds to export 150,000,000 rows 
-- 27 GB of data...

--select count_big(*)  FROM  [dbo].[streaming_customer_test];

-- 59736
-- 92441
-- 153442
/*
delete FROM [dbo].[streaming_customer_bulk]
delete FROM  [dbo].[streaming_customer_test]
*/
/*
select count(*)  FROM  [dbo].[streaming_customer_test]
select count(*)  FROM  [dbo].[streaming_customer_bulk]

select * from sys.dm_pdw_exec_requests order by submit_time desc;
*/
/*
--drop table [dbo].[streaming_customer_copy_into]
CREATE TABLE [dbo].[streaming_customer_copy_into]
(
	[c_custkey] [bigint]  NULL,
	[c_name] [varchar](25)  NULL,
	[c_address] [varchar](40)  NULL,
	[c_nationkey] [int]  NULL,
	[c_phone] [char](15)  NULL,
	[c_acctbal] [float]  NULL,
	[c_mktsegment] [char](10)  NULL,
	[c_comment] [varchar](117)  NULL,
	[EventProcessedUtcTime] [varchar](50) NULL,
	[PartitionId] [bigint] NULL,
	[BlobName] [varchar](255) NULL,
	[BlobLastModifiedUtcTime] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = round_robin,
	CLUSTERED COLUMNSTORE INDEX
)
GO
drop TABLE [dbo].[streaming_customer_test]
CREATE TABLE [dbo].[streaming_customer_test]
(
	[c_custkey] [varchar](255)  NULL,
	[c_name] [varchar](255)  NULL,
	[c_address] [varchar](255)  NULL,
	[c_nationkey] [varchar](255)  NULL,
	[c_phone] [varchar](255)  NULL,
	[c_acctbal] [varchar](255)  NULL,
	[c_mktsegment] [varchar](255)  NULL,
	[c_comment] [varchar](255)  NULL,
	[EventProcessedUtcTime] [varchar](50) NULL,
	[PartitionId] [bigint] NULL,
	[BlobName] [varchar](255) NULL,
	[BlobLastModifiedUtcTime] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = round_robin,
	CLUSTERED COLUMNSTORE INDEX
)
GO
*/

