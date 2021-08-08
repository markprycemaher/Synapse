-- I created a seperate loading user and gave it 100% of the system for faster loading. Details are here: https://docs.microsoft.com/en-us/sql/t-sql/statements/execute-as-transact-sql?view=sql-server-ver15

--EXECUTE AS USER = 'loader';  

/*
--clean tables if they already exist 
truncate table dbo.customer
truncate table [dbo].[lineitem]
truncate table [dbo].[nation]
truncate table [dbo].[orders]
truncate table [dbo].[part]
truncate table [dbo].[partsupp]
truncate table [dbo].[region]
truncate table [dbo].[supplier]
*/

COPY INTO [customer] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/customer/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|'
)
go

COPY INTO [lineitem] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/lineitem/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|')
go


COPY INTO [nation] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/nation/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|')
go


COPY INTO [orders] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/orders/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|')
go

COPY INTO [part] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/part/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|')
go

COPY INTO [partsupp] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/partsupp/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|')
go

COPY INTO [region] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/region/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|')
go

COPY INTO [supplier] 
FROM 'https://publictpch.blob.core.windows.net/public/1gb_tsv/supplier/*'
WITH (
FILE_TYPE = 'Csv',
CREDENTIAL=(IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = ' '),
FIELDQUOTE = '',
FIELDTERMINATOR ='|')
go

select 'load completed', getdate()
go


select count_big(*) from customer
Go
select count_big(*) from lineitem
Go
select count_big(*) from nation
Go
select count_big(*) from orders
Go
select count_big(*) from part
Go
select count_big(*) from partsupp
Go
select count_big(*) from region
Go
select count_big(*) from supplier
Go


