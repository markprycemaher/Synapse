-- Simple example of using Row Level Security over a file (external table)

-- create demo table, [col3] will be masked and col4 will have random numbers
CREATE EXTERNAL TABLE [dbo].[test456]
(
	[col1] varchar(50) NULL,
	[col2] varchar(50) NULL,
	[col3] varchar(50) MASKED WITH (FUNCTION = 'default()') NULL,
	[col4] int MASKED WITH (FUNCTION = 'random(1, 100)') NULL
)
WITH (DATA_SOURCE = [external_data_source],
LOCATION = N'sample/', -- folder in storage
FILE_FORMAT = csv, -- external file format
REJECT_TYPE = VALUE,REJECT_VALUE = 99)
GO

-- you will see the raw data
select * from [dbo].[test456];

-- creating user that will see masked data (admins will see all the data)
CREATE USER MaskingTestUser WITHOUT LOGIN; 

-- give user access to the tables
GRANT SELECT ON SCHEMA::dbo TO MaskingTestUser;  
  -- impersonate for testing:
EXECUTE AS USER = 'MaskingTestUser';  

-- some of the data is masked
select * from [dbo].[test456]; 

-- go back to orginal user
REVERT;  
