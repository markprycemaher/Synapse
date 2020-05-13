
if exists(select * from sys.procedures p 
inner join sys.schemas s on p.schema_id = s.schema_id
where p.name like 'poly_into' and s.name = 'dbo')
BEGIN
DROP PROC [dbo].[poly_into] 
END

go

CREATE PROC [dbo].[poly_into] 
@tbname [varchar](200),  -- Table we wish to load into
@schema [varchar](200),  -- schema we wish to load into
@storageacc [varchar](200), -- External Data Source name ( example given below)   https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-data-source-transact-sql?view=azure-sqldw-latest
@datafolder [varchar](400), -- Path to the folders or file we want to import 
@fileformat [varchar](400), -- https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=azure-sqldw-latest
@loaddata [int], -- Create External table / import the data (todo: just create external table)
@truncate [int],  -- Truncate the table we want to load into 
@strictDataTypes [int],-- strict data types -- use data types from table or just use varchar(4000)
@removeExternalTable [int], -- Clean up remove the external table after the sucessful execution
@externalTableSchema [varchar](200) --  schema where the external table will go
AS
BEGIN
    SET NOCOUNT ON
/*

This procedure is designed to make Polybase a bit easier to use.

This proc creates an external table based on the shape of the table we want to import into.
The 

Usage:
exec [dbo].[poly_into] 'tablename','schema','MyAzureStorage','/path/path/file.csv','FormatCSV',1,0

@tbname [varchar](200),  -- Table we wish to load into , i.e. 'sales'
@schema [varchar](200),  -- schema we wish to load into, i.e. 'dbo'

@storageacc [varchar](200) External Data Source name i.e. 'MyAzureStorage'
--https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-data-source-transact-sql?view=azure-sqldw-latest
--CREATE EXTERNAL DATA SOURCE [MyAzureStorage] WITH (TYPE = HADOOP, LOCATION = N'wasbs://container@abc.blob.core.windows.net/', CREDENTIAL = [AzureStorageCredential])

@datafolder [varchar](400), -- Path to the folders or file we want to import, i.e. '/folder/folder/file.csv' or '/folder/' 

@fileformat [varchar](400),
https://docs.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=azure-sqldw-latest

@loaddata [int], -- Create External table / import the data (todo: just create external table)

@truncate [int]  -- Truncate the table we want to load into 

*/



declare @folderpath [varchar](700)
declare @procname varchar(200)
declare @rcount bigint;
set @procname = 'poly_into'

declare @Rundate datetime2
set @Rundate = getdate()

    -- Insert statements for procedure here
 DECLARE @intColumnCount  INT, 
        @intProcessCount INT, 
        @varColList      VARCHAR(max) ,
		@varFileColList      VARCHAR(max) ,
		@varFileColListwithDateTypes      VARCHAR(max) ,
		@varDestColList      VARCHAR(max) ,
		@varSQL VARCHAR(max),
		@varExtTableName VARCHAR(max),
		@colaname VARCHAR(max),
		@colanamereplace VARCHAR(max),
		@thiscol VARCHAR(max),
		@actualcol VARCHAR(max),
		@indent int

SET @varColList = '' 
SET @varFileColList = ''
SET @varFileColListwithDateTypes = ' '
SET @varDestColList = ' '
SET @colaname = ''
SET @colanamereplace = ''
set @actualcol = ''

set @varExtTableName= '"ext_' + @tbname + '"'

IF Object_id('tempdb.dbo.#tempColumnNames') IS NOT NULL 
  BEGIN 
      DROP TABLE #tempcolumnnames; 
  END 

CREATE TABLE #tempcolumnnames 
  ( 
     intid          INT, 
     colname VARCHAR(512) ,
	 colaname VARCHAR(512) ,
	 actualcol VARCHAR(512)
  ) 


BEGIN

	INSERT INTO #tempcolumnnames 
		   select  c.column_id,  c.NAME + ' varchar(4000)' , c.NAME as colaname, '[' + c.NAME + '] ' +
                CASE WHEN c.system_type_id != c.user_type_id   
                    THEN '[' + SCHEMA_NAME(tp.[schema_id]) + '].[' + tp.name + ']'   
                    ELSE '[' + UPPER(tp.name) + ']'   
                END  +   
                CASE   
                    WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary')  
                        THEN '(' + CASE WHEN c.max_length = -1   
                                        THEN 'MAX'   
                                        ELSE CAST(c.max_length AS VARCHAR(5))   
                                    END + ')'  
                    WHEN tp.name IN ('nvarchar', 'nchar')  
                        THEN '(' + CASE WHEN c.max_length = -1   
                                        THEN 'MAX'   
                                        ELSE CAST(c.max_length / 2 AS VARCHAR(5))   
                                    END + ')'  
                    WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')   
                        THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'  
                    WHEN tp.name = 'decimal'  
                        THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'  
                    ELSE ''  
                END as columnss
               
    FROM sys.columns c WITH(NOLOCK)  
    JOIN sys.types tp WITH(NOLOCK) ON c.user_type_id = tp.user_type_id  
	inner join sys.tables t on t.object_id = c.object_id 
	  inner join sys.schemas s on s.schema_id = t.schema_id 
			   where t.name =@tbname
		   and s.name = @schema 
 


	select * from #tempcolumnnames 
	SET @intProcessCount = 1 
	SET @intColumnCount = (SELECT Count(*) 
						   FROM   #tempcolumnnames) 

	WHILE ( @intProcessCount <= @intColumnCount ) 
	  BEGIN 

	   SELECT @thiscol = colaname
							   FROM   #tempcolumnnames 
							   WHERE  intid = @intProcessCount 

		  SET @varFileColList = @varFileColList + ',
		  ' 
							+ (SELECT colname
							   FROM   #tempcolumnnames 
							   WHERE  intid = @intProcessCount) 
					

set @colaname = @colaname + ',
		  ' 
							+ (SELECT colaname
							   FROM   #tempcolumnnames 
							   WHERE  intid = @intProcessCount) 

set @actualcol = @actualcol + ',
		  ' 
							+ (SELECT actualcol
							   FROM   #tempcolumnnames 
							   WHERE  intid = @intProcessCount) 

SET @colanamereplace= @colanamereplace + ',  
  case ' + @thiscol + '
  WHEN ''NULL'' THEN NULL
  ELSE ' + @thiscol + ' END as ' + @thiscol + ''

		  SET @intProcessCount +=1 
	  END 

	set @folderpath  = @datafolder

	set @varFileColList = substring( @varFileColList,2, len(@varFileColList))
	set @colaname = substring( @colaname,2, len(@colaname))
	set @colanamereplace =  substring( @colanamereplace,2, len(@colanamereplace))
	set @actualcol =  substring( @actualcol,2, len(@actualcol))

	print @actualcol;

	if @strictDataTypes = 1
	BEGIN
		set @varFileColList = @actualcol;
	END

		set @varSQL = '
			 if exists(select * from  sys.tables t 
		   inner join sys.schemas s on s.schema_id = t.schema_id where t.name =''ext_'+ @tbname + '''
		   and s.name = ''ext'')
		   begin
			 drop external table ext.ext_'+ @tbname + '
		   end
		   else
			begin
			print ''y''
		   end

		CREATE EXTERNAL TABLE [ext].[ext_' + @tbname + ']
	( ' + @varFileColList + ' )
	WITH (
	DATA_SOURCE = [' + @storageacc  + '],
	LOCATION = N'''+ @folderpath + ''',
	FILE_FORMAT = [' + @fileformat + '],    
	REJECT_TYPE = VALUE,
	REJECT_VALUE = 99999);
	'
	
	

if @loaddata= 1
 PRINT 'Create External table'
	print @varSQL
	EXEC(@varSQL)
END

PRINT @colanamereplace

if @loaddata= 1 or  @loaddata = 2
BEGIN
PRINT 'Do polybase load'


select @indent = count(*)
from sys.tables t
join sys.schemas s on (s.schema_id = t.schema_id)
join sys.identity_columns ic on (ic.object_id = t.object_id)
where t.name = @tbname and s.name = @schema



set @varSQL = ''
if @truncate = 1 
BEGIN
set @varSQL = @varSQL  + 'TRUNCATE TABLE [' + @schema + '].[' + @tbname + '];  '
END

if @indent > 0 
BEGIN
set @varSQL = @varSQL  + '
  set IDENTITY_INSERT [' + @schema + '].[' + @tbname + ']  on; '
END

set @varSQL = @varSQL  + '

   INSERT INTO [' + @schema + '].[' + @tbname + '] 
   (' + @colaname + ') 
   SELECT ' + 
   @colanamereplace
   + ' FROM [EXT].[EXT_' + @tbname + ']
   

  '
if @indent > 0 
BEGIN
set @varSQL = @varSQL  + '
    set IDENTITY_INSERT [' + @schema + '].[' + @tbname + ']  OFF; '
END

/*
if @truncate = 1 
BEGIN
set @varSQL = @varSQL  + 'TRUNCATE TABLE [' + @schema + '].[' + @tbname + ']; '
END
*/


	 print @varSQL

	 print '-----'
	 EXEC(@varSQL)

set @rundate = getdate()

declare @ssql varchar(4000)
set @ssql= ''

if @removeExternalTable = 1 
BEGIN
		print 'Removing External table'
		
		set @ssql = '
			 if exists(select * from  sys.tables t 
		   inner join sys.schemas s on s.schema_id = t.schema_id where t.name =''ext_'+ @tbname + '''
		   and s.name = ''ext'')
		   begin
			 drop external table ext.ext_'+ @tbname + '
		   end
		   else
			begin
			print ''y''
		   end'

		   print @ssql

          EXEC(@ssql)
END


end


end