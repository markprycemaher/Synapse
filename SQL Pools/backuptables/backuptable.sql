CREATE PROC [dbo].[BackupTable] @tbname [varchar](200),@subfolder [varchar](200) AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Insert statements for procedure here
 DECLARE @intColumnCount  INT, 
        @intProcessCount INT, 
        @varColList      VARCHAR(max) ,
		@varFileColList      VARCHAR(max) ,
		@varFileColListwithDateTypes      VARCHAR(max) ,
		@varDestColList      VARCHAR(max) ,
		@varSQL VARCHAR(max),
		@varExtTableName VARCHAR(max),
		@dbname varchar(200)

SET @varColList = '' 
SET @varFileColList = ''
SET @varFileColListwithDateTypes = ' '
SET @varDestColList = ' '

set @varExtTableName= '"ext_' + @tbname + '"'


select @dbname = name from sys.databases where database_id > 1

set @varSQL = '
CREATE EXTERNAL TABLE [dbo].' + @varExtTableName + '
WITH 
(
	LOCATION = ''backup/' + @dbname + '/' + @subfolder + '/' + @tbname + ''' ,      
	DATA_SOURCE = MastData_stor,      
	FILE_FORMAT = CSVFileFormat 
)
AS
SELECT * from dbo.' + @tbname + ' ;

DROP EXTERNAL TABLE ' + @varExtTableName + ';'

select @varSQL

EXEC(@varSQL)

END
GO