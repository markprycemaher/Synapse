CREATE PROC [dbo].[GeneratePolybase] @tbname [varchar](200) AS
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
		@varExtTableName VARCHAR(max)

SET @varColList = '' 
SET @varFileColList = ''
SET @varFileColListwithDateTypes = ' '
SET @varDestColList = ' '

set @varExtTableName= '"ext_' + @tbname + '"'

IF Object_id('tempdb.dbo.#tempColumnNames') IS NOT NULL 
  BEGIN 
      DROP TABLE #tempcolumnnames; 
  END 

CREATE TABLE #tempcolumnnames 
  ( 
     intid          INT, 
     varcolumnnames VARCHAR(256) 
  ) 

INSERT INTO #tempcolumnnames 
select Row_number()  OVER ( 
           ORDER BY c.NAME), 
       c.NAME  from sys.columns c inner join sys.tables t on t.object_id = c.object_id where t.name =@tbname


SET @intProcessCount = 1 
SET @intColumnCount = (SELECT Count(*) 
                       FROM   #tempcolumnnames) 

WHILE ( @intProcessCount <= @intColumnCount ) 
  BEGIN 
      SET @varDestColList = @varDestColList + ', ' 
                        + (SELECT '"' + varcolumnnames + '"'
                           FROM   #tempcolumnnames 
                           WHERE  intid = @intProcessCount) 

	  SET @varFileColList = @varFileColList + ',"Column' + CONVERT(nvarchar(30),@intProcessCount) + '"'

	  SET @varFileColListwithDateTypes = @varFileColListwithDateTypes  + ',"Column' + CONVERT(nvarchar(30),@intProcessCount) + '" varchar(255)'
      SET @intProcessCount +=1 
  END 



	set @varSQL = '

CREATE EXTERNAL TABLE ' + @varExtTableName + ' (
    ' + Stuff(@varFileColListwithDateTypes, 1, 2, '')  + '
) WITH (
    LOCATION = ''in/'',
    DATA_SOURCE = HStorage,
    FILE_FORMAT = CSVFileFormat
);
INSERT INTO "dbo"."' + @tbname + '" (
    ' + Stuff(@varDestColList, 1, 2, '')  + '
)
SELECT
    "' + Stuff(@varFileColList, 1, 2, '')  + '
FROM ' + @varExtTableName + ';
DROP EXTERNAL TABLE ' + @varExtTableName + ';'

select @varSQL

EXEC(@varSQL)

END
GO


