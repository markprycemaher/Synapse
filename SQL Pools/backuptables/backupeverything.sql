ALTER PROC [dbo].[BackupEverything] AS
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
		@varDate varchar(200),
		@tbl varchar(200)

SET @varColList = '' 
SET @varFileColList = ''
SET @varFileColListwithDateTypes = ' '
SET @varDestColList = ' '

select @varDate =  convert(varchar,datepart(yyyy,getdate())) + '' + convert(varchar,datepart(mm,getdate()))+ '' + convert(varchar,datepart(dd,getdate()))


IF Object_id('tempdb.dbo.#tempBackupTables') IS NOT NULL 
  BEGIN 
      DROP TABLE #tempBackupTables; 
  END 

CREATE TABLE #tempBackupTables 
  ( 
     intid          INT, 
     vartables VARCHAR(256) 
  ) 

INSERT INTO #tempBackupTables 
select Row_number()  OVER ( 
           ORDER BY c.NAME), 
        name from sys.tables  c where is_external = 0


SET @intProcessCount = 1 
SET @intColumnCount = (SELECT Count(*) 
                       FROM   #tempBackupTables) 

WHILE ( @intProcessCount <= @intColumnCount ) 
  BEGIN 
       SET @intProcessCount +=1 

	   SELECT @tbl = vartables 
                           FROM   #tempBackupTables 
                           WHERE  intid = @intProcessCount

		exec [dbo].[BackupTable] @tbl, @varDate


  END 


END