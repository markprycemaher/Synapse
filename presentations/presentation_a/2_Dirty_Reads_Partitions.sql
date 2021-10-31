-- Scripts  to swap data between tables.

-- Thanks to Ron for this ideas : https://www.linkedin.com/pulse/ctas-alter-switch-vs-drop-rename-ron-dunn/

-- 3 Different options
-- Option 1 : Enable READ COMMITTED SNAPSHOT ISOLATION  https://docs.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql-set-options?toc=/azure/synapse-analytics/sql-data-warehouse/toc.json&bc=/azure/synapse-analytics/sql-data-warehouse/breadcrumb/toc.json&view=azure-sqldw-latest&preserve-view=true

-- connect to master
ALTER DATABASE [mydatabase] SET READ_COMMITTED_SNAPSHOT ON;
select is_read_committed_snapshot_on, [name] from sys.databases;

-- Option 2 : Rename tables

-- create my reporting table
/*
drop table  dbo.my_table
drop table  dbo.my_staging_table
*/


create table dbo.my_table
(
    id int,
    firstname varchar(50)
)

-- create staging table
create table dbo.my_staging_table
(
    id int,
    firstname varchar(50)
)

-- insert some data
insert into dbo.my_table values (1,'mark');

insert into dbo.my_staging_table values (1,'mark');
insert into dbo.my_staging_table values (2,'john');
insert into dbo.my_staging_table values (3,'peter');

--select count(*) from dbo.my_staging_table;
--select count(*) from dbo.my_table;

-- swap tables around
rename object dbo.my_table to my_table_old;
rename object dbo.my_staging_table to my_table;
drop table dbo.my_table_old;

select * from my_table;


-- Option 3 : Swap partitions

-- Each table in Azure DW / Synapse Dedicated SQL Pool is created with a partition. 
/*
drop table  dbo.my_table
drop table  dbo.my_staging_table
*/
-- create my reporting table
create table dbo.my_table
(
    id int,
    firstname varchar(50)
)

-- create staging table
create table dbo.my_staging_table
(
    id int,
    firstname varchar(50)
)

-- insert some data
insert into dbo.my_table values (1,'mark');

insert into dbo.my_staging_table values (1,'mark');
insert into dbo.my_staging_table values (2,'john');
insert into dbo.my_staging_table values (3,'peter');

--select count(*) from dbo.my_staging_table;
--select count(*) from dbo.my_table;

-- copy the data from table dbo.my_staging_table to  dbo.my_table 
ALTER TABLE dbo.my_staging_table SWITCH TO  dbo.my_table  with (truncate_target=on);

-- see the results
select * from dbo.my_staging_table ;
select * from dbo.my_table ;

/*
drop table  dbo.my_table
drop table  dbo.my_staging_table
*/
