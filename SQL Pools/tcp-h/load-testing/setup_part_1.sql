
drop table  dbo.dataloading_logs;

create table dbo.dataloading_logs
(
[start_time] datetime2,
[end_time] datetime2,
[data_size] varchar(99),
[schema_name] varchar(99),
[table_name] varchar(99),
[slo] varchar(99),
[input_type] varchar(99),
[sql] varchar(8000),
[row_count] bigint,
[label] varchar(99)
)

 
alter proc [dbo].[log_it](@start_time datetime2,
		@end_time datetime2,
		@data_size varchar(99),
		@schema_name varchar(99),
		@table_name varchar(99),
		@slo varchar(99),
		@input_type varchar(99),
		@sql varchar(8000),
		@label varchar(99))
as
begin 

INSERT INTO [dbo].[dataloading_logs]
           ([start_time]
           ,[end_time]
		   ,[data_size]
           ,[schema_name]
           ,[table_name]
           ,[slo]
           ,[input_type]
		   ,[sql]
		   ,[label])
     VALUES
           (@start_time 
           ,@end_time 
		   ,@data_size 
           ,@schema_name 
           ,@table_name 
           ,@slo
           ,@input_type
		   ,@sql
		   ,@label)

end