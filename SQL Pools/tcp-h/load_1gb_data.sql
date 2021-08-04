-- I created a seperate loading user and gave it 100% of the system for faster loading. Details are here: https://docs.microsoft.com/en-us/sql/t-sql/statements/execute-as-transact-sql?view=sql-server-ver15

--EXECUTE AS USER = 'loader';  

/*
--clean tables if they already exist 

truncate table customer
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
FROM 'https://ignitedsv2acc0.blob.core.windows.net/datasets/tpch1tb/supplier/*'
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


-------------------
--Create statistics
create statistics c_custkey on [customer] (c_custkey);
go                                               
create statistics c_name on [customer] (c_name);
go                                                     
create statistics c_address on [customer] (c_address);
go                                               
create statistics c_nationkey on [customer] (c_nationkey);
go                                           
create statistics c_phone on [customer] (c_phone);
go                                                   
create statistics c_acctbal on [customer] (c_acctbal);
go                                               
create statistics c_mktsegment on [customer] (c_mktsegment);
go                                         
create statistics c_comment on [customer] (c_comment);
go                                               


create statistics l_orderkey on [lineitem] (l_orderkey);
go                                             
create statistics l_partkey on [lineitem] (l_partkey);
go                                               
create statistics l_suppkey on [lineitem] (l_suppkey);
go                                               
create statistics l_linenumber on [lineitem] (l_linenumber);
go                                         
create statistics l_quantity on [lineitem] (l_quantity);
go                                             
create statistics l_extendedprice on [lineitem] (l_extendedprice);
go                                   
create statistics l_discount on [lineitem] (l_discount);
go                                             
create statistics l_tax on [lineitem] (l_tax);
go                                                       
create statistics l_returnflag on [lineitem] (l_returnflag);
go                                         
create statistics l_linestatus on [lineitem] (l_linestatus);
go                                         
create statistics l_shipdate on [lineitem] (l_shipdate);
go                                             
create statistics l_commitdate on [lineitem] (l_commitdate);
go                                         
create statistics l_receiptdate on [lineitem] (l_receiptdate);
go                                       
create statistics l_shipinstruct on [lineitem] (l_shipinstruct);
go                                     
create statistics l_shipmode on [lineitem] (l_shipmode);
go                                             
create statistics l_comment on [lineitem] (l_comment);
go                                               


create statistics n_nationkey on [nation] (n_nationkey);
go                                             
create statistics n_name on [nation] (n_name);
go                                                       
create statistics n_regionkey on [nation] (n_regionkey);
go                                             
create statistics n_comment on [nation] (n_comment);
go             

                                    
create statistics o_orderkey on [orders] (o_orderkey);
go                                               
create statistics o_custkey on [orders] (o_custkey);
go                                                 
create statistics o_orderstatus on [orders] (o_orderstatus);
go                                         
create statistics o_totalprice on [orders] (o_totalprice);
go                                           
create statistics o_orderdate on [orders] (o_orderdate);
go                                             
create statistics o_orderpriority on [orders] (o_orderpriority);
go                                     
create statistics o_clerk on [orders] (o_clerk);
go                                                     
create statistics o_shippriority on [orders] (o_shippriority);
go                                       
create statistics o_comment on [orders] (o_comment);
go                                   

              
create statistics p_partkey on [part] (p_partkey);
go                                                   
create statistics p_name on [part] (p_name);
go                                                         
create statistics p_mfgr on [part] (p_mfgr);
go                                                         
create statistics p_brand on [part] (p_brand);
go                                                       
create statistics p_type on [part] (p_type);
go                                                         
create statistics p_size on [part] (p_size);
go                                                         
create statistics p_container on [part] (p_container);
go                                               
create statistics p_retailprice on [part] (p_retailprice);
go                                           
create statistics p_comment on [part] (p_comment);
go       

                                            
create statistics ps_partkey on [partsupp] (ps_partkey);
go                                             
create statistics ps_suppkey on [partsupp] (ps_suppkey);
go                                             
create statistics ps_availqty on [partsupp] (ps_availqty);
go                                           
create statistics ps_supplycost on [partsupp] (ps_supplycost);
go                                       
create statistics ps_comment on [partsupp] (ps_comment);
go                   

                          
create statistics r_regionkey on [region] (r_regionkey);
go                                             
create statistics r_name on [region] (r_name);
go                                                       
create statistics r_comment on [region] (r_comment);
go                  

                               
create statistics s_suppkey on [supplier] (s_suppkey);
go                                               
create statistics s_name on [supplier] (s_name);
go                                                     
create statistics s_address on [supplier] (s_address);
go                                               
create statistics s_nationkey on [supplier] (s_nationkey);
go 
create statistics s_phone on [supplier] (s_phone);
go         
create statistics s_acctbal on [supplier] (s_acctbal);
go
create statistics s_comment on [supplier] (s_comment);
go 


