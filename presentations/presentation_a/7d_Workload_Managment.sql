CREATE LOGIN perftest WITH PASSWORD = ''; -- run on master

-- switch to normal DW
 CREATE USER perftest FOR LOGIN perftest;
 GRANT control TO perftest;

CREATE WORKLOAD GROUP smallresource
   WITH ( 
       MIN_PERCENTAGE_RESOURCE = 0
       ,CAP_PERCENTAGE_RESOURCE = 100
       ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 100
);
ALTER WORKLOAD GROUP smallresource WITH
(   MIN_PERCENTAGE_RESOURCE = 0
       ,CAP_PERCENTAGE_RESOURCE = 100
       ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 100 );

CREATE WORKLOAD CLASSIFIER [wgsmallresource]
   WITH (
	     WORKLOAD_GROUP = 'smallresource'
       ,MEMBERNAME = 'perftest'
   );



   select count(*), datepart(hr,submit_time) from sys.dm_pdw_exec_requests
   where submit_time > getdate() - 0.001
   group by datepart(hr,submit_time)
