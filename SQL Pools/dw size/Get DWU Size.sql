
-- Get DW size
SELECT DATABASEPROPERTYEX (DB_NAME(), 'ServiceObjective' ) as ServiceObjective


-- Run against master database
SELECT  db.name [Database]
,	    ds.edition [Edition]
,	    ds.service_objective [Service Objective]
FROM    sys.database_service_objectives   AS ds
JOIN    sys.databases                     AS db ON ds.database_id = db.database_id
;