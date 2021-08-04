-- This demo uses the TCP-H table Lineitem , but will work with any table

--drop table lineitem_dest;
--drop table lineitem_source;


create table lineitem_dest with (distribution = round_robin) as select * from lineitem where 1=0;
create table lineitem_source with (distribution = round_robin) as select * from lineitem where 1=1;

select count_big(*) from lineitem_dest;
select count_big(*) from lineitem_source;  -- rows 12,002,430

ALTER TABLE dbo.lineitem_source SWITCH TO  dbo.lineitem_dest  with (truncate_target=on);

select count_big(*) from lineitem_dest;
select count_big(*) from lineitem_source;

-- I can even swap back
-- ALTER TABLE dbo.lineitem_dest SWITCH TO   dbo.lineitem_source   with (truncate_target=on);
