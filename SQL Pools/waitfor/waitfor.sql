/*
  Simple proc to wait for a few seconds
*/
create proc wait_for(@seconds bigint)
as
begin
	declare @quit bit  = 0;
	declare @ConpareDateTime datetime2;
	set @ConpareDateTime = dateadd(s, @seconds, getdate() )

	while(@quit=0)
	begin
		if getdate() > @ConpareDateTime
			set @quit = 1;
	end
end