/* HINTs */

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count_big(*) as count_order
from
    lineitem
where
    l_shipdate <= dateadd(day,-90,cast ('1998-12-01' as date))
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
	option(MAXDOP 1, 
		label='hint test1', 
		FORCE ORDER, ----- ONLY use when there are lots of tables
					----- stops the optimizer trying lots of options
		USE HINT ('FORCE_LEGACY_CARDINALITY_ESTIMATION')
		-- use the old estimation model
		USE HINT('allow_batch_mode') -- stops plans from going into ROW mode
		USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE') 
		)