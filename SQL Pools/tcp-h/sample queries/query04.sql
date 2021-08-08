-- TPC-H/TPC-R Order Priority Checking Query (Q4)

select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= convert(date,'1997-10-01')
	and o_orderdate < convert(date,'1998-01-01')
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
