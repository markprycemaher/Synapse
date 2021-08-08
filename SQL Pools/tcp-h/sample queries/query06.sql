-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)

select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= convert(date,'1996-01-01')
	and l_shipdate < convert(date,'1997-01-01')
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
