create schema etl



create EXTERNAL table etl.customer
  ( c_custkey       bigint not null,
    c_name          varchar(25) not null,
    c_address       varchar(40) not null,
    c_nationkey     integer not null,
    c_phone         char(15) not null,
    c_acctbal       float not null,
    c_mktsegment    char(10) not null,
    c_comment       varchar(117) not null)
WITH
  (
    DISTRIBUTION = HASH (c_custkey),
    CLUSTERED COLUMNSTORE INDEX
  )
go


create table lineitem
  ( l_orderkey      bigint not null,
    l_partkey       bigint not null,
    l_suppkey       bigint not null,
    l_linenumber    bigint not null,
    l_quantity      float not null,
    l_extendedprice float not null,
    l_discount      float not null,
    l_tax           float not null,
    l_returnflag    char(1) not null,
    l_linestatus    char(1) not null,
    l_shipdate      date not null,
    l_commitdate    date not null,
    l_receiptdate   date not null,
    l_shipinstruct  char(25) not null,
    l_shipmode      char(10) not null,
    l_comment       varchar(44) not null)
WITH
  (
    DISTRIBUTION = HASH(l_orderkey),
    CLUSTERED COLUMNSTORE INDEX
  )
go

create table nation
  ( n_nationkey     integer not null,
    n_name          char(25) not null,
    n_regionkey     integer not null,
    n_comment       varchar(152) not null)
WITH
  (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
  )
go

create table orders
  ( o_orderkey         bigint not null,
    o_custkey          bigint not null,
    o_orderstatus      char(1) not null,
    o_totalprice       float not null,
    o_orderdate        date not null,
    o_orderpriority    char(15) not null,
    o_clerk            char(15) not null,
    o_shippriority     integer not null,
    o_comment          varchar(79) not null)
WITH
  (
    DISTRIBUTION = HASH (o_orderkey),
    CLUSTERED COLUMNSTORE INDEX
  )
go

create table part
  ( p_partkey       bigint not null,
    p_name          varchar(55) not null,
    p_mfgr          char(25) not null,
    p_brand         char(10) not null,
    p_type          varchar(25) not null,
    p_size          integer not null,
    p_container     char(10) not null,
    p_retailprice   float not null,
    p_comment       varchar(23) not null)
WITH
  (
    DISTRIBUTION = HASH (p_partkey),
    CLUSTERED COLUMNSTORE INDEX
  )
go

create table partsupp
  ( ps_partkey      bigint not null,
    ps_suppkey      bigint not null,
    ps_availqty     integer not null,
    ps_supplycost   float not null,
    ps_comment      varchar(199) not null)
WITH
  (
    DISTRIBUTION = HASH (ps_partkey),
    CLUSTERED COLUMNSTORE INDEX
  )
go

create table region
  ( r_regionkey     integer not null,
    r_name          char(25) not null,
    r_comment       varchar(152) not null)
WITH
  (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
  )

go

create table supplier
  ( s_suppkey       bigint not null,
    s_name          char(25) not null,
    s_address       varchar(40) not null,
    s_nationkey     integer not null,
    s_phone         char(15) not null,
    s_acctbal       float not null,
    s_comment       varchar(101) not null)
WITH
  (
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
  )
go

