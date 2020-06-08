Result Set Caching
------------

The documentation behind [sys.dm_pdw_exec_requests] (https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-pdw-exec-requests-transact-sql?view=aps-pdw-2016-au7) shows the *result_set_cache_hit* flag is a hex value, but the results outputted in the DMV are decimal.

The table below shows both values.

|Value|	Decimal Value| Description|
|-----|--------------|------------|
|1|1	|Result set cache hit |
|-0x00|0	|Result set cache miss|
|-0x01|-1	|Result set caching is disabled on the database.|
|-0x02|-2	|Result set caching is disabled on the session.|
|-0x04|-4	|Result set caching is disabled due to no data sources for the query.|
|-0x08|-8	|Result set caching is disabled due to row level security predicates.|
|-0x10|-16	|Result set caching is disabled due to the use of system table, temporary table, or external table in the query.|
|-0x20|-32	|Result set caching is disabled because the query contains runtime constants, user-defined functions, or non-deterministic functions.|
|-0x40|-64	|Result set caching is disabled due to estimated result set size is >10GB.|
|-0x80|-128|	Result set caching is disabled because the result set contains rows with large size (>64kb).|


Code Examples
-------
Simple code samples.



Use the below code to check to see if result set caching is turned on.
<pre><code>
SELECT name, is_result_set_caching_on FROM sys.databases
</code></pre>

Turn on result_set_caching
<pre><code>
ALTER DATABASE [database_name] SET RESULT_SET_CACHING ON;
</code></pre>

Turn on/off result set cahcing for a session
<pre><code>
SET RESULT_SET_CACHING { ON | OFF };
</code></pre>


