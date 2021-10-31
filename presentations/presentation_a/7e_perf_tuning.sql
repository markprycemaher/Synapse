---- https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-manage-monitor
-- Sessions
SELECT * FROM sys.dm_pdw_exec_sessions where status <> 'Closed' and session_id <> session_id();

-- Running requests
SELECT *
FROM sys.dm_pdw_exec_requests
WHERE status not in ('Completed','Failed','Cancelled')
  AND session_id <> session_id()
ORDER BY submit_time DESC;

-- Find top 10 queries longest running queries
SELECT TOP 100 *
FROM sys.dm_pdw_exec_requests
ORDER BY total_elapsed_time DESC;


SELECT * FROM sys.dm_pdw_request_steps
WHERE request_id = 'QID2359'
ORDER BY step_index;

-- Find the distribution run times for a SQL step.
-- Replace request_id and step_index with values from Step 1 and 3.

SELECT * FROM sys.dm_pdw_sql_requests
WHERE request_id = 'QID2359' AND step_index = 2;

-- Find the SQL Server execution plan for a query running on a specific SQL pool or control node.
-- Replace distribution_id and spid with values from previous query.
--DBCC PDW_SHOWEXECUTIONPLAN(distribution_id, pid );
DBCC PDW_SHOWEXECUTIONPLAN(1, 125);
--<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.557" Build="15.0.2195.191"><BatchSequence><Batch><Statements><StmtSimple StatementId="1" StatementType="SELECT" StatementSubTreeCost="1.257e-06" StatementEstRows="1" SecurityPolicyApplied="false" StatementOptmLevel="FULL" QueryHash="0x5C984C943A0D21D9" QueryPlanHash="0x92611ADC64C77D80" StatementOptmEarlyAbortReason="GoodEnoughPlanFound" CardinalityEstimationModelVersion="130"><QueryPlan DegreeOfParallelism="1" NonParallelPlanReason="CouldNotGenerateValidParallelPlan" CachedPlanSize="16" CompileTime="0" CompileCPU="0" CompileMemory="192"><Warnings><PlanAffectingConvert ConvertIssue="Cardinality Estimate" Expression="CONVERT(nvarchar(max),DM_EXEC_QUERY_STATS_XML.[query_plan],0)"/></Warnings><MemoryGrantInfo SerialRequiredMemory="0" SerialDesiredMemory="0" GrantedMemory="0" MaxUsedMemory="0"/><OptimizerHardwareDependentProperties EstimatedAvailableMemoryGrant="734002" EstimatedPagesCached="2936009" EstimatedAvailableDegreeOfParallelism="32" MaxCompileMemory="271884304"/><QueryTimeStats ElapsedTime="0" CpuTime="0"/><RelOp NodeId="0" PhysicalOp="Compute Scalar" LogicalOp="Compute Scalar" EstimateRows="1" EstimateIO="0" EstimateCPU="1e-07" AvgRowSize="4035" EstimatedTotalSubtreeCost="1.257e-06" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row"><OutputList><ColumnReference Column="Expr1000"/></OutputList><ComputeScalar><DefinedValues><DefinedValue><ColumnReference Column="Expr1000"/><ScalarOperator ScalarString="CONVERT(nvarchar(max),DM_EXEC_QUERY_STATS_XML.[query_plan],0)"><Convert DataType="nvarchar(max)" Length="2147483647" Style="0" Implicit="0"><ScalarOperator><Identifier><ColumnReference Table="[DM_EXEC_QUERY_STATS_XML]" Column="query_plan"/></Identifier></ScalarOperator></Convert></ScalarOperator></DefinedValue></DefinedValues><RelOp NodeId="1" PhysicalOp="Table-valued function" LogicalOp="Table-valued function" EstimateRows="1" EstimateIO="0" EstimateCPU="1.157e-06" AvgRowSize="4035" EstimatedTotalSubtreeCost="1.157e-06" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row"><OutputList><ColumnReference Table="[DM_EXEC_QUERY_STATS_XML]" Column="query_plan"/></OutputList><MemoryFractions Input="1" Output="1"/><RunTimeInformation><RunTimeCountersPerThread Thread="0" ActualRows="0" Batches="0" ActualRebinds="1" ActualRewinds="0" ActualEndOfScans="0" ActualExecutions="1" ActualExecutionMode="Row"/></RunTimeInformation><TableValuedFunction><DefinedValues><DefinedValue><ColumnReference Table="[DM_EXEC_QUERY_STATS_XML]" Column="query_plan"/></DefinedValue></DefinedValues><Object Table="[DM_EXEC_QUERY_STATS_XML]"/><ParameterList><ScalarOperator ScalarString="(125)"><Const ConstValue="(125)"/></ScalarOperator></ParameterList></TableValuedFunction></RelOp></ComputeScalar></RelOp></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>

-- Find information about all the workers completing a Data Movement Step.
-- Replace request_id and step_index with values from Step 1 and 3.

SELECT * FROM sys.dm_pdw_dms_workers
WHERE request_id = 'QID2359' AND step_index = 2;


-- Find queries
-- Replace request_id with value from Step 1.

SELECT waits.session_id,
      waits.request_id,  
      requests.command,
      requests.status,
      requests.start_time,  
      waits.type,
      waits.state,
      waits.object_type,
      waits.object_name
FROM   sys.dm_pdw_waits waits
   JOIN  sys.dm_pdw_exec_requests requests
   ON waits.request_id=requests.request_id
WHERE waits.request_id = 'QID2359'
ORDER BY waits.object_name, waits.object_type, waits.state;

