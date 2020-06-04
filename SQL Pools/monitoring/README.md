# Synapse

These scripts are designed to record DMV information, as DMV information is on a ring buffer, so it disappears very quickly on busy systems.

WARNING: These tables can become very very large very very quickly, so please be very careful when using these scripts.  Test them, test them and test them again!!! 

The is no archive process, so this tables will just grow and grow.

Process.
1) Exeucte the createSchema.sql script - this just creates a monitoring schema.
2) Execute the CollectStats.sql, CreateMonitoring.sql and RemoveMonitoring.sql scripts - they just create stored procs.
3) Execute - exec [monitoring].[CreateMonitoring] -- This creates the monitoring.control table
4) Execute - exec [monitoring].[CollectStats] 1  -- This creates the monitoring.*DMV* tables.
5) Create a schedued job using Azure Data Factory, Azure automation or any other schscheduleing  tool, to execute exec [monitoring].[CollectStats] 0

If you need to delete the tables, execute RemoveMonitoring.sql






