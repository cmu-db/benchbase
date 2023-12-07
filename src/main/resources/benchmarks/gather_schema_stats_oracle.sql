-- Gather optimizer statistics to improve performance
-- Sample config for tpch/chbenchmark should already include afterload option to execute this query
BEGIN dbms_stats.gather_schema_stats(ownname => 'benchbase', estimate_percent => dbms_stats.auto_sample_size, method_opt => 'for all columns size AUTO'); END;;