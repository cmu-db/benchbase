-- SQL Server Database Console Command statements (DBCC)
-- NOTE: Requires "ALTER SERVER STATE" permission
DBCC DROPCLEANBUFFERS -- clear buffers (for cold runs)
DBCC FREEPROCCACHE -- clean plan cache