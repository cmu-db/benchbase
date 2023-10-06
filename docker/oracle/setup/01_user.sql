-- Copyright (c) 2023,  Oracle and/or its affiliates.
-- Non-DBA user specified in sample config file
alter session set container = FREEPDB1;
create user benchbase identified by password;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO benchbase;
-- Resourcestresser benchmark requires these two packages from SYS
GRANT EXECUTE ON DBMS_CRYPTO TO benchbase;
GRANT EXECUTE ON DBMS_LOCK TO benchbase;
