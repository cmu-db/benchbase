-- This SQL file recreate Oracle DB user (benchbase/password) for sample configs
-- This file is inteded to be copied into the OracleDB container, to be invoked later using sqlplus
-- docker cp config/oracle/scripts/reset.sql oracle:/opt/oracle/reset.sql
-- docker exec oracle sqlplus "sys/password@xepdb1 as sysdba" @reset.sql

DROP USER benchbase CASCADE;
CREATE USER benchbase IDENTIFIED BY password;
GRANT CONNECT, RESOURCE, CREATE VIEW, UNLIMITED TABLESPACE TO benchbase;
-- Resourcestresser benchmark for Oracle requires access to these two packages
-- These will not be needed if running with user sys/system instead
GRANT EXECUTE ON DBMS_CRYPTO TO benchbase;
GRANT EXECUTE ON DBMS_LOCK TO benchbase;
