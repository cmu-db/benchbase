-- Non-DBA user specified in sample config file
DROP USER benchbase CASCADE;
CREATE USER benchbase IDENTIFIED BY password;
GRANT CONNECT, RESOURCE, CREATE VIEW, UNLIMITED TABLESPACE TO benchbase;
-- Resourcestresser benchmark requires these two packages from SYS
GRANT EXECUTE ON DBMS_CRYPTO TO benchbase;
GRANT EXECUTE ON DBMS_LOCK TO benchbase;
