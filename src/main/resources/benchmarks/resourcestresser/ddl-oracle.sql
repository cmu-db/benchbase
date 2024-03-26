-- Drop all tables

BEGIN EXECUTE IMMEDIATE 'DROP TABLE cputable'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE iotable'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE iotablesmallrow'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE locktable'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

-- Create table

CREATE TABLE cputable (
  empid integer NOT NULL,
  passwd varchar2(255) NOT NULL,
  PRIMARY KEY (empid)
);

CREATE TABLE iotable (
  empid int NOT NULL,
  data1 varchar2(255) NOT NULL,
  data2 varchar2(255) NOT NULL,
  data3 varchar2(255) NOT NULL,
  data4 varchar2(255) NOT NULL,
  data5 varchar2(255) NOT NULL,
  data6 varchar2(255) NOT NULL,
  data7 varchar2(255) NOT NULL,
  data8 varchar2(255) NOT NULL,
  data9 varchar2(255) NOT NULL,
  data10 varchar2(255) NOT NULL,
  data11 varchar2(255) NOT NULL,
  data12 varchar2(255) NOT NULL,
  data13 varchar2(255) NOT NULL,
  data14 varchar2(255) NOT NULL,
  data15 varchar2(255) NOT NULL,
  data16 varchar2(255) NOT NULL,
  PRIMARY KEY (empid)
);

CREATE TABLE iotablesmallrow (
  empid integer NOT NULL,
  flag1 integer NOT NULL,
  PRIMARY KEY (empid)
);

CREATE TABLE locktable (
  empid integer NOT NULL,
  salary integer NOT NULL,
  PRIMARY KEY (empid)
);

-- Procedures

-- See https://docs.oracle.com/en/database/oracle/oracle-database/23/arpls/DBMS_CRYPTO.html
-- DBMS_OBFUSCATION_TOOLKIT is deprecated, using DBMS_CRYPTO

-- MD5 is deprecated for Oracle 23c onwards, potentially change to SHA-2 using DBMS_CRYPTO.HASH_SH256.
-- The procedure name however should still be kept as "md5".

-- Current (Oct 5, 2023) dialect file cannot change the function name as it can only change the SQL string
--      inside SQLStmt, but related SQL for this procedure is constructed outside.

create or replace
function md5(text in varchar2)
return varchar2 is hash_value varchar2(32)
;begin
select lower(UTL_I18N.RAW_TO_CHAR (DBMS_CRYPTO.HASH(text, DBMS_CRYPTO.HASH_MD5), 'AL32UTF8'))
into hash_value from dual
;return hash_value
;end;;
