-- Drop all tables

BEGIN EXECUTE IMMEDIATE 'DROP TABLE "cputable"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "iotable"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "iotableSmallrow"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "locktable"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

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

CREATE TABLE iotableSmallrow (
  empid integer NOT NULL,
  flag1 integer NOT NULL,
  PRIMARY KEY (empid)
);

CREATE TABLE locktable (
  empid integer NOT NULL,
  salary integer NOT NULL,
  PRIMARY KEY (empid)
);