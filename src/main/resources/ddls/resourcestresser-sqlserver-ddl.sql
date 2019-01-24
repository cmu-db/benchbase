-- Drop Exisiting Tables

IF OBJECT_ID('[cputable]') IS NOT NULL DROP table [dbo].[cputable];
IF OBJECT_ID('[iotable]') IS NOT NULL DROP table [dbo].[iotable];
IF OBJECT_ID('[iotablesmallrow]') IS NOT NULL DROP table [dbo].[iotablesmallrow];
IF OBJECT_ID('[locktable]') IS NOT NULL DROP table [dbo].[locktable];

-- Create Tables

CREATE TABLE cputable (
  empid int NOT NULL,
  passwd varchar(255) NOT NULL,
  PRIMARY KEY (empid)
);

CREATE TABLE iotable (
  empid int NOT NULL,
  data1 varchar(255) NOT NULL,
  data2 varchar(255) NOT NULL,
  data3 varchar(255) NOT NULL,
  data4 varchar(255) NOT NULL,
  data5 varchar(255) NOT NULL,
  data6 varchar(255) NOT NULL,
  data7 varchar(255) NOT NULL,
  data8 varchar(255) NOT NULL,
  data9 varchar(255) NOT NULL,
  data10 varchar(255) NOT NULL,
  data11 varchar(255) NOT NULL,
  data12 varchar(255) NOT NULL,
  data13 varchar(255) NOT NULL,
  data14 varchar(255) NOT NULL,
  data15 varchar(255) NOT NULL,
  data16 varchar(255) NOT NULL,
  PRIMARY KEY (empid)
);

CREATE TABLE iotablesmallrow (
  empid int NOT NULL,
  flag1 int NOT NULL,
  PRIMARY KEY (empid)
);

CREATE TABLE locktable (
  empid int NOT NULL,
  salary int NOT NULL,
  PRIMARY KEY (empid)
);