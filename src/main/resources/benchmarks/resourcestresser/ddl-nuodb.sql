DROP TABLE IF EXISTS cputable CASCADE;
CREATE TABLE cputable (
  empid int NOT NULL,
  passwd String NOT NULL,
  PRIMARY KEY (empid)
);

DROP TABLE IF EXISTS iotable CASCADE;
CREATE TABLE iotable (
  empid int NOT NULL,
  data1 String NOT NULL,
  data2 String NOT NULL,
  data3 String NOT NULL,
  data4 String NOT NULL,
  data5 String NOT NULL,
  data6 String NOT NULL,
  data7 String NOT NULL,
  data8 String NOT NULL,
  data9 String NOT NULL,
  data10 String NOT NULL,
  data11 String NOT NULL,
  data12 String NOT NULL,
  data13 String NOT NULL,
  data14 String NOT NULL,
  data15 String NOT NULL,
  data16 String NOT NULL,
  PRIMARY KEY (empid)
);

DROP TABLE IF EXISTS iotablesmallrow CASCADE;
CREATE TABLE iotablesmallrow (
  empid int NOT NULL,
  flag1 int NOT NULL,
  PRIMARY KEY (empid)
);

DROP TABLE IF EXISTS locktable CASCADE;
CREATE TABLE locktable (
  empid int NOT NULL,
  salary int NOT NULL,
  PRIMARY KEY (empid)
);
