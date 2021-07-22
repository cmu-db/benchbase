SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS cputable CASCADE;
DROP TABLE IF EXISTS iotable CASCADE;
DROP TABLE IF EXISTS iotablesmallrow CASCADE;
DROP TABLE IF EXISTS locktable CASCADE;

CREATE TABLE cputable (
    empid  int       NOT NULL,
    passwd char(255) NOT NULL,
    PRIMARY KEY (empid)
);

CREATE TABLE iotable (
    empid  int       NOT NULL,
    data1  char(255) NOT NULL,
    data2  char(255) NOT NULL,
    data3  char(255) NOT NULL,
    data4  char(255) NOT NULL,
    data5  char(255) NOT NULL,
    data6  char(255) NOT NULL,
    data7  char(255) NOT NULL,
    data8  char(255) NOT NULL,
    data9  char(255) NOT NULL,
    data10 char(255) NOT NULL,
    data11 char(255) NOT NULL,
    data12 char(255) NOT NULL,
    data13 char(255) NOT NULL,
    data14 char(255) NOT NULL,
    data15 char(255) NOT NULL,
    data16 char(255) NOT NULL,
    PRIMARY KEY (empid)
);

CREATE TABLE iotablesmallrow (
    empid int NOT NULL,
    flag1 int NOT NULL,
    PRIMARY KEY (empid)
);

CREATE TABLE locktable (
    empid  int NOT NULL,
    salary int NOT NULL,
    PRIMARY KEY (empid)
);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;