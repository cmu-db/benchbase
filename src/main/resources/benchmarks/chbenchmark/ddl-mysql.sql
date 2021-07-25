SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS region CASCADE;

CREATE TABLE region (
    r_regionkey int       NOT NULL,
    r_name      char(55)  NOT NULL,
    r_comment   char(152) NOT NULL,
    PRIMARY KEY (r_regionkey)
);

CREATE TABLE nation (
    n_nationkey int       NOT NULL,
    n_name      char(25)  NOT NULL,
    n_regionkey int       NOT NULL,
    n_comment   char(152) NOT NULL,
    FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey) ON DELETE CASCADE,
    PRIMARY KEY (n_nationkey)
);

CREATE TABLE supplier (
    su_suppkey   int            NOT NULL,
    su_name      char(25)       NOT NULL,
    su_address   varchar(40)    NOT NULL,
    su_nationkey int            NOT NULL,
    su_phone     char(15)       NOT NULL,
    su_acctbal   numeric(12, 2) NOT NULL,
    su_comment   char(101)      NOT NULL,
    FOREIGN KEY (su_nationkey) REFERENCES nation (n_nationkey) ON DELETE CASCADE,
    PRIMARY KEY (su_suppkey)
);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;