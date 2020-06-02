DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;

CREATE TABLE region (
    r_regionkey int       NOT NULL,
    r_name      char(55)  NOT NULL,
    r_comment   char(152) NOT NULL,
    PRIMARY KEY (r_regionkey)
);

CREATE TABLE nation (
    n_nationkey int       NOT NULL,
    n_name      char(25)  NOT NULL,
    n_regionkey int       NOT NULL REFERENCES region (r_regionkey) ON DELETE CASCADE,
    n_comment   char(152) NOT NULL,
    PRIMARY KEY (n_nationkey)
);

CREATE TABLE supplier (
    su_suppkey   int            NOT NULL,
    su_name      char(25)       NOT NULL,
    su_address   varchar(40)    NOT NULL,
    su_nationkey int            NOT NULL REFERENCES nation (n_nationkey) ON DELETE CASCADE,
    su_phone     char(15)       NOT NULL,
    su_acctbal   numeric(12, 2) NOT NULL,
    su_comment   char(101)      NOT NULL,
    PRIMARY KEY (su_suppkey)
);
