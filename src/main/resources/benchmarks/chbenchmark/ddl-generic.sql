DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;

create table region
(
    r_regionkey int       not null,
    r_name      char(55)  not null,
    r_comment   char(152) not null,
    PRIMARY KEY (r_regionkey)
);

create table nation
(
    n_nationkey int    not null,
   n_name char(25) not null,
   n_regionkey int not null references region(r_regionkey) ON DELETE CASCADE,
   n_comment char(152) not null,
   PRIMARY KEY ( n_nationkey )
);

create table supplier (
   su_suppkey int not null,
   su_name char(25) not null,
   su_address varchar(40) not null,
   su_nationkey int not null references nation(n_nationkey)  ON DELETE CASCADE,
   su_phone char(15) not null,
   su_acctbal numeric(12,2) not null,
   su_comment char(101) not null,
   PRIMARY KEY ( su_suppkey )
);