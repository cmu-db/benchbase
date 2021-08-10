DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS nation CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;

create table region (
   r_regionkey int not null,
   r_name string not null,
   r_comment string not null,
   PRIMARY KEY ( r_regionkey )
);

create table nation (
   n_nationkey int not null,
   n_name string not null,
   n_regionkey int not null references region(r_regionkey) ON DELETE CASCADE,
   n_comment string not null,
   PRIMARY KEY ( n_nationkey )
);

create table supplier (
   su_suppkey int not null,
   su_name string not null,
   su_address string not null,
   su_nationkey int not null references nation(n_nationkey)  ON DELETE CASCADE,
   su_phone string not null,
   su_acctbal numeric(12,2) not null,
   su_comment string not null,
   PRIMARY KEY ( su_suppkey )
);
