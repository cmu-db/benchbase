drop table if exists checking;
drop table if exists savings;
drop table if exists accounts;

create table accounts (
    custid      bigint      not null,
    name        varchar(64) not null,
    constraint pk_accounts primary key (custid)
);
create index idx_accounts_name on accounts (name);

create table savings (
    custid      bigint      not null,
    bal         float       not null,
    constraint pk_savings primary key (custid),
    foreign key (custid) references accounts (custid)
);

create table checking (
    custid      bigint      not null,
    bal         float       not null,
    constraint pk_checking primary key (custid),
    foreign key (custid) references accounts (custid)
);
