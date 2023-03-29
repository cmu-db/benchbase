drop table if exists call_forwarding;
drop table if exists special_facility;
drop table if exists access_info;
drop table if exists subscriber;

create table subscriber
(
    s_id         integer     not null primary key,
    sub_nbr      varchar(15) not null unique,
    bit_1        smallint,
    bit_2        smallint,
    bit_3        smallint,
    bit_4        smallint,
    bit_5        smallint,
    bit_6        smallint,
    bit_7        smallint,
    bit_8        smallint,
    bit_9        smallint,
    bit_10       smallint,
    hex_1        smallint,
    hex_2        smallint,
    hex_3        smallint,
    hex_4        smallint,
    hex_5        smallint,
    hex_6        smallint,
    hex_7        smallint,
    hex_8        smallint,
    hex_9        smallint,
    hex_10       smallint,
    byte2_1      smallint,
    byte2_2      smallint,
    byte2_3      smallint,
    byte2_4      smallint,
    byte2_5      smallint,
    byte2_6      smallint,
    byte2_7      smallint,
    byte2_8      smallint,
    byte2_9      smallint,
    byte2_10     smallint,
    msc_location integer,
    vlr_location integer
);

create table access_info
(
    s_id    integer not null,
    ai_type smallint not null,
    data1   smallint,
    data2   smallint,
    data3   varchar(3),
    data4   varchar(5),
    primary key (s_id, ai_type),
    foreign key (s_id) references subscriber (s_id)
);


create table special_facility
(
    s_id        integer not null,
    sf_type     smallint not null,
    is_active   smallint not null,
    error_cntrl smallint,
    data_a      smallint,
    data_b      varchar(5),
    primary key (s_id, sf_type),
    foreign key (s_id) references subscriber (s_id)
);

create table call_forwarding
(
    s_id       integer not null,
    sf_type    smallint not null,
    start_time smallint not null,
    end_time   smallint,
    numberx    varchar(15),
    primary key (s_id, sf_type, start_time),
    foreign key (s_id, sf_type) references special_facility (s_id, sf_type)
);
create index idx_cf on call_forwarding (s_id);
