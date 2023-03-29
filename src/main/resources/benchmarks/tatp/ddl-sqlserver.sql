-- drop exisiting tables
if object_id('access_info') is not null drop table access_info;
if object_id('call_forwarding') is not null drop table call_forwarding;
if object_id('special_facility') is not null drop table special_facility;
if object_id('subscriber') is not null drop table subscriber;

-- create tables

create table subscriber (
   s_id integer not null primary key,
   sub_nbr varchar(15) not null unique,
   bit_1 tinyint,
   bit_2 tinyint,
   bit_3 tinyint,
   bit_4 tinyint,
   bit_5 tinyint,
   bit_6 tinyint,
   bit_7 tinyint,
   bit_8 tinyint,
   bit_9 tinyint,
   bit_10 tinyint,
   hex_1 tinyint,
   hex_2 tinyint,
   hex_3 tinyint,
   hex_4 tinyint,
   hex_5 tinyint,
   hex_6 tinyint,
   hex_7 tinyint,
   hex_8 tinyint,
   hex_9 tinyint,
   hex_10 tinyint,
   byte2_1 smallint,
   byte2_2 smallint,
   byte2_3 smallint,
   byte2_4 smallint,
   byte2_5 smallint,
   byte2_6 smallint,
   byte2_7 smallint,
   byte2_8 smallint,
   byte2_9 smallint,
   byte2_10 smallint,
   msc_location integer,
   vlr_location integer
);

create table access_info (
   s_id integer not null,
   ai_type tinyint not null,
   data1 smallint,
   data2 smallint,
   data3 varchar(3),
   data4 varchar(5),
   primary key(s_id, ai_type),
   foreign key (s_id) references subscriber (s_id)
);

create table special_facility (
   s_id integer not null,
   sf_type tinyint not null,
   is_active tinyint not null,
   error_cntrl smallint,
   data_a smallint,
   data_b varchar(5),
   primary key (s_id, sf_type),
   foreign key (s_id) references subscriber (s_id)
);

create table call_forwarding (
   s_id integer not null,
   sf_type tinyint not null,
   start_time tinyint not null,
   end_time tinyint,
   numberx varchar(15),
   primary key (s_id, sf_type, start_time),
   foreign key (s_id, sf_type) references special_facility(s_id, sf_type)	
);

-- create indexes
create index idx_cf on call_forwarding (s_id);