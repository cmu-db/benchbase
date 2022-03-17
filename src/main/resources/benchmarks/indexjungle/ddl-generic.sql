DROP TABLE IF EXISTS jungle;

CREATE TABLE jungle (
  uuid_field VARCHAR(36) NOT NULL PRIMARY KEY,
  int_field0 INTEGER NOT NULL,
  int_field1 INTEGER NOT NULL,
  int_field2 INTEGER NOT NULL,
  int_field3 INTEGER NOT NULL,
  int_field4 INTEGER NOT NULL,
  int_field5 INTEGER NOT NULL,
  int_field6 INTEGER NOT NULL,
  int_field7 INTEGER NOT NULL,
  int_field8 INTEGER NOT NULL,
  int_field9 INTEGER NOT NULL,
  float_field0 FLOAT NOT NULL,
  float_field1 FLOAT NOT NULL,
  float_field2 FLOAT NOT NULL,
  float_field3 FLOAT NOT NULL,
  float_field4 FLOAT NOT NULL,
  float_field5 FLOAT NOT NULL,
  float_field6 FLOAT NOT NULL,
  float_field7 FLOAT NOT NULL,
  float_field8 FLOAT NOT NULL,
  float_field9 FLOAT NOT NULL,
  varchar_field0 VARCHAR(32) NOT NULL,
  varchar_field1 VARCHAR(32) NOT NULL,
  varchar_field2 VARCHAR(32) NOT NULL,
  varchar_field3 VARCHAR(32) NOT NULL,
  varchar_field4 VARCHAR(32) NOT NULL,
  varchar_field5 VARCHAR(32) NOT NULL,
  varchar_field6 VARCHAR(32) NOT NULL,
  varchar_field7 VARCHAR(32) NOT NULL,
  varchar_field8 VARCHAR(32) NOT NULL,
  varchar_field9 VARCHAR(32) NOT NULL,
  timestamp_field0 TIMESTAMP NOT NULL,
  timestamp_field1 TIMESTAMP NOT NULL,
  timestamp_field2 TIMESTAMP NOT NULL,
  timestamp_field3 TIMESTAMP NOT NULL,
  timestamp_field4 TIMESTAMP NOT NULL,
  timestamp_field5 TIMESTAMP NOT NULL,
  timestamp_field6 TIMESTAMP NOT NULL,
  timestamp_field7 TIMESTAMP NOT NULL,
  timestamp_field8 TIMESTAMP NOT NULL,
  timestamp_field9 TIMESTAMP NOT NULL
);

create index if not exists idx_timestamp_field9_float_field6_varchar_field7_varchar_field5 on jungle (timestamp_field9,float_field6,varchar_field7,varchar_field5);
create index if not exists idx_timestamp_field7_varchar_field2_float_field8 on jungle (timestamp_field7,varchar_field2,float_field8);
create index if not exists idx_float_field5_float_field1 on jungle (float_field5,float_field1);
create index if not exists idx_float_field9_int_field4 on jungle (float_field9,int_field4);
create index if not exists idx_int_field9_int_field7_timestamp_field9_int_field1_int_field0 on jungle (int_field9,int_field7,timestamp_field9,int_field1,int_field0);
create index if not exists idx_timestamp_field9_int_field5_int_field1 on jungle (timestamp_field9,int_field5,int_field1);
create index if not exists idx_timestamp_field1_int_field6 on jungle (timestamp_field1,int_field6);
create index if not exists idx_timestamp_field5_varchar_field6_uuid_field on jungle (timestamp_field5,varchar_field6,uuid_field);
create index if not exists idx_float_field9_varchar_field5_float_field1_varchar_field0 on jungle (float_field9,varchar_field5,float_field1,varchar_field0);
create index if not exists idx_timestamp_field0_int_field5_timestamp_field0_timestamp_field5 on jungle (timestamp_field0,int_field5,timestamp_field0,timestamp_field5);
create index if not exists idx_int_field2_float_field2_int_field6 on jungle (int_field2,float_field2,int_field6);
create index if not exists idx_int_field4_int_field1 on jungle (int_field4,int_field1);
create index if not exists idx_int_field5_int_field3_varchar_field4 on jungle (int_field5,int_field3,varchar_field4);
create index if not exists idx_varchar_field2_float_field5_varchar_field2_timestamp_field5 on jungle (varchar_field2,float_field5,varchar_field2,timestamp_field5);
create index if not exists idx_int_field4_timestamp_field1_int_field0 on jungle (int_field4,timestamp_field1,int_field0);
create index if not exists idx_timestamp_field7 on jungle (timestamp_field7);
create index if not exists idx_timestamp_field5_varchar_field4_float_field0_float_field5_float_field6 on jungle (timestamp_field5,varchar_field4,float_field0,float_field5,float_field6);
create index if not exists idx_int_field5_int_field5 on jungle (int_field5,int_field5);
create index if not exists idx_float_field4_timestamp_field7 on jungle (float_field4,timestamp_field7);
create index if not exists idx_float_field7_timestamp_field6 on jungle (float_field7,timestamp_field6);
create index if not exists idx_varchar_field2_varchar_field8_float_field3_timestamp_field9_int_field8 on jungle (varchar_field2,varchar_field8,float_field3,timestamp_field9,int_field8);
create index if not exists idx_varchar_field5_int_field7_timestamp_field6_timestamp_field2 on jungle (varchar_field5,int_field7,timestamp_field6,timestamp_field2);
create index if not exists idx_int_field6_float_field4_int_field5_float_field2_timestamp_field6 on jungle (int_field6,float_field4,int_field5,float_field2,timestamp_field6);
create index if not exists idx_varchar_field3_timestamp_field1 on jungle (varchar_field3,timestamp_field1);
create index if not exists idx_float_field7_float_field5_uuid_field_varchar_field7_int_field2 on jungle (float_field7,float_field5,uuid_field,varchar_field7,int_field2);
create index if not exists idx_int_field5_varchar_field0_int_field5_varchar_field4 on jungle (int_field5,varchar_field0,int_field5,varchar_field4);
create index if not exists idx_int_field4_timestamp_field4_timestamp_field5 on jungle (int_field4,timestamp_field4,timestamp_field5);
create index if not exists idx_varchar_field7_float_field7_float_field7_float_field4_timestamp_field8 on jungle (varchar_field7,float_field7,float_field7,float_field4,timestamp_field8);
create index if not exists idx_float_field9_varchar_field6_int_field4_timestamp_field7 on jungle (float_field9,varchar_field6,int_field4,timestamp_field7);
create index if not exists idx_int_field5_varchar_field7_float_field3 on jungle (int_field5,varchar_field7,float_field3);
create index if not exists idx_int_field1_int_field1_float_field1 on jungle (int_field1,int_field1,float_field1);
create index if not exists idx_timestamp_field0 on jungle (timestamp_field0);
create index if not exists idx_int_field0_varchar_field9_timestamp_field2_varchar_field8 on jungle (int_field0,varchar_field9,timestamp_field2,varchar_field8);
create index if not exists idx_int_field1_varchar_field5_varchar_field7_int_field4_timestamp_field2 on jungle (int_field1,varchar_field5,varchar_field7,int_field4,timestamp_field2);
create index if not exists idx_varchar_field4_timestamp_field9_varchar_field4_timestamp_field2 on jungle (varchar_field4,timestamp_field9,varchar_field4,timestamp_field2);
create index if not exists idx_int_field2_timestamp_field3_varchar_field8_timestamp_field2_uuid_field on jungle (int_field2,timestamp_field3,varchar_field8,timestamp_field2,uuid_field);
create index if not exists idx_timestamp_field6_timestamp_field2_int_field0_float_field0_varchar_field1 on jungle (timestamp_field6,timestamp_field2,int_field0,float_field0,varchar_field1);
create index if not exists idx_timestamp_field5_int_field6_uuid_field_int_field0_int_field4 on jungle (timestamp_field5,int_field6,uuid_field,int_field0,int_field4);
create index if not exists idx_varchar_field4_float_field5 on jungle (varchar_field4,float_field5);
create index if not exists idx_int_field6_float_field8 on jungle (int_field6,float_field8);
create index if not exists idx_int_field4 on jungle (int_field4);
create index if not exists idx_int_field6 on jungle (int_field6);
create index if not exists idx_timestamp_field3_uuid_field on jungle (timestamp_field3,uuid_field);
create index if not exists idx_timestamp_field3_timestamp_field7_varchar_field5 on jungle (timestamp_field3,timestamp_field7,varchar_field5);
create index if not exists idx_timestamp_field2_timestamp_field7 on jungle (timestamp_field2,timestamp_field7);
create index if not exists idx_int_field2_varchar_field0_int_field2_timestamp_field0_timestamp_field9 on jungle (int_field2,varchar_field0,int_field2,timestamp_field0,timestamp_field9);
create index if not exists idx_int_field8_varchar_field4_int_field1_timestamp_field2_int_field4 on jungle (int_field8,varchar_field4,int_field1,timestamp_field2,int_field4);
create index if not exists idx_float_field5_varchar_field2_timestamp_field8_int_field1 on jungle (float_field5,varchar_field2,timestamp_field8,int_field1);
create index if not exists idx_varchar_field0 on jungle (varchar_field0);
create index if not exists idx_int_field2_int_field4_varchar_field5 on jungle (int_field2,int_field4,varchar_field5);
