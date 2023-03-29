-- ycsb ddl for microsoft sql server

if object_id('usertable') is not null drop table usertable;

create table usertable(
  ycsb_key int primary key,
  field1 text, 
  field2 text,
  field3 text, 
  field4 text,
  field5 text, 
  field6 text,
  field7 text, 
  field8 text,
  field9 text, 
  field10 text
  );
