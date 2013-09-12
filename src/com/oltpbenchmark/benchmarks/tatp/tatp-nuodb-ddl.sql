DROP TABLE IF EXISTS SUBSCRIBER;
CREATE TABLE SUBSCRIBER (
   s_id INTEGER NOT NULL PRIMARY KEY,
   sub_nbr VARCHAR(15) NOT NULL UNIQUE,
   bit_1 int,
   bit_2 int,
   bit_3 int,
   bit_4 int,
   bit_5 int,
   bit_6 int,
   bit_7 int,
   bit_8 int,
   bit_9 int,
   bit_10 int,
   hex_1 int,
   hex_2 int,
   hex_3 int,
   hex_4 int,
   hex_5 int,
   hex_6 int,
   hex_7 int,
   hex_8 int,
   hex_9 int,
   hex_10 int,
   byte2_1 SMALLINT,
   byte2_2 SMALLINT,
   byte2_3 SMALLINT,
   byte2_4 SMALLINT,
   byte2_5 SMALLINT,
   byte2_6 SMALLINT,
   byte2_7 SMALLINT,
   byte2_8 SMALLINT,
   byte2_9 SMALLINT,
   byte2_10 SMALLINT,
   msc_location INTEGER,
   vlr_location INTEGER
);

DROP TABLE IF EXISTS ACCESS_INFO;
CREATE TABLE ACCESS_INFO (
   s_id INTEGER NOT NULL,
   ai_type int NOT NULL,
   data1 SMALLINT,
   data2 SMALLINT,
   data3 VARCHAR(3),
   data4 VARCHAR(5),
   PRIMARY KEY(s_id, ai_type),
   FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)
);

DROP TABLE IF EXISTS SPECIAL_FACILITY;
CREATE TABLE SPECIAL_FACILITY (
   s_id INTEGER NOT NULL,
   sf_type int NOT NULL,
   is_active int NOT NULL,
   error_cntrl SMALLINT,
   data_a SMALLINT,
   data_b VARCHAR(5),
   PRIMARY KEY (s_id, sf_type),
   FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)
);

DROP TABLE IF EXISTS CALL_FORWARDING;
CREATE TABLE CALL_FORWARDING (
   s_id INTEGER NOT NULL,
   sf_type int NOT NULL,
   start_time int NOT NULL,
   end_time int,
   numberx VARCHAR(15),
   PRIMARY KEY (s_id, sf_type, start_time),
   FOREIGN KEY (s_id, sf_type) REFERENCES SPECIAL_FACILITY(s_id, sf_type)
);
CREATE INDEX IDX_CF ON CALL_FORWARDING (S_ID);
