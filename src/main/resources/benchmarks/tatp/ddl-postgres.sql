DROP TABLE IF EXISTS access_info CASCADE;
DROP TABLE IF EXISTS call_forwarding CASCADE;
DROP TABLE IF EXISTS special_facility CASCADE;
DROP TABLE IF EXISTS subscriber CASCADE;

CREATE TABLE subscriber (
    s_id         integer     NOT NULL PRIMARY KEY,
    sub_nbr      varchar(15) NOT NULL UNIQUE,
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

CREATE TABLE access_info (
    s_id    integer  NOT NULL,
    ai_type smallint NOT NULL,
    data1   smallint,
    data2   smallint,
    data3   varchar(3),
    data4   varchar(5),
    PRIMARY KEY (s_id, ai_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
);

CREATE TABLE special_facility (
    s_id        integer  NOT NULL,
    sf_type     smallint NOT NULL,
    is_active   smallint NOT NULL,
    error_cntrl smallint,
    data_a      smallint,
    data_b      varchar(5),
    PRIMARY KEY (s_id, sf_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
);

CREATE TABLE call_forwarding (
    s_id       integer  NOT NULL,
    sf_type    smallint NOT NULL,
    start_time smallint NOT NULL,
    end_time   smallint,
    numberx    varchar(15),
    PRIMARY KEY (s_id, sf_type, start_time),
    FOREIGN KEY (s_id, sf_type) REFERENCES special_facility (s_id, sf_type)
);

CREATE INDEX idx_cf ON call_forwarding (s_id);