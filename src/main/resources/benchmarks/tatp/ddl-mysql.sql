SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS access_info CASCADE;
DROP TABLE IF EXISTS call_forwarding CASCADE;
DROP TABLE IF EXISTS special_facility CASCADE;
DROP TABLE IF EXISTS subscriber CASCADE;

CREATE TABLE subscriber (
    s_id         integer     NOT NULL PRIMARY KEY,
    sub_nbr      varchar(15) NOT NULL UNIQUE,
    bit_1        tinyint,
    bit_2        tinyint,
    bit_3        tinyint,
    bit_4        tinyint,
    bit_5        tinyint,
    bit_6        tinyint,
    bit_7        tinyint,
    bit_8        tinyint,
    bit_9        tinyint,
    bit_10       tinyint,
    hex_1        tinyint,
    hex_2        tinyint,
    hex_3        tinyint,
    hex_4        tinyint,
    hex_5        tinyint,
    hex_6        tinyint,
    hex_7        tinyint,
    hex_8        tinyint,
    hex_9        tinyint,
    hex_10       tinyint,
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
    s_id    integer NOT NULL,
    ai_type tinyint NOT NULL,
    data1   smallint,
    data2   smallint,
    data3   varchar(3),
    data4   varchar(5),
    PRIMARY KEY (s_id, ai_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
);

CREATE TABLE special_facility (
    s_id        integer NOT NULL,
    sf_type     tinyint NOT NULL,
    is_active   tinyint NOT NULL,
    error_cntrl smallint,
    data_a      smallint,
    data_b      varchar(5),
    PRIMARY KEY (s_id, sf_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id)
);

CREATE TABLE call_forwarding (
    s_id       integer NOT NULL,
    sf_type    tinyint NOT NULL,
    start_time tinyint NOT NULL,
    end_time   tinyint,
    numberx    varchar(15),
    PRIMARY KEY (s_id, sf_type, start_time),
    FOREIGN KEY (s_id, sf_type) REFERENCES special_facility (s_id, sf_type)
);

CREATE INDEX idx_cf ON call_forwarding (s_id);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;