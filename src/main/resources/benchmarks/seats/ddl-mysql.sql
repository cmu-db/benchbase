SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

-- Drop Tables
DROP TABLE IF EXISTS config_profile CASCADE;
DROP TABLE IF EXISTS config_histograms CASCADE;
DROP TABLE IF EXISTS reservation CASCADE;
DROP TABLE IF EXISTS frequent_flyer CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS flight CASCADE;
DROP TABLE IF EXISTS airport_distance CASCADE;
DROP TABLE IF EXISTS airport CASCADE;
DROP TABLE IF EXISTS airline CASCADE;
DROP TABLE IF EXISTS country CASCADE;

-- 
-- CONFIG_PROFILE
--
CREATE TABLE config_profile (
    cfp_scale_factor        float                               NOT NULL,
    cfp_aiport_max_customer text                                NOT NULL,
    cfp_flight_start        timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    cfp_flight_upcoming     timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    cfp_flight_past_days    int                                 NOT NULL,
    cfp_flight_future_days  int                                 NOT NULL,
    cfp_flight_offset       int,
    cfp_reservation_offset  int,
    cfp_num_reservations    bigint                              NOT NULL,
    cfp_code_ids_xrefs      text                                NOT NULL
);

--
-- CONFIG_HISTOGRAMS
--
CREATE TABLE config_histograms (
    cfh_name       varchar(128)   NOT NULL,
    cfh_data       varchar(10005) NOT NULL,
    cfh_is_airport tinyint DEFAULT 0,
    PRIMARY KEY (cfh_name)
);

-- 
-- COUNTRY
--
CREATE TABLE country (
    co_id     bigint      NOT NULL,
    co_name   varchar(64) NOT NULL,
    co_code_2 varchar(2)  NOT NULL,
    co_code_3 varchar(3)  NOT NULL,
    PRIMARY KEY (co_id)
);

--
-- AIRPORT
--
CREATE TABLE airport (
    ap_id          bigint       NOT NULL,
    ap_code        varchar(3)   NOT NULL,
    ap_name        varchar(128) NOT NULL,
    ap_city        varchar(64)  NOT NULL,
    ap_postal_code varchar(12),
    ap_co_id       bigint       NOT NULL,
    ap_longitude   float,
    ap_latitude    float,
    ap_gmt_offset  float,
    ap_wac         bigint,
    ap_iattr00     bigint,
    ap_iattr01     bigint,
    ap_iattr02     bigint,
    ap_iattr03     bigint,
    ap_iattr04     bigint,
    ap_iattr05     bigint,
    ap_iattr06     bigint,
    ap_iattr07     bigint,
    ap_iattr08     bigint,
    ap_iattr09     bigint,
    ap_iattr10     bigint,
    ap_iattr11     bigint,
    ap_iattr12     bigint,
    ap_iattr13     bigint,
    ap_iattr14     bigint,
    ap_iattr15     bigint,
    PRIMARY KEY (ap_id),
    FOREIGN KEY (ap_co_id) REFERENCES country (co_id)
);

--
-- AIRPORT_DISTANCE
--
CREATE TABLE airport_distance (
    d_ap_id0   bigint NOT NULL,
    d_ap_id1   bigint NOT NULL,
    d_distance float  NOT NULL,
    PRIMARY KEY (d_ap_id0, d_ap_id1),
    FOREIGN KEY (d_ap_id0) REFERENCES airport (ap_id),
    FOREIGN KEY (d_ap_id1) REFERENCES airport (ap_id)
);

--
-- AIRLINE
--
CREATE TABLE airline (
    al_id        bigint       NOT NULL,
    al_iata_code varchar(3),
    al_icao_code varchar(3),
    al_call_sign varchar(32),
    al_name      varchar(128) NOT NULL,
    al_co_id     bigint       NOT NULL,
    al_iattr00   bigint,
    al_iattr01   bigint,
    al_iattr02   bigint,
    al_iattr03   bigint,
    al_iattr04   bigint,
    al_iattr05   bigint,
    al_iattr06   bigint,
    al_iattr07   bigint,
    al_iattr08   bigint,
    al_iattr09   bigint,
    al_iattr10   bigint,
    al_iattr11   bigint,
    al_iattr12   bigint,
    al_iattr13   bigint,
    al_iattr14   bigint,
    al_iattr15   bigint,
    PRIMARY KEY (al_id),
    FOREIGN KEY (al_co_id) REFERENCES country (co_id)
);

--
-- CUSTOMER
--
CREATE TABLE customer (
    c_id         varchar(128)             NOT NULL,
    c_id_str     varchar(64) UNIQUE NOT NULL,
    c_base_ap_id bigint,
    c_balance    float              NOT NULL,
    c_sattr00    varchar(32),
    c_sattr01    varchar(8),
    c_sattr02    varchar(8),
    c_sattr03    varchar(8),
    c_sattr04    varchar(8),
    c_sattr05    varchar(8),
    c_sattr06    varchar(8),
    c_sattr07    varchar(8),
    c_sattr08    varchar(8),
    c_sattr09    varchar(8),
    c_sattr10    varchar(8),
    c_sattr11    varchar(8),
    c_sattr12    varchar(8),
    c_sattr13    varchar(8),
    c_sattr14    varchar(8),
    c_sattr15    varchar(8),
    c_sattr16    varchar(8),
    c_sattr17    varchar(8),
    c_sattr18    varchar(8),
    c_sattr19    varchar(8),
    c_iattr00    bigint,
    c_iattr01    bigint,
    c_iattr02    bigint,
    c_iattr03    bigint,
    c_iattr04    bigint,
    c_iattr05    bigint,
    c_iattr06    bigint,
    c_iattr07    bigint,
    c_iattr08    bigint,
    c_iattr09    bigint,
    c_iattr10    bigint,
    c_iattr11    bigint,
    c_iattr12    bigint,
    c_iattr13    bigint,
    c_iattr14    bigint,
    c_iattr15    bigint,
    c_iattr16    bigint,
    c_iattr17    bigint,
    c_iattr18    bigint,
    c_iattr19    bigint,
    PRIMARY KEY (c_id),
    FOREIGN KEY (c_base_ap_id) REFERENCES airport (ap_id)
);

--
-- FREQUENT_FLYER
--
CREATE TABLE frequent_flyer (
    ff_c_id     varchar(128)      NOT NULL,
    ff_al_id    bigint      NOT NULL,
    ff_c_id_str varchar(64) NOT NULL,
    ff_sattr00  varchar(32),
    ff_sattr01  varchar(32),
    ff_sattr02  varchar(32),
    ff_sattr03  varchar(32),
    ff_iattr00  bigint,
    ff_iattr01  bigint,
    ff_iattr02  bigint,
    ff_iattr03  bigint,
    ff_iattr04  bigint,
    ff_iattr05  bigint,
    ff_iattr06  bigint,
    ff_iattr07  bigint,
    ff_iattr08  bigint,
    ff_iattr09  bigint,
    ff_iattr10  bigint,
    ff_iattr11  bigint,
    ff_iattr12  bigint,
    ff_iattr13  bigint,
    ff_iattr14  bigint,
    ff_iattr15  bigint,
    PRIMARY KEY (ff_c_id, ff_al_id),
    FOREIGN KEY (ff_c_id) REFERENCES customer (c_id),
    FOREIGN KEY (ff_al_id) REFERENCES airline (al_id)
);
CREATE INDEX idx_ff_customer_id ON frequent_flyer (ff_c_id_str);

--
-- FLIGHT
--
CREATE TABLE flight (
    f_id           varchar(128)                              NOT NULL,
    f_al_id        bigint                              NOT NULL,
    f_depart_ap_id bigint                              NOT NULL,
    f_depart_time  timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    f_arrive_ap_id bigint                              NOT NULL,
    f_arrive_time  timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
    f_status       bigint                              NOT NULL,
    f_base_price   float                               NOT NULL,
    f_seats_total  bigint                              NOT NULL,
    f_seats_left   bigint                              NOT NULL,
    f_iattr00      bigint,
    f_iattr01      bigint,
    f_iattr02      bigint,
    f_iattr03      bigint,
    f_iattr04      bigint,
    f_iattr05      bigint,
    f_iattr06      bigint,
    f_iattr07      bigint,
    f_iattr08      bigint,
    f_iattr09      bigint,
    f_iattr10      bigint,
    f_iattr11      bigint,
    f_iattr12      bigint,
    f_iattr13      bigint,
    f_iattr14      bigint,
    f_iattr15      bigint,
    f_iattr16      bigint,
    f_iattr17      bigint,
    f_iattr18      bigint,
    f_iattr19      bigint,
    f_iattr20      bigint,
    f_iattr21      bigint,
    f_iattr22      bigint,
    f_iattr23      bigint,
    f_iattr24      bigint,
    f_iattr25      bigint,
    f_iattr26      bigint,
    f_iattr27      bigint,
    f_iattr28      bigint,
    f_iattr29      bigint,
    PRIMARY KEY (f_id),
    FOREIGN KEY (f_al_id) REFERENCES airline (al_id),
    FOREIGN KEY (f_depart_ap_id) REFERENCES airport (ap_id),
    FOREIGN KEY (f_arrive_ap_id) REFERENCES airport (ap_id)
);
CREATE INDEX f_depart_time_idx ON flight (f_depart_time);

--
-- RESERVATION
--
CREATE TABLE reservation (
    r_id      bigint NOT NULL,
    r_c_id    varchar(128) NOT NULL,
    r_f_id    varchar(128) NOT NULL,
    r_seat    bigint NOT NULL,
    r_price   float  NOT NULL,
    r_iattr00 bigint,
    r_iattr01 bigint,
    r_iattr02 bigint,
    r_iattr03 bigint,
    r_iattr04 bigint,
    r_iattr05 bigint,
    r_iattr06 bigint,
    r_iattr07 bigint,
    r_iattr08 bigint,
    UNIQUE (r_f_id, r_seat),
    PRIMARY KEY (r_id, r_c_id, r_f_id),
    FOREIGN KEY (r_c_id) REFERENCES customer (c_id),
    FOREIGN KEY (r_f_id) REFERENCES flight (f_id)
);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;