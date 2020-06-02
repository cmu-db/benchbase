-- PostgreSQL DDL

DROP TABLE IF EXISTS useracct CASCADE;
CREATE TABLE useracct (
    u_id int NOT NULL,
    name varchar(128) DEFAULT NULL,
    PRIMARY KEY (u_id)
);

DROP TABLE IF EXISTS item CASCADE;
CREATE TABLE item (
    i_id  int NOT NULL,
    title varchar(20) DEFAULT NULL,
    PRIMARY KEY (i_id)
);

DROP TABLE IF EXISTS review;
CREATE TABLE review (
    a_id   int NOT NULL,
    u_id   int NOT NULL REFERENCES useracct (u_id),
    i_id   int NOT NULL REFERENCES item (i_id),
    rating int DEFAULT NULL,
    rank   int DEFAULT NULL
);
CREATE INDEX idx_rating_uid ON review (u_id);
CREATE INDEX idx_rating_aid ON review (a_id);
CREATE INDEX idx_rating_iid ON review (i_id);

DROP TABLE IF EXISTS review_rating;
CREATE TABLE review_rating (
    u_id          int NOT NULL REFERENCES useracct (u_id),
    a_id          int NOT NULL,
    rating        int NOT NULL,
    status        int NOT NULL,
    creation_date timestamp DEFAULT NULL,
    last_mod_date timestamp DEFAULT NULL,
    type          int       DEFAULT NULL,
    vertical_id   int       DEFAULT NULL
);
CREATE INDEX idx_review_rating_uid ON review_rating (u_id);
CREATE INDEX idx_review_rating_aid ON review_rating (a_id);

DROP TABLE IF EXISTS trust;
CREATE TABLE trust (
    source_u_id   int NOT NULL REFERENCES useracct (u_id),
    target_u_id   int NOT NULL REFERENCES useracct (u_id),
    trust         int NOT NULL,
    creation_date timestamp DEFAULT NULL
);
CREATE INDEX idx_trust_sid ON trust (source_u_id);
CREATE INDEX idx_trust_tid ON trust (target_u_id);
