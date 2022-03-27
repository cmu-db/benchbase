DROP TABLE IF EXISTS review CASCADE;
DROP TABLE IF EXISTS review_rating CASCADE;
DROP TABLE IF EXISTS trust CASCADE;
DROP TABLE IF EXISTS useracct CASCADE;
DROP TABLE IF EXISTS item CASCADE;

CREATE TABLE useracct (
    u_id int NOT NULL,
    name varchar(128) NOT NULL,
    email varchar(128) NOT NULL,
    creation_date timestamp DEFAULT NULL,
    PRIMARY KEY (u_id)
);

CREATE TABLE item (
    i_id  int NOT NULL,
    title varchar(128) NOT NULL,
    description varchar(512) DEFAULT NULL,
    creation_date timestamp DEFAULT NULL,
    PRIMARY KEY (i_id)
);

CREATE TABLE review (
    a_id   int NOT NULL,
    u_id   int NOT NULL,
    i_id   int NOT NULL,
    rating int DEFAULT NULL,
    rank   int DEFAULT NULL,
    comment varchar(256) DEFAULT NULL,
    creation_date timestamp DEFAULT NULL,
    FOREIGN KEY (u_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    FOREIGN KEY (i_id) REFERENCES item (i_id) ON DELETE CASCADE
);
CREATE INDEX idx_rating_uid ON review (u_id);
CREATE INDEX idx_rating_aid ON review (a_id);
CREATE INDEX idx_rating_iid ON review (i_id);

CREATE TABLE review_rating (
    u_id          int NOT NULL,
    a_id          int NOT NULL,
    rating        int NOT NULL,
    status        int NOT NULL,
    creation_date timestamp DEFAULT NULL,
    last_mod_date timestamp DEFAULT NULL,
    type          int       DEFAULT NULL,
    vertical_id   int       DEFAULT NULL,
    FOREIGN KEY (u_id) REFERENCES useracct (u_id) ON DELETE CASCADE
);
CREATE INDEX idx_review_rating_uid ON review_rating (u_id);
CREATE INDEX idx_review_rating_aid ON review_rating (a_id);

CREATE TABLE trust (
    source_u_id   int NOT NULL,
    target_u_id   int NOT NULL,
    trust         int NOT NULL,
    creation_date timestamp DEFAULT NULL,
    FOREIGN KEY (source_u_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    FOREIGN KEY (target_u_id) REFERENCES useracct (u_id) ON DELETE CASCADE
);
CREATE INDEX idx_trust_sid ON trust (source_u_id);
CREATE INDEX idx_trust_tid ON trust (target_u_id);
