-- WARNING!!!
-- All of the indexes were removed for the 15-799 project. Do not use this for real benchmarking!
-- https://15799.courses.cs.cmu.edu/spring2022/project1.html

DROP TABLE IF EXISTS review CASCADE;
DROP TABLE IF EXISTS review_rating CASCADE;
DROP TABLE IF EXISTS trust CASCADE;
DROP TABLE IF EXISTS useracct CASCADE;
DROP TABLE IF EXISTS item CASCADE;

CREATE TABLE useracct (
    u_id int NOT NULL,
    name varchar(128) NOT NULL,
    email varchar(128) NOT NULL,
    creation_date timestamp DEFAULT NULL
);

CREATE TABLE item (
    i_id  int NOT NULL,
    title varchar(128) NOT NULL,
    description varchar(512) DEFAULT NULL,
    creation_date timestamp DEFAULT NULL
);

CREATE TABLE review (
    a_id   int NOT NULL,
    u_id   int NOT NULL,
    i_id   int NOT NULL,
    rating int DEFAULT NULL,
    rank   int DEFAULT NULL,
    comment varchar(256) DEFAULT NULL,
    creation_date timestamp DEFAULT NULL
);

CREATE TABLE review_rating (
    u_id          int NOT NULL,
    a_id          int NOT NULL,
    rating        int NOT NULL,
    status        int NOT NULL,
    creation_date timestamp DEFAULT NULL,
    last_mod_date timestamp DEFAULT NULL,
    type          int       DEFAULT NULL,
    vertical_id   int       DEFAULT NULL
);

CREATE TABLE trust (
    source_u_id   int NOT NULL,
    target_u_id   int NOT NULL,
    trust         int NOT NULL,
    creation_date timestamp DEFAULT NULL
);
