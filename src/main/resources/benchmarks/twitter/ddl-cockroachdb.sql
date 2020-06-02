-- DDL Script converted from Mysql Twitter dump

DROP TABLE IF EXISTS user_profiles CASCADE;
DROP TABLE IF EXISTS followers CASCADE;
DROP TABLE IF EXISTS follows CASCADE;
DROP TABLE IF EXISTS tweets CASCADE;
DROP TABLE IF EXISTS added_tweets CASCADE;

CREATE TABLE user_profiles (
    uid          int NOT NULL DEFAULT '0',
    name         varchar(255) DEFAULT NULL,
    email        varchar(255) DEFAULT NULL,
    partitionid  int          DEFAULT NULL,
    partitionid2 smallint     DEFAULT NULL,
    followers    int          DEFAULT NULL,
    PRIMARY KEY (uid)
);
CREATE INDEX idx_user_followers ON user_profiles (followers);
CREATE INDEX idx_user_partition ON user_profiles (partitionid);

CREATE TABLE followers (
    f1 int NOT NULL DEFAULT '0' REFERENCES user_profiles (uid),
    f2 int NOT NULL DEFAULT '0' REFERENCES user_profiles (uid),
    PRIMARY KEY (f1, f2)
);

CREATE TABLE follows (
    f1 int NOT NULL DEFAULT '0',
    f2 int NOT NULL DEFAULT '0',
    PRIMARY KEY (f1, f2)
);

-- TODO: id AUTO_INCREMENT

CREATE TABLE tweets (
    id         bigint    NOT NULL,
    uid        int       NOT NULL REFERENCES user_profiles (uid),
    text       char(140) NOT NULL,
    createdate timestamp DEFAULT NULL,
    PRIMARY KEY (id)
);

-- TODO: id AUTO_INCREMENT

CREATE TABLE added_tweets (
    id         serial,
    uid        int       NOT NULL REFERENCES user_profiles (uid),
    text       char(140) NOT NULL,
    createdate timestamp DEFAULT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX idx_added_tweets_uid ON added_tweets (uid);
