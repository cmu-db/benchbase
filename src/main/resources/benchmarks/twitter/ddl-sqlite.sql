-- MySQL ddl from Twitter dump
-- Adapted for sqlite

DROP TABLE IF EXISTS added_tweets;
DROP TABLE IF EXISTS tweets;
DROP TABLE IF EXISTS followers;
DROP TABLE IF EXISTS follows;
DROP TABLE IF EXISTS user_profiles;

CREATE TABLE user_profiles
(
    uid          int NOT NULL,
    name         varchar(255) DEFAULT NULL,
    email        varchar(255) DEFAULT NULL,
    partitionid  int          DEFAULT NULL,
    partitionid2 tinyint      DEFAULT NULL,
    followers    int          DEFAULT NULL,
    PRIMARY KEY (uid)
);
CREATE INDEX IDX_USER_FOLLOWERS ON user_profiles (followers);
CREATE INDEX IDX_USER_PARTITION ON user_profiles (partitionid);

CREATE TABLE followers
(
    f1 int NOT NULL REFERENCES user_profiles (uid),
    f2 int NOT NULL REFERENCES user_profiles (uid),
    PRIMARY KEY (f1, f2)
);

CREATE TABLE follows
(
    f1 int NOT NULL REFERENCES user_profiles (uid),
    f2 int NOT NULL REFERENCES user_profiles (uid),
    PRIMARY KEY (f1, f2)
);

CREATE TABLE tweets
(
    id         bigint    NOT NULL,
    uid        int       NOT NULL REFERENCES user_profiles (uid),
    text       char(140) NOT NULL,
    createdate datetime DEFAULT NULL
);
CREATE INDEX IDX_TWEETS_UID ON tweets (uid);

CREATE TABLE added_tweets
(
    id         INTEGER    PRIMARY KEY AUTOINCREMENT,
    uid        int       NOT NULL REFERENCES user_profiles (uid),
    text       char(140) NOT NULL,
    createdate datetime DEFAULT NULL
);
CREATE INDEX IDX_ADDED_TWEETS_UID ON added_tweets (uid);
