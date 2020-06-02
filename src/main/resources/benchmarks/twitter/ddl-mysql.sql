DROP TABLE IF EXISTS user_profiles;
DROP TABLE IF EXISTS followers;
DROP TABLE IF EXISTS follows;
DROP TABLE IF EXISTS tweets;
DROP TABLE IF EXISTS added_tweets;

CREATE TABLE user_profiles (
    uid          int NOT NULL,
    name         varchar(255) DEFAULT NULL,
    email        varchar(255) DEFAULT NULL,
    partitionid  int          DEFAULT NULL,
    partitionid2 tinyint      DEFAULT NULL,
    followers    int          DEFAULT NULL,
    PRIMARY KEY (uid)
);
CREATE INDEX idx_user_followers ON user_profiles (followers);
CREATE INDEX idx_user_partition ON user_profiles (partitionid);


CREATE TABLE followers (
    f1 int NOT NULL REFERENCES user_profiles (uid),
    f2 int NOT NULL REFERENCES user_profiles (uid),
    PRIMARY KEY (f1, f2)
);


CREATE TABLE follows (
    f1 int NOT NULL REFERENCES user_profiles (uid),
    f2 int NOT NULL REFERENCES user_profiles (uid),
    PRIMARY KEY (f1, f2)
);

CREATE TABLE tweets (
    id         bigint    NOT NULL,
    uid        int       NOT NULL REFERENCES user_profiles (uid),
    text       char(140) NOT NULL,
    createdate datetime DEFAULT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX idx_tweets_uid ON tweets (uid);


CREATE TABLE added_tweets (
    id         bigint    NOT NULL AUTO_INCREMENT,
    uid        int       NOT NULL REFERENCES user_profiles (uid),
    text       char(140) NOT NULL,
    createdate datetime DEFAULT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX idx_added_tweets_uid ON added_tweets (uid);
