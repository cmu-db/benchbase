SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS followers CASCADE;
DROP TABLE IF EXISTS follows CASCADE;
DROP TABLE IF EXISTS tweets CASCADE;
DROP TABLE IF EXISTS added_tweets CASCADE;
DROP TABLE IF EXISTS user_profiles CASCADE;

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
    f1 int NOT NULL,
    f2 int NOT NULL,
    FOREIGN KEY (f1) REFERENCES user_profiles (uid) ON DELETE CASCADE,
    FOREIGN KEY (f2) REFERENCES user_profiles (uid) ON DELETE CASCADE,
    PRIMARY KEY (f1, f2)
);

CREATE TABLE follows (
    f1 int NOT NULL,
    f2 int NOT NULL,
    FOREIGN KEY (f1) REFERENCES user_profiles (uid) ON DELETE CASCADE,
    FOREIGN KEY (f2) REFERENCES user_profiles (uid) ON DELETE CASCADE,
    PRIMARY KEY (f1, f2)
);

CREATE TABLE tweets (
    id         bigint    NOT NULL,
    uid        int       NOT NULL,
    text       char(140) NOT NULL,
    createdate datetime DEFAULT NULL,
    FOREIGN KEY (uid) REFERENCES user_profiles (uid) ON DELETE CASCADE,
    PRIMARY KEY (id)
);
CREATE INDEX idx_tweets_uid ON tweets (uid);

CREATE TABLE added_tweets (
    id         bigint    NOT NULL AUTO_INCREMENT,
    uid        int       NOT NULL,
    text       char(140) NOT NULL,
    createdate datetime DEFAULT NULL,
    FOREIGN KEY (uid) REFERENCES user_profiles (uid) ON DELETE CASCADE,
    PRIMARY KEY (id)
);
CREATE INDEX idx_added_tweets_uid ON added_tweets (uid);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;