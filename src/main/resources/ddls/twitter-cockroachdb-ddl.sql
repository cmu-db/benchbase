-- DDL Script converted from Mysql Twitter dump

DROP TABLE IF EXISTS user_profiles CASCADE;
CREATE TABLE user_profiles (
  uid int NOT NULL DEFAULT '0',
  name varchar(255) DEFAULT NULL,
  email varchar(255) DEFAULT NULL,
  partitionid int DEFAULT NULL,
  partitionid2 smallint DEFAULT NULL,
  followers int DEFAULT NULL,
  PRIMARY KEY (uid)
);
CREATE INDEX IDX_USER_FOLLOWERS ON user_profiles (followers);
CREATE INDEX IDX_USER_PARTITION ON user_profiles (partitionid);

DROP TABLE IF EXISTS followers;
CREATE TABLE followers (
  f1 int NOT NULL DEFAULT '0' REFERENCES user_profiles (uid),
  f2 int NOT NULL DEFAULT '0' REFERENCES user_profiles (uid),
  PRIMARY KEY (f1,f2)
);

DROP TABLE IF EXISTS follows;
CREATE TABLE follows (
  f1 int NOT NULL DEFAULT '0',
  f2 int NOT NULL DEFAULT '0',
  PRIMARY KEY (f1,f2)
);

-- TODO: id AUTO_INCREMENT
DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets (
  id bigint NOT NULL,
  uid int NOT NULL REFERENCES user_profiles (uid),
  text char(140) NOT NULL,
  createdate timestamp DEFAULT NULL,
  PRIMARY KEY (id)
);

-- TODO: id AUTO_INCREMENT
DROP TABLE IF EXISTS added_tweets;
CREATE TABLE added_tweets (
  id serial,
  uid int NOT NULL REFERENCES user_profiles (uid),
  text char(140) NOT NULL,
  createdate timestamp DEFAULT NULL,
  PRIMARY KEY (id)
);
CREATE INDEX IDX_ADDED_TWEETS_UID ON added_tweets (uid);
