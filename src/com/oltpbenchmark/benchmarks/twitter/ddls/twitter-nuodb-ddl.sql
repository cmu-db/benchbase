-- MySQL ddl from Twitter dump

DROP TABLE IF EXISTS user_profiles CASCADE;
CREATE TABLE user_profiles (
  uid int NOT NULL,
  name string DEFAULT NULL,
  email string DEFAULT NULL,
  partitionid int DEFAULT NULL,
  partitionid2 int DEFAULT NULL,
  followers int DEFAULT NULL,
  PRIMARY KEY (uid)
);
CREATE INDEX IDX_USER_FOLLOWERS ON user_profiles (followers);
CREATE INDEX IDX_USER_PARTITION ON user_profiles (partitionid);

DROP TABLE IF EXISTS followers CASCADE;
CREATE TABLE followers (
  f1 int NOT NULL REFERENCES user_profiles (uid),
  f2 int NOT NULL REFERENCES user_profiles (uid),
  PRIMARY KEY (f1,f2)
);

DROP TABLE IF EXISTS follows CASCADE;
CREATE TABLE follows (
  f1 int NOT NULL REFERENCES user_profiles (uid),
  f2 int NOT NULL REFERENCES user_profiles (uid),
  PRIMARY KEY (f1,f2)
);

-- TODO: id AUTO_INCREMENT
DROP TABLE IF EXISTS tweets CASCADE;
CREATE TABLE tweets (
  id bigint NOT NULL,
  uid int NOT NULL REFERENCES user_profiles (uid),
  text string NOT NULL,
  createdate datetime DEFAULT NULL,
  PRIMARY KEY (id)
);
CREATE INDEX IDX_TWEETS_UID ON tweets (uid);

-- TODO: id auto_increment
DROP TABLE IF EXISTS added_tweets CASCADE;
CREATE TABLE added_tweets (
  id bigint generated always as identity,
  uid int NOT NULL REFERENCES user_profiles (uid),
  text string NOT NULL,
  createdate datetime DEFAULT NULL
--  PRIMARY KEY (id)
);
CREATE INDEX IDX_ADDED_TWEETS_UID ON added_tweets (uid);
