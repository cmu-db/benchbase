DROP TABLE IF EXISTS user_profiles CASCADE;
CREATE TABLE user_profiles (
  uid int NOT NULL,
  name varchar(255) DEFAULT NULL,
  email varchar(255) DEFAULT NULL,
  partitionid int DEFAULT NULL,
  partitionid2 int DEFAULT NULL,
  followers int DEFAULT NULL,
  PRIMARY KEY (uid)
);
CREATE INDEX IDX_USER_FOLLOWERS ON user_profiles (followers);
CREATE INDEX IDX_USER_PARTITION ON user_profiles (partitionid);

DROP TABLE IF EXISTS followers;
CREATE TABLE followers (
  f1 int NOT NULL,
  f2 int NOT NULL,
  PRIMARY KEY (f1,f2)
);

DROP TABLE IF EXISTS follows;
CREATE TABLE follows (
  f1 int NOT NULL,
  f2 int NOT NULL,
  PRIMARY KEY (f1,f2)
);

DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets (
  id bigint NOT NULL,
  uid int NOT NULL,
  text char(140) NOT NULL,
  createdate date DEFAULT NULL,
  PRIMARY KEY (id)
);
CREATE INDEX IDX_TWEETS_UID ON tweets (uid);

DROP TABLE IF EXISTS added_tweets;
CREATE TABLE added_tweets (
  id bigint NOT NULL,
  uid int NOT NULL,
  text char(140) NOT NULL,
  createdate date DEFAULT NULL,
  PRIMARY KEY (id)
);
CREATE INDEX IDX_ADDED_TWEETS_UID ON added_tweets (uid);
