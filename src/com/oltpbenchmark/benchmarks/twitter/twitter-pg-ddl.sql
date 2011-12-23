DROP TABLE IF EXISTS added_tweets;
CREATE TABLE added_tweets (
  id bigint(20) NOT NULL,
  uid int NOT NULL REFERENCES userid (uid),
  text char(140) NOT NULL,
  createdate datetime DEFAULT NULL,
  PRIMARY KEY (id)
);
--CREATE INDEX IDX_ADDED_TWEETS_UID ON added_tweets (uid);

DROP TABLE IF EXISTS followers;
CREATE TABLE followers (
  f1 int NOT NULL DEFAULT '0',
  f2 int NOT NULL DEFAULT '0',
  PRIMARY KEY (f1,f2)
);

DROP TABLE IF EXISTS follows;
CREATE TABLE follows (
  f1 int NOT NULL DEFAULT '0',
  f2 int NOT NULL DEFAULT '0',
  PRIMARY KEY (f1,f2)
);

DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets (
  id bigint(20) NOT NULL,
  uid int NOT NULL REFERENCES usr (uid),
  text char(140) NOT NULL,
  createdate datetime DEFAULT NULL,
  PRIMARY KEY (id)
);

DROP TABLE IF EXISTS usr;
CREATE TABLE usr (
  uid int NOT NULL DEFAULT '0',
  name varchar(255) DEFAULT NULL,
  email varchar(255) DEFAULT NULL,
  partitionid int DEFAULT NULL,
  partitionid2 tinyint(4) DEFAULT NULL,
  followers int DEFAULT NULL,
  PRIMARY KEY (uid)
);

--CREATE INDEX IDX_USR_FOLLOWERS ON usr (followers);
--CREATE INDEX IDX_USR_PARTITION ON usr (partitionid);