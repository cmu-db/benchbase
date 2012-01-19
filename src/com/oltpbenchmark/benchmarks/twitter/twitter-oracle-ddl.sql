BEGIN EXECUTE IMMEDIATE 'DROP TABLE "added_tweets"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "tweets"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "user"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

CREATE TABLE "user" (
  uuid int NOT NULL,
  name varchar2(255) DEFAULT NULL,
  email varchar2(255) DEFAULT NULL,
  partitionid number(11,0) DEFAULT NULL,
  partitionid2 number(3,0) DEFAULT NULL,
  followers number(11,0) DEFAULT NULL,
  CONSTRAINT uid_key PRIMARY KEY (uuid)
);
CREATE INDEX IDX_USER_FOLLOWERS ON "user" (followers);
CREATE INDEX IDX_USER_PARTITION ON "user" (partitionid);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE "followers"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE "followers" (
  f1 int NOT NULL,
  f2 int NOT NULL,
  CONSTRAINT follower_key PRIMARY KEY (f2,f1)
);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE "follows"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE "follows" (
  f1 int NOT NULL,
  f2 int NOT NULL,
  CONSTRAINT follows_key PRIMARY KEY (f2,f1)
);

-- TODO: id AUTO_INCREMENT

CREATE TABLE "tweets" (
  id number(19,0) NOT NULL,
  uuid int NOT NULL REFERENCES "user" (uuid),
  text char(140) NOT NULL,
  createdate date DEFAULT NULL,
  CONSTRAINT tweetid_key PRIMARY KEY (id)
);
CREATE INDEX IDX_TWEETS_uuid ON "tweets" (uuid);

CREATE TABLE "added_tweets" (
  id number(19,0) NOT NULL,
  uuid int NOT NULL REFERENCES "user" (uuid),
  text char(140) NOT NULL,
  createdate date DEFAULT NULL,
  CONSTRAINT new_tweet_id PRIMARY KEY (id)
);
CREATE INDEX IDX_ADDED_TWEETS_uuid ON "added_tweets" (uuid);
BEGIN EXECUTE IMMEDIATE 'DROP sequence tweet_idseq'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
create sequence tweet_idseq start with 1 increment by 1 nomaxvalue; 