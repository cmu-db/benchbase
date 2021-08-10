BEGIN EXECUTE IMMEDIATE 'DROP TABLE "added_tweets"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "tweets"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "user_profiles"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

CREATE TABLE "user_profiles" (
  uuid int NOT NULL,
  name varchar2(255) DEFAULT NULL,
  email varchar2(255) DEFAULT NULL,
  partitionid number(11,0) DEFAULT NULL,
  partitionid2 number(3,0) DEFAULT NULL,
  followers number(11,0) DEFAULT NULL,
  CONSTRAINT uid_key PRIMARY KEY (uuid)
);
CREATE INDEX IDX_USER_FOLLOWERS ON "user_profiles" (followers);
CREATE INDEX IDX_USER_PARTITION ON "user_profiles" (partitionid);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE "followers"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE "followers" (
  f1 int NOT NULL REFERENCES "user_profiles" (uid),
  f2 int NOT NULL REFERENCES "user_profiles" (uid),
  CONSTRAINT follower_key PRIMARY KEY (f1,f2)
);

BEGIN EXECUTE IMMEDIATE 'DROP TABLE "follows"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
CREATE TABLE "follows" (
  f1 int NOT NULL REFERENCES "user_profiles" (uid),
  f2 int NOT NULL REFERENCES "user_profiles" (uid),
  CONSTRAINT follows_key PRIMARY KEY (f1,f2)
);

-- TODO: id AUTO_INCREMENT

CREATE TABLE "tweets" (
  id number(19,0) NOT NULL,
  uuid int NOT NULL REFERENCES "user_profiles" (uuid),
  text char(140) NOT NULL,
  createdate date DEFAULT NULL,
  CONSTRAINT tweetid_key PRIMARY KEY (id)
);
CREATE INDEX IDX_TWEETS_uuid ON "tweets" (uuid);

CREATE TABLE "added_tweets" (
  id number(19,0) NOT NULL,
  uuid int NOT NULL REFERENCES "user_profiles" (uuid),
  text char(140) NOT NULL,
  createdate date DEFAULT NULL,
  CONSTRAINT new_tweet_id PRIMARY KEY (id)
);
CREATE INDEX IDX_ADDED_TWEETS_uuid ON "added_tweets" (uuid);


-- sequence
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'TWEET_IDSEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE TWEET_IDSEQ'; END IF; END;;
create sequence tweet_idseq start with 1 increment by 1 nomaxvalue; 