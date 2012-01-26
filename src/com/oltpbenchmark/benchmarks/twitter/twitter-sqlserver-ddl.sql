-- Drop Exisiting Tables

IF OBJECT_ID('[followers]') IS NOT NULL DROP table [dbo].[followers];
IF OBJECT_ID('[follows]') IS NOT NULL DROP table [dbo].[follows];
IF OBJECT_ID('[tweets]') IS NOT NULL DROP table [dbo].[tweets];
IF OBJECT_ID('[added_tweets]') IS NOT NULL DROP table [dbo].[added_tweets];
IF OBJECT_ID('[user]') IS NOT NULL DROP table [dbo].[user];

-- Create Tables
CREATE TABLE [dbo].[user] (
  uid int NOT NULL,
  name varchar(255) DEFAULT NULL,
  email varchar(255) DEFAULT NULL,
  partitionid int DEFAULT NULL,
  partitionid2 tinyint DEFAULT NULL,
  followers int DEFAULT NULL,
  PRIMARY KEY (uid)
);

CREATE TABLE [dbo].[followers] (
  f1 int NOT NULL,
  f2 int NOT NULL,
  PRIMARY KEY (f1,f2)
);

CREATE TABLE [dbo].[follows] (
  f1 int NOT NULL,
  f2 int NOT NULL,
  PRIMARY KEY (f1,f2)
);

-- TODO: id AUTO_INCREMENT
CREATE TABLE [dbo].[tweets] (
  id bigint NOT NULL,
  uid int NOT NULL REFERENCES [user] (uid),
  text char(140) NOT NULL,
  createdate datetime DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE [dbo].[added_tweets] (
  id bigint NOT NULL identity(1,1),
  uid int NOT NULL REFERENCES [user] (uid),
  text char(140) NOT NULL,
  createdate datetime DEFAULT NULL,
  PRIMARY KEY (id)
);


-- Create Indexes
CREATE INDEX IDX_USER_FOLLOWERS ON [dbo].[user] (followers);
CREATE INDEX IDX_USER_PARTITION ON [dbo].[user] (partitionid);
CREATE INDEX IDX_TWEETS_UID ON [dbo].[tweets] (uid);
CREATE INDEX IDX_ADDED_TWEETS_UID ON [dbo].[added_tweets] (uid);