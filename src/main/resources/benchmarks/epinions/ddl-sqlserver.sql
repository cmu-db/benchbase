-- Drop tables
IF OBJECT_ID('[trust]') IS NOT NULL DROP table [dbo].[trust];
IF OBJECT_ID('[review_rating]') IS NOT NULL DROP table [dbo].[review_rating];
IF OBJECT_ID('[review]') IS NOT NULL DROP table [dbo].[review];
IF OBJECT_ID('[item]') IS NOT NULL DROP table [dbo].[item];
IF OBJECT_ID('[useracct]') IS NOT NULL DROP table [dbo].[useracct];

-- Create table

CREATE TABLE [useracct] (
  u_id int NOT NULL,
  name varchar(128) NOT NULL,
  email varchar(128) NOT NULL,
  creation_date datetime DEFAULT NULL,
  PRIMARY KEY (u_id)
);

CREATE TABLE [item] (
  i_id int NOT NULL,
  title varchar(128) NOT NULL,
  description varchar(512) DEFAULT NULL,
  creation_date datetime DEFAULT NULL,
  PRIMARY KEY (i_id)
);

CREATE TABLE [review] (
  a_id int NOT NULL,
  u_id int NOT NULL REFERENCES [useracct] (u_id),
  i_id int NOT NULL REFERENCES [item] (i_id),
  rating int DEFAULT NULL,
  rank int DEFAULT NULL,
  comment varchar(256) DEFAULT NULL,
  creation_date datetime DEFAULT NULL
);

CREATE TABLE [review_rating] (
  u_id int NOT NULL REFERENCES [useracct] (u_id),
  a_id int NOT NULL,
  rating int NOT NULL,
  status int NOT NULL,
  creation_date datetime DEFAULT NULL,
  last_mod_date datetime DEFAULT NULL,
  type int DEFAULT NULL,
  vertical_id int DEFAULT NULL
);

CREATE TABLE [trust] (
  source_u_id int NOT NULL REFERENCES [useracct] (u_id),
  target_u_id int NOT NULL REFERENCES [useracct] (u_id),
  trust int NOT NULL,
  creation_date datetime DEFAULT NULL
);

-- Create index
CREATE INDEX IDX_RATING_UID ON review (u_id);
CREATE INDEX IDX_RATING_AID ON review (a_id);
CREATE INDEX IDX_RATING_IID ON review (i_id);
CREATE INDEX IDX_REVIEW_RATING_UID ON review_rating (u_id);
CREATE INDEX IDX_REVIEW_RATING_AID ON review_rating (a_id);
CREATE INDEX IDX_TRUST_SID ON trust (source_u_id);
CREATE INDEX IDX_TRUST_TID ON trust (target_u_id);

