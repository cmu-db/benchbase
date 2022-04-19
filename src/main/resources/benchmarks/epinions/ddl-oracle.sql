-- Epinions DDL for Oracle DDL

-- Drop all tables

BEGIN EXECUTE IMMEDIATE 'DROP TABLE "review"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "review_rating"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "trust"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "item"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE "useracct"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

-- create tables

CREATE TABLE "useracct" (
  u_id number(11,0) NOT NULL,
  name varchar(128) NOT NULL,
  email varchar(128) NOT NULL,
  creation_date date DEFAULT NULL,
  PRIMARY KEY (u_id)
);

CREATE TABLE "item" (
  i_id number(11,0) NOT NULL,
  title varchar(128) NOT NULL,
  description varchar(512) DEFAULT NULL,
  creation_date date DEFAULT NULL,
  PRIMARY KEY (i_id)
);

CREATE TABLE "review" (
  a_id number(11,0) NOT NULL,
  u_id number(11,0) NOT NULL REFERENCES "useracct" (u_id),
  i_id number(11,0) NOT NULL REFERENCES "item" (i_id),
  rating number(11,0) DEFAULT NULL,
  rank number(11,0) DEFAULT NULL,
  comment varchar(256) DEFAULT NULL,
  creation_date date DEFAULT NULL
);

CREATE TABLE "review_rating" (
  u_id number(11,0) NOT NULL REFERENCES "useracct" (u_id),
  a_id number(11,0) NOT NULL,
  rating number(11,0) NOT NULL,
  status number(11,0) NOT NULL,
  creation_date date DEFAULT NULL,
  last_mod_date date DEFAULT NULL,
  type number(11,0) DEFAULT NULL,
  vertical_id number(11,0) DEFAULT NULL
);

CREATE TABLE "trust" (
  source_u_id number(11,0) NOT NULL REFERENCES "useracct" (u_id),
  target_u_id number(11,0) NOT NULL REFERENCES "useracct" (u_id),
  trust number(11,0) NOT NULL,
  creation_date date DEFAULT NULL
);

-- create indexes

CREATE INDEX IDX_REVIEW_RATING_UID ON "review_rating" (u_id);
CREATE INDEX IDX_REVIEW_RATING_AID ON "review_rating" (a_id);
CREATE INDEX IDX_TRUST_SID ON "trust" (source_u_id);
CREATE INDEX IDX_TRUST_TID ON "trust" (target_u_id);
CREATE INDEX IDX_RATING_UID ON "review" (u_id);
CREATE INDEX IDX_RATING_AID ON "review" (a_id);
CREATE INDEX IDX_RATING_IID ON "review" (i_id);

