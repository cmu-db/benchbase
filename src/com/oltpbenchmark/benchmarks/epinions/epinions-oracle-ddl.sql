-- Epinions DDL for Oracle DDL

DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM user_tables WHERE table_name = 'USER'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP TABLE "user" constraints'; END IF;END;;
CREATE TABLE "user" (
  u_id number(11,0) NOT NULL,
  name varchar(128) DEFAULT NULL,
  PRIMARY KEY (u_id)
);

DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM user_tables WHERE table_name = 'ITEM'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP TABLE item constraints'; END IF;END;;
CREATE TABLE item (
  i_id number(11,0) NOT NULL,
  title varchar(20) DEFAULT NULL,
  PRIMARY KEY (i_id)
);

DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM user_tables WHERE table_name = 'REVIEW'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP TABLE review'; END IF;END;;

CREATE TABLE review (
  a_id number(11,0) NOT NULL,
  u_id number(11,0) NOT NULL REFERENCES "user" (u_id),
  i_id number(11,0) NOT NULL REFERENCES item (i_id),
  rating number(11,0) DEFAULT NULL,
  rank number(11,0) DEFAULT NULL
);
CREATE INDEX IDX_RATING_UID ON review (u_id);
CREATE INDEX IDX_RATING_AID ON review (a_id);

DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM user_tables WHERE table_name = 'REVIEW_RATING'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP TABLE review_rating'; END IF;END;;
CREATE TABLE review_rating (
  u_id number(11,0) NOT NULL REFERENCES "user" (u_id),
  a_id number(11,0) NOT NULL,
  rating number(11,0) NOT NULL,
  status number(11,0) NOT NULL,
  creation_date date DEFAULT NULL,
  last_mod_date date DEFAULT NULL,
  type number(11,0) DEFAULT NULL,
  vertical_id number(11,0) DEFAULT NULL
);
CREATE INDEX IDX_REVIEW_RATING_UID ON review_rating (u_id);
CREATE INDEX IDX_REVIEW_RATING_AID ON review_rating (a_id);

DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM user_tables WHERE table_name = 'TRUST'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP TABLE trust'; END IF;END;;
CREATE TABLE trust (
  source_u_id number(11,0) NOT NULL REFERENCES "user" (u_id),
  target_u_id number(11,0) NOT NULL REFERENCES "user" (u_id),
  trust number(11,0) NOT NULL,
  creation_date date DEFAULT NULL
);


