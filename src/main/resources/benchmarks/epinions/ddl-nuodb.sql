DROP TABLE IF EXISTS useracct CASCADE;
CREATE TABLE useracct (
  u_id int NOT NULL,
  name string NOT NULL,
  email string NOT NULL,
  creation_date datetime DEFAULT NULL,
  PRIMARY KEY (u_id)
);

DROP TABLE IF EXISTS item CASCADE;
CREATE TABLE item (
  i_id int NOT NULL,
  title string NOT NULL,
  description string DEFAULT NULL,
  creation_date datetime DEFAULT NULL,
  PRIMARY KEY (i_id)
);

DROP TABLE IF EXISTS review CASCADE;
CREATE TABLE review (
  a_id int NOT NULL,
  u_id int NOT NULL REFERENCES useracct (u_id),
  i_id int NOT NULL REFERENCES item (i_id),
  rating int DEFAULT NULL,
  rank int DEFAULT NULL,
  comment varchar(256) DEFAULT NULL,
  creation_date datetime DEFAULT NULL
);
CREATE INDEX IDX_RATING_UID ON review (u_id);
CREATE INDEX IDX_RATING_AID ON review (a_id);
CREATE INDEX IDX_RATING_IID ON review (i_id);

DROP TABLE IF EXISTS review_rating CASCADE;
CREATE TABLE review_rating (
  u_id int NOT NULL REFERENCES useracct (u_id),
  a_id int NOT NULL,
  rating int NOT NULL,
  status int NOT NULL,
  creation_date datetime DEFAULT NULL,
  last_mod_date datetime DEFAULT NULL,
  type int DEFAULT NULL,
  vertical_id int DEFAULT NULL
);
CREATE INDEX IDX_REVIEW_RATING_UID ON review_rating (u_id);
CREATE INDEX IDX_REVIEW_RATING_AID ON review_rating (a_id);

DROP TABLE IF EXISTS trust CASCADE;
CREATE TABLE trust (
  source_u_id int NOT NULL REFERENCES useracct (u_id),
  target_u_id int NOT NULL REFERENCES useracct (u_id),
  trust int NOT NULL,
  creation_date datetime DEFAULT NULL
);
CREATE INDEX IDX_TRUST_SID ON trust (source_u_id);
CREATE INDEX IDX_TRUST_TID ON trust (target_u_id);
