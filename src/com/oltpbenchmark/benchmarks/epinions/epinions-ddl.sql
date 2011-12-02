DROP TABLE IF EXISTS item;
CREATE TABLE item (
  i_id int(11) NOT NULL,
  title varchar(20) DEFAULT NULL,
  PRIMARY KEY (i_id)
);

DROP TABLE IF EXISTS review;
CREATE TABLE review (
  a_id int(11) NOT NULL,
  u_id int(11) NOT NULL REFERENCES usr (u_id),
  i_id int(11) NOT NULL REFERENCES item (i_id),
  rating int(11) DEFAULT NULL,
  rank int(11) DEFAULT NULL
);
CREATE INDEX IDX_RATING_UID ON review (u_id);
CREATE INDEX IDX_RATING_AID ON review (a_id);

DROP TABLE IF EXISTS review_rating;
CREATE TABLE review_rating (
  u_id int(11) NOT NULL REFERENCES usr (u_id),
  a_id int(11) NOT NULL,
  rating int(11) NOT NULL,
  status int(11) NOT NULL,
  creation_date datetime DEFAULT NULL,
  last_mod_date datetime DEFAULT NULL,
  type int(11) DEFAULT NULL,
  vertical_id int(11) DEFAULT NULL
);
CREATE INDEX IDX_REVIEW_RATING_UID ON review_rating (u_id);
CREATE INDEX IDX_REVIEW_RATING_AID ON review_rating (a_id);

DROP TABLE IF EXISTS trust;
CREATE TABLE trust (
  source_u_id int(11) NOT NULL REFERENCES usr (u_id),
  target_u_id int(11) NOT NULL REFERENCES usr (u_id),
  trust int(11) NOT NULL,
  creation_date datetime DEFAULT NULL
);

DROP TABLE IF EXISTS usr;
CREATE TABLE usr (
  u_id int(11) NOT NULL,
  name varchar(128) DEFAULT NULL,
  PRIMARY KEY (u_id)
);