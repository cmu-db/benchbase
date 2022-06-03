DROP VIEW IF EXISTS V_VOTES_BY_PHONE_NUMBER;
DROP VIEW IF EXISTS V_VOTES_BY_CONTESTANT_NUMBER_STATE;
DROP TABLE IF EXISTS VOTES;
DROP TABLE IF EXISTS AREA_CODE_STATE;
DROP TABLE IF EXISTS CONTESTANTS;

-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE CONTESTANTS
(
    contestant_number integer     NOT NULL,
    contestant_name   varchar(50) NOT NULL,
    PRIMARY KEY (contestant_number)
);

-- Map of Area Codes and States for geolocation classification of incoming calls
CREATE TABLE AREA_CODE_STATE
(
  area_code smallint   NOT NULL,
  state     varchar(2) NOT NULL,
  PRIMARY KEY ( area_code )
);

-- votes table holds every valid vote.
--   voters are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE VOTES
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL,
  state              varchar(2) NOT NULL,
  contestant_number  integer    NOT NULL REFERENCES CONTESTANTS (contestant_number),
  created            timestamp  NOT NULL
);
CREATE INDEX idx_votes_phone_number ON VOTES (phone_number);

-- rollup of votes by phone number, used to reject excessive voting
CREATE VIEW V_VOTES_BY_PHONE_NUMBER
(
  phone_number, num_votes
)
AS
   SELECT phone_number, COUNT(*)
     FROM VOTES
 GROUP BY phone_number;

-- rollup of votes by contestant and state for the heat map and results
CREATE VIEW V_VOTES_BY_CONTESTANT_NUMBER_STATE
(
  contestant_number, state, num_votes
)
AS
   SELECT contestant_number, state , COUNT(*)
     FROM VOTES
 GROUP BY contestant_number, state;
