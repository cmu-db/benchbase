SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP VIEW IF EXISTS v_votes_by_phone_number CASCADE;
DROP VIEW IF EXISTS v_votes_by_contestant_number_state CASCADE;
DROP TABLE IF EXISTS votes CASCADE;
DROP TABLE IF EXISTS area_code_state CASCADE;
DROP TABLE IF EXISTS contestants CASCADE;

CREATE TABLE contestants (
    contestant_number integer     NOT NULL,
    contestant_name   varchar(50) NOT NULL,
    PRIMARY KEY (contestant_number)
);

CREATE TABLE area_code_state (
    area_code smallint   NOT NULL,
    state     varchar(2) NOT NULL,
    PRIMARY KEY (area_code)
);

CREATE TABLE votes (
    vote_id           bigint     NOT NULL,
    phone_number      bigint     NOT NULL,
    state             varchar(2) NOT NULL,
    contestant_number integer    NOT NULL,
    created           timestamp  NOT NULL,
    FOREIGN KEY (contestant_number) REFERENCES contestants (contestant_number) ON DELETE CASCADE
);
CREATE INDEX idx_votes_phone_number ON votes (phone_number);

CREATE VIEW v_votes_by_phone_number (phone_number, num_votes) AS
SELECT phone_number, COUNT(*)
FROM votes
GROUP BY phone_number;

CREATE VIEW v_votes_by_contestant_number_state (contestant_number, state, num_votes) AS
SELECT contestant_number, state, COUNT(*)
FROM votes
GROUP BY contestant_number, state;

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;