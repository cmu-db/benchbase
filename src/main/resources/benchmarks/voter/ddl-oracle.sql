BEGIN EXECUTE IMMEDIATE 'DROP VIEW v_votes_by_phone_number CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP VIEW v_votes_by_contestant_number_state CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE votes CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE area_code_state CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE contestants CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

CREATE TABLE contestants (
    contestant_number INT     NOT NULL,
    contestant_name   VARCHAR(50) NOT NULL,
    PRIMARY KEY (contestant_number)
);

CREATE TABLE area_code_state (
    area_code INT   NOT NULL,
    state     VARCHAR(2) NOT NULL,
    PRIMARY KEY (area_code)
);

CREATE TABLE votes (
    vote_id           NUMBER(19)     NOT NULL,
    phone_number      NUMBER(19)    NOT NULL,
    state             VARCHAR(2) NOT NULL,
    contestant_number INT    NOT NULL,
    created           TIMESTAMP(9) NOT NULL,
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