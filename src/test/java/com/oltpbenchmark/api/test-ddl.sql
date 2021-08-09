-- NOTE: The table names are purposely left as lower case so that we
--       can test where the Catalog loader can properly get the information

CREATE TABLE a
(
    A_ID      BIGINT             NOT NULL,
    A_ID_STR  VARCHAR(64) UNIQUE NOT NULL,
    A_BALANCE FLOAT              NOT NULL,
    A_SATTR00 VARCHAR(32),
    A_SATTR01 VARCHAR(16),
    A_SATTR02 VARCHAR(8),
    A_IATTR00 TINYINT,
    A_IATTR01 SMALLINT,
    A_IATTR02 INT,
    A_IATTR03 BIGINT,
    PRIMARY KEY (A_ID)
);

CREATE TABLE b
(
    B_ID          BIGINT    NOT NULL,
    B_DEPART_TIME TIMESTAMP NOT NULL,
    B_ARRIVE_TIME TIMESTAMP NOT NULL,
    B_STATUS      TINYINT   NOT NULL,
    B_BASE_PRICE  FLOAT     NOT NULL,
    PRIMARY KEY (B_ID)
);

CREATE TABLE c
(
    C_ID   BIGINT NOT NULL,
    C_A_ID BIGINT NOT NULL REFERENCES A (A_ID),
    C_B_ID BIGINT NOT NULL REFERENCES B (B_ID),
    C_SEAT BIGINT NOT NULL,
    UNIQUE (C_B_ID, C_SEAT),
    PRIMARY KEY (C_ID, C_A_ID, C_B_ID)
);