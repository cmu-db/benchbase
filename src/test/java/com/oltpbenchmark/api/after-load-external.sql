DROP TABLE IF EXISTS extra_external CASCADE;

CREATE TABLE extra_external (
    extra_pk int NOT NULL,
    PRIMARY KEY (extra_pk)
);

INSERT INTO extra_external VALUES (1);
