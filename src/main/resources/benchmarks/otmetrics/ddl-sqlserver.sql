DROP TABLE IF EXISTS observations;
DROP TABLE IF exists types;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF exists sources;

CREATE TABLE sources (
    id INTEGER NOT NULL,
    name VARCHAR(128) NOT NULL UNIQUE,
    comment varchar(256) DEFAULT NULL,
    created_time DATETIME2 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE types (
    id INTEGER NOT NULL,
    category INTEGER NOT NULL,
    value_type INTEGER NOT NULL,
    name VARCHAR(64) NOT NULL,
    comment varchar(256) DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE (category, name)
);

CREATE TABLE sessions (
    id INTEGER NOT NULL,
    source_id INTEGER NOT NULL REFERENCES sources (id),
    agent VARCHAR(32) NOT NULL,
    created_time DATETIME2 NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE observations (
  source_id INTEGER NOT NULL REFERENCES sources (id),
  session_id INTEGER NOT NULL REFERENCES sessions (id),
  type_id INTEGER NOT NULL REFERENCES types (id),
  value DOUBLE PRECISION NOT NULL,
  created_time DATETIME2 NOT NULL
);
CREATE INDEX idx_observations_source_session ON observations (source_id, session_id, type_id);