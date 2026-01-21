DROP TABLE IF EXISTS LOGS;

CREATE TABLE LOGS (
    log_id       BIGINT      NOT NULL,
    log_timestamp TIMESTAMP   NOT NULL,
    message      VARCHAR(255) NOT NULL,
    worker_id    INTEGER     NOT NULL,
    CONSTRAINT pk_logs PRIMARY KEY (log_id)
);

CREATE INDEX IDX_LOGS_TIMESTAMP ON LOGS (log_timestamp);
CREATE INDEX IDX_LOGS_WORKER_ID ON LOGS (worker_id); 