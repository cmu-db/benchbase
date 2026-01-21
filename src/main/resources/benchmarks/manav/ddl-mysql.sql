SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS logs;

CREATE TABLE logs (
    log_id       bigint      NOT NULL AUTO_INCREMENT,
    log_timestamp timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    message      varchar(255) NOT NULL,
    worker_id    int         NOT NULL,
    CONSTRAINT pk_logs PRIMARY KEY (log_id)
);

CREATE INDEX idx_logs_timestamp ON logs (log_timestamp);
CREATE INDEX idx_logs_worker_id ON logs (worker_id);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS; 