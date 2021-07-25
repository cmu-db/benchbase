SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS ipblocks CASCADE;
DROP TABLE IF EXISTS logging CASCADE;
DROP TABLE IF EXISTS recentchanges CASCADE;
DROP TABLE IF EXISTS revision CASCADE;
DROP TABLE IF EXISTS page_restrictions CASCADE;
DROP TABLE IF EXISTS page CASCADE;
DROP TABLE IF EXISTS text CASCADE;
DROP TABLE IF EXISTS watchlist CASCADE;
DROP TABLE IF EXISTS user_groups CASCADE;
DROP TABLE IF EXISTS useracct CASCADE;

CREATE TABLE useracct (
    user_id                  int            NOT NULL,
    user_name                varbinary(255) NOT NULL DEFAULT '',
    user_real_name           varbinary(255) NOT NULL DEFAULT '',
    user_password            tinyblob       NOT NULL,
    user_newpassword         tinyblob       NOT NULL,
    user_newpass_time        binary(14)              DEFAULT NULL,
    user_email               tinyblob       NOT NULL,
    user_options             blob           NOT NULL,
    user_touched             binary(14)     NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    user_token               binary(32)     NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    user_email_authenticated binary(14)              DEFAULT NULL,
    user_email_token         binary(32)              DEFAULT NULL,
    user_email_token_expires binary(14)              DEFAULT NULL,
    user_registration        binary(14)              DEFAULT NULL,
    user_editcount           int                     DEFAULT NULL,
    PRIMARY KEY (user_id),
    UNIQUE (user_name)
);
CREATE INDEX idx_user_email_token ON useracct (user_email_token);

CREATE TABLE user_groups (
    ug_user  int           NOT NULL DEFAULT '0',
    ug_group varbinary(16) NOT NULL DEFAULT '',
    FOREIGN KEY (ug_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (ug_user, ug_group)
);
CREATE INDEX idx_ug_group ON user_groups (ug_group);


CREATE TABLE ipblocks (
    ipb_id               int            NOT NULL AUTO_INCREMENT,
    ipb_address          tinyblob       NOT NULL,
    ipb_user             int            NOT NULL DEFAULT '0',
    ipb_by               int            NOT NULL DEFAULT '0',
    ipb_by_text          varbinary(255) NOT NULL DEFAULT '',
    ipb_reason           tinyblob       NOT NULL,
    ipb_timestamp        binary(14)     NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    ipb_auto             tinyint(1)     NOT NULL DEFAULT '0',
    ipb_anon_only        tinyint(1)     NOT NULL DEFAULT '0',
    ipb_create_account   tinyint(1)     NOT NULL DEFAULT '1',
    ipb_enable_autoblock tinyint(1)     NOT NULL DEFAULT '1',
    ipb_expiry           varbinary(14)  NOT NULL DEFAULT '',
    ipb_range_start      tinyblob       NOT NULL,
    ipb_range_end        tinyblob       NOT NULL,
    ipb_deleted          tinyint(1)     NOT NULL DEFAULT '0',
    ipb_block_email      tinyint(1)     NOT NULL DEFAULT '0',
    ipb_allow_usertalk   tinyint(1)     NOT NULL DEFAULT '0',
    PRIMARY KEY (ipb_id),
    FOREIGN KEY (ipb_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    FOREIGN KEY (ipb_by) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (ipb_address(255), ipb_user, ipb_auto, ipb_anon_only)
);
CREATE INDEX idx_ipb_user ON ipblocks (ipb_user);
CREATE INDEX idx_ipb_range ON ipblocks (ipb_range_start(8), ipb_range_end(8));
CREATE INDEX idx_ipb_timestamp ON ipblocks (ipb_timestamp);
CREATE INDEX idx_ipb_expiry ON ipblocks (ipb_expiry);

CREATE TABLE logging (
    log_id        int            NOT NULL AUTO_INCREMENT,
    log_type      varbinary(32)  NOT NULL,
    log_action    varbinary(32)  NOT NULL,
    log_timestamp binary(14)     NOT NULL DEFAULT '19700101000000',
    log_user      int            NOT NULL DEFAULT '0',
    log_namespace int            NOT NULL DEFAULT '0',
    log_title     varbinary(255) NOT NULL DEFAULT '',
    log_comment   varbinary(255) NOT NULL DEFAULT '',
    log_params    blob           NOT NULL,
    log_deleted   tinyint(3)     NOT NULL DEFAULT '0',
    log_user_text varbinary(255) NOT NULL DEFAULT '',
    log_page      int                     DEFAULT NULL,
    FOREIGN KEY (log_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    PRIMARY KEY (log_id)
);
CREATE INDEX idx_log_type_time ON logging (log_type, log_timestamp);
CREATE INDEX idx_log_user_time ON logging (log_user, log_timestamp);
CREATE INDEX idx_log_page_time ON logging (log_namespace, log_title, log_timestamp);
CREATE INDEX idx_log_times ON logging (log_timestamp);
CREATE INDEX idx_log_user_type_time ON logging (log_user, log_type, log_timestamp);
CREATE INDEX idx_log_page_id_time ON logging (log_page, log_timestamp);

CREATE TABLE page (
    page_id           int            NOT NULL AUTO_INCREMENT,
    page_namespace    int            NOT NULL,
    page_title        varbinary(255) NOT NULL,
    page_restrictions tinyblob       NOT NULL,
    page_counter      bigint(20)     NOT NULL DEFAULT '0',
    page_is_redirect  tinyint(3)     NOT NULL DEFAULT '0',
    page_is_new       tinyint(3)     NOT NULL DEFAULT '0',
    page_random       double         NOT NULL,
    page_touched      binary(14)     NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    page_latest       int            NOT NULL,
    page_len          int            NOT NULL,
    PRIMARY KEY (page_id),
    UNIQUE (page_namespace, page_title)
);
CREATE INDEX idx_page_random ON page (page_random);
CREATE INDEX idx_page_len ON page (page_len);

CREATE TABLE page_restrictions (
    pr_id      int           NOT NULL,
    pr_page    int           NOT NULL,
    pr_type    varbinary(60) NOT NULL,
    pr_level   varbinary(60) NOT NULL,
    pr_cascade tinyint(4)    NOT NULL,
    pr_user    int           DEFAULT NULL,
    pr_expiry  varbinary(14) DEFAULT NULL,
    PRIMARY KEY (pr_id),
    FOREIGN KEY (pr_page) REFERENCES page (page_id) ON DELETE CASCADE,
    UNIQUE (pr_page, pr_type)
);
CREATE INDEX idx_pr_typelevel ON page_restrictions (pr_type, pr_level);
CREATE INDEX idx_pr_level ON page_restrictions (pr_level);
CREATE INDEX idx_pr_cascade ON page_restrictions (pr_cascade);

CREATE TABLE recentchanges (
    rc_id             int            NOT NULL AUTO_INCREMENT,
    rc_timestamp      varbinary(14)  NOT NULL DEFAULT '',
    rc_cur_time       varbinary(14)  NOT NULL DEFAULT '',
    rc_user           int            NOT NULL DEFAULT '0',
    rc_user_text      varbinary(255) NOT NULL,
    rc_namespace      int            NOT NULL DEFAULT '0',
    rc_title          varbinary(255) NOT NULL DEFAULT '',
    rc_comment        varbinary(255) NOT NULL DEFAULT '',
    rc_minor          tinyint(3)     NOT NULL DEFAULT '0',
    rc_bot            tinyint(3)     NOT NULL DEFAULT '0',
    rc_new            tinyint(3)     NOT NULL DEFAULT '0',
    rc_cur_id         int            NOT NULL DEFAULT '0',
    rc_this_oldid     int            NOT NULL DEFAULT '0',
    rc_last_oldid     int            NOT NULL DEFAULT '0',
    rc_type           tinyint(3)     NOT NULL DEFAULT '0',
    rc_moved_to_ns    tinyint(3)     NOT NULL DEFAULT '0',
    rc_moved_to_title varbinary(255) NOT NULL DEFAULT '',
    rc_patrolled      tinyint(3)     NOT NULL DEFAULT '0',
    rc_ip             varbinary(40)  NOT NULL DEFAULT '',
    rc_old_len        int                     DEFAULT NULL,
    rc_new_len        int                     DEFAULT NULL,
    rc_deleted        tinyint(3)     NOT NULL DEFAULT '0',
    rc_logid          int            NOT NULL DEFAULT '0',
    rc_log_type       varbinary(255)          DEFAULT NULL,
    rc_log_action     varbinary(255)          DEFAULT NULL,
    rc_params         blob,
    FOREIGN KEY (rc_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    FOREIGN KEY (rc_cur_id) REFERENCES page (page_id) ON DELETE CASCADE,
    PRIMARY KEY (rc_id)
);
CREATE INDEX idx_rc_timestamp ON recentchanges (rc_timestamp);
CREATE INDEX idx_rc_namespace_title ON recentchanges (rc_namespace, rc_title);
CREATE INDEX idx_rc_cur_id ON recentchanges (rc_cur_id);
CREATE INDEX idx_new_name_timestamp ON recentchanges (rc_new, rc_namespace, rc_timestamp);
CREATE INDEX idx_rc_ip ON recentchanges (rc_ip);
CREATE INDEX idx_rc_ns_usertext ON recentchanges (rc_namespace, rc_user_text);
CREATE INDEX idx_rc_user_text ON recentchanges (rc_user_text, rc_timestamp);

CREATE TABLE revision (
    rev_id         int            NOT NULL AUTO_INCREMENT,
    rev_page       int            NOT NULL,
    rev_text_id    int            NOT NULL,
    rev_comment    varchar(1024)  NOT NULL,
    rev_user       int            NOT NULL DEFAULT '0',
    rev_user_text  varbinary(255) NOT NULL DEFAULT '',
    rev_timestamp  binary(14)     NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    rev_minor_edit tinyint(3)     NOT NULL DEFAULT '0',
    rev_deleted    tinyint(3)     NOT NULL DEFAULT '0',
    rev_len        int                     DEFAULT NULL,
    rev_parent_id  int                     DEFAULT NULL,
    PRIMARY KEY (rev_id),
    UNIQUE (rev_page, rev_id),
    FOREIGN KEY (rev_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    FOREIGN KEY (rev_page) REFERENCES page (page_id) ON DELETE CASCADE
);
CREATE INDEX idx_rev_timestamp ON revision (rev_timestamp);
CREATE INDEX idx_page_timestamp ON revision (rev_page, rev_timestamp);
CREATE INDEX idx_user_timestamp ON revision (rev_user, rev_timestamp);
CREATE INDEX idx_usertext_timestamp ON revision (rev_user_text, rev_timestamp);

CREATE TABLE text (
    old_id    int        NOT NULL AUTO_INCREMENT,
    old_text  mediumblob NOT NULL,
    old_flags tinyblob   NOT NULL,
    old_page  int DEFAULT NULL,
    PRIMARY KEY (old_id)
);

CREATE TABLE watchlist (
    wl_user                  int            NOT NULL,
    wl_namespace             int            NOT NULL DEFAULT '0',
    wl_title                 varbinary(255) NOT NULL DEFAULT '',
    wl_notificationtimestamp varbinary(14)           DEFAULT NULL,
    FOREIGN KEY (wl_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (wl_user, wl_namespace, wl_title)
);
CREATE INDEX idx_wl_namespace_title ON watchlist (wl_namespace, wl_title);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;