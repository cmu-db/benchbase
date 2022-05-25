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

CREATE TABLE useracct
(
    user_id                  int           NOT NULL,
    user_name                varchar(255)  NOT NULL,
    user_real_name           varchar(255)  NOT NULL,
    user_password            varchar(1024) NOT NULL,
    user_newpassword         varchar(1024) NOT NULL,
    user_newpass_time        varchar(14) DEFAULT NULL,
    user_email               varchar(1024) NOT NULL,
    user_options             varchar(1024) NOT NULL,
    user_touched             varchar(14)   NOT NULL,
    user_token               char(32)      NOT NULL,
    user_email_authenticated char(14)    DEFAULT NULL,
    user_email_token         char(32)    DEFAULT NULL,
    user_email_token_expires char(14)    DEFAULT NULL,
    user_registration        varchar(14) DEFAULT NULL,
    user_editcount           int         DEFAULT NULL,
    PRIMARY KEY (user_id),
    UNIQUE (user_name)
);
CREATE INDEX idx_user_email_token ON useracct (user_email_token);

CREATE TABLE user_groups
(
    ug_user  int         NOT NULL,
    ug_group varchar(16) NOT NULL,
    FOREIGN KEY (ug_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (ug_user, ug_group)
);
CREATE INDEX idx_ug_group ON user_groups (ug_group);

CREATE TABLE ipblocks
(
    ipb_id               int           NOT NULL,
    ipb_address          varchar(1024) NOT NULL,
    ipb_user             int           NOT NULL,
    ipb_by               int           NOT NULL,
    ipb_by_text          varchar(255)  NOT NULL,
    ipb_reason           varchar(1024) NOT NULL,
    ipb_timestamp        binary(14)    NOT NULL,
    ipb_auto             tinyint       NOT NULL,
    ipb_anon_only        tinyint       NOT NULL,
    ipb_create_account   tinyint       NOT NULL,
    ipb_enable_autoblock tinyint       NOT NULL,
    ipb_expiry           varchar(14)   NOT NULL,
    ipb_range_start      varchar(1024) NOT NULL,
    ipb_range_end        varchar(1024) NOT NULL,
    ipb_deleted          tinyint       NOT NULL,
    ipb_block_email      tinyint       NOT NULL,
    ipb_allow_usertalk   tinyint       NOT NULL,
    PRIMARY KEY (ipb_id),
    FOREIGN KEY (ipb_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    FOREIGN KEY (ipb_by) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (ipb_address, ipb_user, ipb_auto, ipb_anon_only)
);
CREATE INDEX idx_ipb_user ON ipblocks (ipb_user);
CREATE INDEX idx_ipb_range ON ipblocks (ipb_range_start, ipb_range_end);
CREATE INDEX idx_ipb_timestamp ON ipblocks (ipb_timestamp);
CREATE INDEX idx_ipb_expiry ON ipblocks (ipb_expiry);

CREATE TABLE logging
(
    log_id        int           IDENTITY NOT NULL,
    log_type      varchar(32)   NOT NULL,
    log_action    varchar(32)   NOT NULL,
    log_timestamp binary(14)    NOT NULL,
    log_user      int           NOT NULL,
    log_namespace int           NOT NULL,
    log_title     varchar(255)  NOT NULL,
    log_comment   varchar(255)  NOT NULL,
    log_params    varchar(1024) NOT NULL,
    log_deleted   tinyint       NOT NULL DEFAULT '0',
    log_user_text varchar(255)  NOT NULL,
    log_page      int DEFAULT NULL,
    FOREIGN KEY (log_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    PRIMARY KEY (log_id)
);
CREATE INDEX idx_log_type_time ON logging (log_type, log_timestamp);
CREATE INDEX idx_log_user_time ON logging (log_user, log_timestamp);
CREATE INDEX idx_log_page_time ON logging (log_namespace, log_title, log_timestamp);
CREATE INDEX idx_log_times ON logging (log_timestamp);
CREATE INDEX idx_log_user_type_time ON logging (log_user, log_type, log_timestamp);
CREATE INDEX idx_log_page_id_time ON logging (log_page, log_timestamp);

CREATE TABLE page
(
    page_id           int           NOT NULL,
    page_namespace    int           NOT NULL,
    page_title        varchar(255)  NOT NULL,
    page_restrictions varchar(1024) NOT NULL,
    page_counter      bigint        NOT NULL,
    page_is_redirect  tinyint       NOT NULL,
    page_is_new       tinyint       NOT NULL,
    page_random       double        NOT NULL,
    page_touched      binary(14)    NOT NULL,
    page_latest       int           NOT NULL,
    page_len          int           NOT NULL,
    PRIMARY KEY (page_id),
    UNIQUE (page_namespace, page_title)
);
CREATE INDEX idx_page_random ON page (page_random);
CREATE INDEX idx_page_len ON page (page_len);

CREATE TABLE page_restrictions
(
    pr_page    int         NOT NULL,
    pr_type    varchar(60) NOT NULL,
    pr_level   varchar(60) NOT NULL,
    pr_cascade tinyint     NOT NULL,
    pr_user    int         DEFAULT NULL,
    pr_expiry  varchar(14) DEFAULT NULL,
    pr_id      int         NOT NULL,
    PRIMARY KEY (pr_id),
    FOREIGN KEY (pr_page) REFERENCES page (page_id) ON DELETE CASCADE,
    UNIQUE (pr_page, pr_type)
);
CREATE INDEX idx_pr_typelevel ON page_restrictions (pr_type, pr_level);
CREATE INDEX idx_pr_level ON page_restrictions (pr_level);
CREATE INDEX idx_pr_cascade ON page_restrictions (pr_cascade);

CREATE TABLE recentchanges
(
    rc_id             int IDENTITY NOT NULL,
    rc_timestamp      varchar(14)  NOT NULL,
    rc_cur_time       varchar(14)  NOT NULL,
    rc_user           int          NOT NULL,
    rc_user_text      varchar(255) NOT NULL,
    rc_namespace      int          NOT NULL,
    rc_title          varchar(255) NOT NULL,
    rc_comment        varchar(255) NOT NULL,
    rc_minor          tinyint      NOT NULL,
    rc_bot            tinyint      NOT NULL,
    rc_new            tinyint      NOT NULL DEFAULT '0',
    rc_cur_id         int          NOT NULL,
    rc_this_oldid     int          NOT NULL,
    rc_last_oldid     int          NOT NULL,
    rc_type           tinyint      NOT NULL,
    rc_moved_to_ns    tinyint      NOT NULL,
    rc_moved_to_title varchar(255) NOT NULL,
    rc_patrolled      tinyint      NOT NULL DEFAULT '0',
    rc_ip             varchar(40)  NOT NULL,
    rc_old_len        int          DEFAULT NULL,
    rc_new_len        int          DEFAULT NULL,
    rc_deleted        tinyint      NOT NULL DEFAULT '0',
    rc_logid          int          NOT NULL DEFAULT '0',
    rc_log_type       varchar(255) DEFAULT NULL,
    rc_log_action     varchar(255) DEFAULT NULL,
    rc_params         varchar(1024),
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

CREATE TABLE revision
(
    rev_id         int IDENTITY  NOT NULL,
    rev_page       int           NOT NULL,
    rev_text_id    int           NOT NULL,
    rev_comment    varchar(1024) NOT NULL,
    rev_user       int           NOT NULL,
    rev_user_text  varchar(255)  NOT NULL,
    rev_timestamp  binary(14)    NOT NULL,
    rev_minor_edit tinyint       NOT NULL,
    rev_deleted    tinyint       NOT NULL,
    rev_len        int DEFAULT NULL,
    rev_parent_id  int DEFAULT NULL,
    PRIMARY KEY (rev_id),
    UNIQUE (rev_page, rev_id),
    FOREIGN KEY (rev_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    FOREIGN KEY (rev_page) REFERENCES page (page_id) ON DELETE CASCADE
);
CREATE INDEX idx_rev_timestamp ON revision (rev_timestamp);
CREATE INDEX idx_page_timestamp ON revision (rev_page, rev_timestamp);
CREATE INDEX idx_user_timestamp ON revision (rev_user, rev_timestamp);
CREATE INDEX idx_usertext_timestamp ON revision (rev_user_text, rev_timestamp);

CREATE TABLE text
(
    old_id    int IDENTITY  NOT NULL,
    old_text  text          NOT NULL,
    old_flags varchar(1024) NOT NULL,
    old_page  int DEFAULT NULL,
    PRIMARY KEY (old_id)
);

CREATE TABLE watchlist
(
    wl_user                  int          NOT NULL,
    wl_namespace             int          NOT NULL,
    wl_title                 varchar(255) NOT NULL,
    wl_notificationtimestamp varchar(14) DEFAULT NULL,
    FOREIGN KEY (wl_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (wl_user, wl_namespace, wl_title)
);
CREATE INDEX idx_wl_namespace_title ON watchlist (wl_namespace, wl_title);