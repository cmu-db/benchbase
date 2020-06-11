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
    user_id                  serial,
    user_name                varchar(255) NOT NULL DEFAULT '',
    user_real_name           varchar(255) NOT NULL DEFAULT '',
    user_password            varchar(255) NOT NULL,
    user_newpassword         varchar(255) NOT NULL,
    user_newpass_time        varchar(14)           DEFAULT NULL,
    user_email               varchar(255) NOT NULL,
    user_options             varchar(255) NOT NULL,
    user_touched             varchar(14)  NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    user_token               varchar(32)  NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    user_email_authenticated varchar(14)           DEFAULT NULL,
    user_email_token         varchar(32)           DEFAULT NULL,
    user_email_token_expires varchar(14)           DEFAULT NULL,
    user_registration        varchar(14)           DEFAULT NULL,
    user_editcount           int                   DEFAULT NULL,
    PRIMARY KEY (user_id),
    UNIQUE (user_name)
);
CREATE INDEX idx_user_email_token ON useracct (user_email_token);

CREATE TABLE user_groups (
    ug_user  int         NOT NULL DEFAULT '0',
    ug_group varchar(16) NOT NULL DEFAULT '',
    FOREIGN KEY (ug_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (ug_user, ug_group)
);
CREATE INDEX idx_ug_group ON user_groups (ug_group);


CREATE TABLE ipblocks (
    ipb_id               serial,
    ipb_address          varchar     NOT NULL,
    ipb_user             int         NOT NULL DEFAULT '0',
    ipb_by               int         NOT NULL DEFAULT '0',
    ipb_by_text          varchar     NOT NULL DEFAULT '',
    ipb_reason           varchar     NOT NULL,
    ipb_timestamp        varchar(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    ipb_auto             smallint    NOT NULL DEFAULT '0',
    ipb_anon_only        smallint    NOT NULL DEFAULT '0',
    ipb_create_account   smallint    NOT NULL DEFAULT '1',
    ipb_enable_autoblock smallint    NOT NULL DEFAULT '1',
    ipb_expiry           varchar(14) NOT NULL DEFAULT '',
    ipb_range_start      varchar     NOT NULL,
    ipb_range_end        varchar     NOT NULL,
    ipb_deleted          smallint    NOT NULL DEFAULT '0',
    ipb_block_email      smallint    NOT NULL DEFAULT '0',
    ipb_allow_usertalk   smallint    NOT NULL DEFAULT '0',
    PRIMARY KEY (ipb_id),
    FOREIGN KEY (ipb_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    FOREIGN KEY (ipb_by) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (ipb_address, ipb_user, ipb_auto, ipb_anon_only)
);
CREATE INDEX idx_ipb_user ON ipblocks (ipb_user);
CREATE INDEX idx_ipb_range ON ipblocks (ipb_range_start, ipb_range_end);
CREATE INDEX idx_ipb_timestamp ON ipblocks (ipb_timestamp);
CREATE INDEX idx_ipb_expiry ON ipblocks (ipb_expiry);

CREATE TABLE logging (
    log_id        serial,
    log_type      varchar(32)  NOT NULL,
    log_action    varchar(32)  NOT NULL,
    log_timestamp varchar(14)  NOT NULL DEFAULT '19700101000000',
    log_user      int          NOT NULL DEFAULT '0',
    log_namespace int          NOT NULL DEFAULT '0',
    log_title     varchar(255) NOT NULL DEFAULT '',
    log_comment   varchar(255) NOT NULL DEFAULT '',
    log_params    varchar(255) NOT NULL,
    log_deleted   smallint     NOT NULL DEFAULT '0',
    log_user_text varchar(255) NOT NULL DEFAULT '',
    log_page      int                   DEFAULT NULL,
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
    page_id           serial,
    page_namespace    int              NOT NULL,
    page_title        varchar          NOT NULL,
    page_restrictions varchar(255)     NOT NULL,
    page_counter      bigint           NOT NULL DEFAULT '0',
    page_is_redirect  smallint         NOT NULL DEFAULT '0',
    page_is_new       smallint         NOT NULL DEFAULT '0',
    page_random       double precision NOT NULL,
    page_touched      varchar(14)      NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    page_latest       int              NOT NULL,
    page_len          int              NOT NULL,
    PRIMARY KEY (page_id),
    UNIQUE (page_namespace, page_title)
);
CREATE INDEX idx_page_random ON page (page_random);
CREATE INDEX idx_page_len ON page (page_len);

CREATE TABLE page_restrictions (
    pr_id      int         NOT NULL,
    pr_page    int         NOT NULL,
    pr_type    varchar(60) NOT NULL,
    pr_level   varchar(60) NOT NULL,
    pr_cascade smallint    NOT NULL,
    pr_user    int         DEFAULT NULL,
    pr_expiry  varchar(14) DEFAULT NULL,
    PRIMARY KEY (pr_id),
    FOREIGN KEY (pr_page) REFERENCES page (page_id) ON DELETE CASCADE,
    UNIQUE (pr_page, pr_type)
);
CREATE INDEX idx_pr_typelevel ON page_restrictions (pr_type, pr_level);
CREATE INDEX idx_pr_level ON page_restrictions (pr_level);
CREATE INDEX idx_pr_cascade ON page_restrictions (pr_cascade);

CREATE TABLE recentchanges (
    rc_id             serial,
    rc_timestamp      varchar(14)  NOT NULL DEFAULT '',
    rc_cur_time       varchar(14)  NOT NULL DEFAULT '',
    rc_user           int          NOT NULL DEFAULT '0',
    rc_user_text      varchar(255) NOT NULL,
    rc_namespace      int          NOT NULL DEFAULT '0',
    rc_title          varchar(255) NOT NULL DEFAULT '',
    rc_comment        varchar(255) NOT NULL DEFAULT '',
    rc_minor          smallint     NOT NULL DEFAULT '0',
    rc_bot            smallint     NOT NULL DEFAULT '0',
    rc_new            smallint     NOT NULL DEFAULT '0',
    rc_cur_id         int          NOT NULL DEFAULT '0',
    rc_this_oldid     int          NOT NULL DEFAULT '0',
    rc_last_oldid     int          NOT NULL DEFAULT '0',
    rc_type           smallint     NOT NULL DEFAULT '0',
    rc_moved_to_ns    smallint     NOT NULL DEFAULT '0',
    rc_moved_to_title varchar(255) NOT NULL DEFAULT '',
    rc_patrolled      smallint     NOT NULL DEFAULT '0',
    rc_ip             varchar(40)  NOT NULL DEFAULT '',
    rc_old_len        int                   DEFAULT NULL,
    rc_new_len        int                   DEFAULT NULL,
    rc_deleted        smallint     NOT NULL DEFAULT '0',
    rc_logid          int          NOT NULL DEFAULT '0',
    rc_log_type       varchar(255)          DEFAULT NULL,
    rc_log_action     varchar(255)          DEFAULT NULL,
    rc_params         varchar(255),
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
    rev_id         serial,
    rev_page       int          NOT NULL,
    rev_text_id    int          NOT NULL,
    rev_comment    text         NOT NULL,
    rev_user       int          NOT NULL DEFAULT '0',
    rev_user_text  varchar(255) NOT NULL DEFAULT '',
    rev_timestamp  varchar(14)  NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    rev_minor_edit smallint     NOT NULL DEFAULT '0',
    rev_deleted    smallint     NOT NULL DEFAULT '0',
    rev_len        int                   DEFAULT NULL,
    rev_parent_id  int                   DEFAULT NULL,
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
    old_id    serial,
    old_text  text         NOT NULL,
    old_flags varchar(255) NOT NULL,
    old_page  int DEFAULT NULL,
    PRIMARY KEY (old_id)
);

CREATE TABLE watchlist (
    wl_user                  int          NOT NULL,
    wl_namespace             int          NOT NULL DEFAULT '0',
    wl_title                 varchar(255) NOT NULL DEFAULT '',
    wl_notificationtimestamp varchar(14)           DEFAULT NULL,
    FOREIGN KEY (wl_user) REFERENCES useracct (user_id) ON DELETE CASCADE,
    UNIQUE (wl_user, wl_namespace, wl_title)
);
CREATE INDEX idx_wl_namespace_title ON watchlist (wl_namespace, wl_title);