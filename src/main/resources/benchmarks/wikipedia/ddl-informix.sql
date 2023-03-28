-- TODO: ipb_id auto_increment

DROP TABLE IF EXISTS watchlist;
DROP TABLE IF EXISTS value_backup;
DROP TABLE IF EXISTS user_groups;
DROP TABLE IF EXISTS text;
DROP TABLE IF EXISTS revision;
DROP TABLE IF EXISTS recentchanges;
DROP TABLE IF EXISTS page_restrictions;
DROP TABLE IF EXISTS page_backup;
DROP TABLE IF EXISTS page;
DROP TABLE IF EXISTS logging;
DROP TABLE IF EXISTS useracct;
DROP TABLE IF EXISTS ipblocks;


CREATE TABLE ipblocks
(
    ipb_id               int     NOT NULL,
    ipb_address          text    NOT NULL,
    ipb_user             int     NOT NULL,
    ipb_by               int     NOT NULL,
    ipb_by_text          text    NOT NULL,
    ipb_reason           text    NOT NULL,
    ipb_timestamp        varchar(14) NOT NULL,
    ipb_auto             SMALLINT NOT NULL,
    ipb_anon_only        SMALLINT NOT NULL,
    ipb_create_account SMALLINT NOT NULL ,
    ipb_enable_autoblock SMALLINT NOT NULL ,
    ipb_expiry varchar(14) NOT NULL,
    ipb_range_start text NOT NULL,
    ipb_range_end text NOT NULL,
    ipb_deleted SMALLINT NOT NULL ,
    ipb_block_email SMALLINT NOT NULL ,
    ipb_allow_usertalk SMALLINT NOT NULL ,
    PRIMARY KEY (ipb_id),
    UNIQUE (ipb_address, ipb_user, ipb_auto, ipb_anon_only)
);

CREATE INDEX IDX_IPB_USER ON ipblocks (ipb_user);
CREATE INDEX IDX_IPB_RANGE ON ipblocks (ipb_range_start, ipb_range_end);
CREATE INDEX IDX_IPB_TIMESTAMP ON ipblocks (ipb_timestamp);
CREATE INDEX IDX_IPB_EXPIRY ON ipblocks (ipb_expiry);

-- TOOD: user_id auto_increment
CREATE TABLE useracct
(
    user_id                  int           NOT NULL,
    user_name                text  NOT NULL,
    user_real_name           text  NOT NULL,
    user_password            text  NOT NULL,
    user_newpassword         text NOT NULL,
    user_newpass_time        varchar(14) DEFAULT NULL,
    user_email               text NOT NULL,
    user_options             text NOT NULL,
    user_touched             varchar(14)   NOT NULL,
    user_token               char(32) NOT NULL,
    user_email_authenticated char(14)    DEFAULT NULL,
    user_email_token         char(32)    DEFAULT NULL,
    user_email_token_expires char(14)    DEFAULT NULL,
    user_registration        varchar(14) DEFAULT NULL,
    user_editcount           int         DEFAULT NULL,
    PRIMARY KEY (user_id),
    UNIQUE (user_name)
);
CREATE INDEX IDX_USER_EMAIL_TOKEN ON useracct (user_email_token);

-- TODO: log_id auto_increment
CREATE TABLE logging
(
    log_id        int   NOT NULL,
    log_type      varchar(32) NOT NULL,
    log_action    varchar(32) NOT NULL,
    log_timestamp varchar(14) NOT NULL,
    log_user      int   NOT NULL,
    log_namespace int   NOT NULL,
    log_title     text NOT NULL,
    log_comment   text NOT NULL,
    log_params    text NOT NULL,
    log_deleted SMALLINT NOT NULL,
    log_user_text text NOT NULL,
    log_page int DEFAULT NULL,
    PRIMARY KEY (log_id)
);
CREATE INDEX IDX_LOG_TYPE_TIME ON logging (log_type, log_timestamp);
CREATE INDEX IDX_LOG_USER_TIME ON logging (log_user, log_timestamp);
CREATE INDEX IDX_LOG_PAGE_TIME ON logging (log_namespace, log_title, log_timestamp);
CREATE INDEX IDX_LOG_TIMES ON logging (log_timestamp);
CREATE INDEX IDX_LOG_USER_TYPE_TIME ON logging (log_user, log_type, log_timestamp);
CREATE INDEX IDX_LOG_PAGE_ID_TIME ON logging (log_page, log_timestamp);

-- TODO: page_id auto_increment
CREATE TABLE page
(
    page_id           int           NOT NULL,
    page_namespace    int           NOT NULL,
    page_title        text  NOT NULL,
    page_restrictions text NOT NULL,
    page_counter      bigint        NOT NULL,
    page_is_redirect  SMALLINT       NOT NULL,
    page_is_new       SMALLINT       NOT NULL,
    page_random       double        NOT NULL,
    page_touched      varchar(14) NOT NULL,
    page_latest       int           NOT NULL,
    page_len          int           NOT NULL,
    PRIMARY KEY (page_id),
    UNIQUE (page_namespace, page_title)
);
CREATE INDEX IDX_PAGE_RANDOM ON page (page_random);
CREATE INDEX IDX_PAGE_LEN ON page (page_len);

-- TODO: page_id auto_increment
CREATE TABLE page_backup
(
    page_id           int           NOT NULL,
    page_namespace    int           NOT NULL,
    page_title        text  NOT NULL,
    page_restrictions text NOT NULL,
    page_counter      bigint        NOT NULL,
    page_is_redirect  SMALLINT       NOT NULL,
    page_is_new       SMALLINT       NOT NULL,
    page_random       double        NOT NULL,
    page_touched      varchar(14) NOT NULL,
    page_latest       int           NOT NULL,
    page_len          int           NOT NULL,
    PRIMARY KEY (page_id),
    UNIQUE (page_namespace, page_title)
);
CREATE INDEX IDX_PAGE_BACKUP_RANDOM ON page_backup (page_random);
CREATE INDEX IDX_PAGE_BACKUP_LEN ON page_backup (page_len);

CREATE TABLE page_restrictions
(
    pr_page    int     NOT NULL,
    pr_type    varchar(60) NOT NULL,
    pr_level   varchar(60) NOT NULL,
    pr_cascade SMALLINT NOT NULL,
    pr_user    int DEFAULT NULL,
    pr_expiry  varchar(14) DEFAULT NULL,
    pr_id      int     NOT NULL,
    PRIMARY KEY (pr_id),
    UNIQUE (pr_page, pr_type)
);
CREATE INDEX IDX_PR_TYPELEVEL ON page_restrictions (pr_type, pr_level);
CREATE INDEX IDX_PR_LEVEL ON page_restrictions (pr_level);
CREATE INDEX IDX_PR_CASCADE ON page_restrictions (pr_cascade);

-- TOOD: rc_id auto_increment
CREATE TABLE recentchanges
(
    rc_id           int     NOT NULL,
    rc_timestamp    varchar(14) NOT NULL,
    rc_cur_time     varchar(14) NOT NULL,
    rc_user         int     NOT NULL,
    rc_user_text    text NOT NULL,
    rc_namespace    int     NOT NULL,
    rc_title        text NOT NULL,
    rc_comment      text NOT NULL,
    rc_minor        SMALLINT NOT NULL,
  rc_bot SMALLINT NOT NULL,
  rc_new SMALLINT NOT NULL,
  rc_cur_id int NOT NULL,
  rc_this_oldid int NOT NULL,
  rc_last_oldid int NOT NULL,
  rc_type SMALLINT NOT NULL,
  rc_moved_to_ns SMALLINT NOT NULL,
  rc_moved_to_title text NOT NULL,
  rc_patrolled SMALLINT NOT NULL,
  rc_ip varchar(40) NOT NULL,
  rc_old_len int DEFAULT NULL,
  rc_new_len int DEFAULT NULL,
  rc_deleted SMALLINT NOT NULL,
  rc_logid int NOT NULL,
  rc_log_type text DEFAULT NULL,
  rc_log_action text DEFAULT NULL,
  rc_params text,
  PRIMARY KEY (rc_id)
);
CREATE INDEX IDX_RC_TIMESTAMP ON recentchanges (rc_timestamp);
CREATE INDEX IDX_RC_NAMESPACE_TITLE ON recentchanges (rc_namespace, rc_title);
CREATE INDEX IDX_RC_CUR_ID ON recentchanges (rc_cur_id);
CREATE INDEX IDX_NEW_NAME_TIMESTAMP ON recentchanges (rc_new, rc_namespace, rc_timestamp);
CREATE INDEX IDX_RC_IP ON recentchanges (rc_ip);
CREATE INDEX IDX_RC_NS_USERTEXT ON recentchanges (rc_namespace, rc_user_text);
CREATE INDEX IDX_RC_USER_TEXT ON recentchanges (rc_user_text, rc_timestamp);

-- TODO: rev_id auto_increment
CREATE TABLE revision
(
    rev_id         int           NOT NULL,
    rev_page       int           NOT NULL,
    rev_text_id    int           NOT NULL,
    rev_comment    text NOT NULL,
    rev_user       int           NOT NULL,
    rev_user_text  text  NOT NULL,
    rev_timestamp  varchar(14) NOT NULL,
    rev_minor_edit SMALLINT       NOT NULL,
    rev_deleted    SMALLINT       NOT NULL,
    rev_len        int DEFAULT NULL,
    rev_parent_id  int DEFAULT NULL,
    PRIMARY KEY (rev_id),
    UNIQUE (rev_page, rev_id)
);
CREATE INDEX IDX_REV_TIMESTAMP ON revision (rev_timestamp);
CREATE INDEX IDX_PAGE_TIMESTAMP ON revision (rev_page, rev_timestamp);
CREATE INDEX IDX_USER_TIMESTAMP ON revision (rev_user, rev_timestamp);
CREATE INDEX IDX_USERTEXT_TIMESTAMP ON revision (rev_user_text, rev_timestamp);

-- TODO old_id auto_increment
CREATE TABLE text
(
    old_id    int           NOT NULL,
    old_text  TEXT          NOT NULL,
    old_flags text NOT NULL,
    old_page  int DEFAULT NULL,
    PRIMARY KEY (old_id)
);

CREATE TABLE user_groups
(
    ug_user  int NOT NULL REFERENCES useracct (user_id),
    ug_group varchar(16) NOT NULL,
    UNIQUE (ug_user, ug_group)
);
CREATE INDEX IDX_UG_GROUP ON user_groups (ug_group);

CREATE TABLE value_backup
(
    table_name text DEFAULT NULL,
    maxid      int          DEFAULT NULL
);

CREATE TABLE watchlist
(
    wl_user                  int          NOT NULL,
    wl_namespace             int          NOT NULL,
    wl_title                 text NOT NULL,
    wl_notificationtimestamp varchar(14) DEFAULT NULL,
    UNIQUE (wl_user, wl_namespace, wl_title)
);
CREATE INDEX IDX_WL_NAMESPACE_TITLE ON watchlist (wl_namespace, wl_title);
