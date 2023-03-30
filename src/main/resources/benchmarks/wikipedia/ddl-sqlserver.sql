-- Drop tables

IF OBJECT_ID('ipblocks') IS NOT NULL DROP table ipblocks;
IF OBJECT_ID('logging') IS NOT NULL DROP table logging;
IF OBJECT_ID('page') IS NOT NULL DROP table page;
IF OBJECT_ID('page_backup') IS NOT NULL DROP table page_backup;
IF OBJECT_ID('page_restrictions') IS NOT NULL DROP table page_restrictions;
IF OBJECT_ID('recentchanges') IS NOT NULL DROP table recentchanges;
IF OBJECT_ID('revision') IS NOT NULL DROP table revision;
IF OBJECT_ID('page_restrictions') IS NOT NULL DROP table page_restrictions;
IF OBJECT_ID('text') IS NOT NULL DROP table text;
IF OBJECT_ID('useracct') IS NOT NULL DROP table useracct;
IF OBJECT_ID('user_groups') IS NOT NULL DROP table user_groups;
IF OBJECT_ID('value_backup') IS NOT NULL DROP table value_backup;
IF OBJECT_ID('watchlist') IS NOT NULL DROP table watchlist;
IF OBJECT_ID('text_old_id_seq') IS NOT NULL DROP SEQUENCE text_old_id_seq;
IF OBJECT_ID('revision_rev_id_seq') IS NOT NULL DROP SEQUENCE revision_rev_id_seq;

-- Create tables

CREATE TABLE ipblocks (
  ipb_id int IDENTITY NOT NULL,
  ipb_address varchar(15) NOT NULL,
  ipb_user int NOT NULL DEFAULT '0',
  ipb_by int NOT NULL DEFAULT '0',
  ipb_by_text varchar(255) NOT NULL DEFAULT '',
  ipb_reason varchar(255) NOT NULL,
  ipb_timestamp varchar(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  ipb_auto tinyint NOT NULL DEFAULT '0',
  ipb_anon_only tinyint NOT NULL DEFAULT '0',
  ipb_create_account tinyint NOT NULL DEFAULT '1',
  ipb_enable_autoblock tinyint NOT NULL DEFAULT '1',
  ipb_expiry varchar(14) NOT NULL DEFAULT '',
  ipb_range_start varbinary NOT NULL,
  ipb_range_end varbinary NOT NULL,
  ipb_deleted tinyint NOT NULL DEFAULT '0',
  ipb_block_email tinyint NOT NULL DEFAULT '0',
  ipb_allow_usertalk tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (ipb_id),
  UNIQUE (ipb_address,ipb_user,ipb_auto,ipb_anon_only)
)
CREATE INDEX IDX_IPB_USER ON ipblocks (ipb_user);
CREATE INDEX IDX_IPB_RANGE ON ipblocks (ipb_range_start,ipb_range_end);
CREATE INDEX IDX_IPB_TIMESTAMP ON ipblocks (ipb_timestamp);
CREATE INDEX IDX_IPB_EXPIRY ON ipblocks (ipb_expiry);

CREATE TABLE logging (
  log_id int IDENTITY NOT NULL,
  log_type varchar(32) NOT NULL,
  log_action varchar(32) NOT NULL,
  log_timestamp varchar(14) NOT NULL DEFAULT '19700101000000',
  log_user int NOT NULL DEFAULT '0',
  log_namespace int NOT NULL DEFAULT '0',
  log_title varchar(255) NOT NULL DEFAULT '',
  log_comment varchar(255) NOT NULL DEFAULT '',
  log_params text NOT NULL,
  log_deleted tinyint NOT NULL DEFAULT '0',
  log_user_text varchar(255) NOT NULL DEFAULT '',
  log_page int DEFAULT NULL,
  PRIMARY KEY (log_id)
);
CREATE INDEX IDX_LOG_TYPE_TIME ON logging (log_type,log_timestamp);
CREATE INDEX IDX_LOG_USER_TIME ON logging (log_user,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_TIME ON logging (log_namespace,log_title,log_timestamp);
CREATE INDEX IDX_LOG_TIMES ON logging (log_timestamp);
CREATE INDEX IDX_LOG_USER_TYPE_TIME ON logging (log_user,log_type,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_ID_TIME ON logging (log_page,log_timestamp);

CREATE TABLE page (
  page_id int IDENTITY NOT NULL,
  page_namespace int NOT NULL,
  page_title varchar(255) NOT NULL,
  page_restrictions varchar(255) NOT NULL,
  page_counter bigint NOT NULL DEFAULT '0',
  page_is_redirect tinyint NOT NULL DEFAULT '0',
  page_is_new tinyint NOT NULL DEFAULT '0',
  page_random float NOT NULL,
  page_touched varchar(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id),
  UNIQUE (page_namespace,page_title)
);
CREATE INDEX IDX_PAGE_RANDOM ON page (page_random);
CREATE INDEX IDX_PAGE_LEN ON page (page_len);

CREATE TABLE page_backup (
  page_id int IDENTITY NOT NULL,
  page_namespace int NOT NULL,
  page_title varchar(255) NOT NULL,
  page_restrictions varbinary NOT NULL,
  page_counter bigint NOT NULL DEFAULT '0',
  page_is_redirect tinyint NOT NULL DEFAULT '0',
  page_is_new tinyint NOT NULL DEFAULT '0',
  page_random float NOT NULL,
  page_touched varchar(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id),
  UNIQUE (page_namespace,page_title)
);
CREATE INDEX IDX_PAGE_BACKUP_RANDOM ON page_backup (page_random);
CREATE INDEX IDX_PAGE_BACKUP_LEN ON page_backup (page_len);

CREATE TABLE page_restrictions (
  pr_page int NOT NULL,
  pr_type varchar(60) NOT NULL,
  pr_level varchar(60) NOT NULL,
  pr_cascade tinyint NOT NULL,
  pr_user int DEFAULT NULL,
  pr_expiry varchar(14) DEFAULT NULL,
  pr_id int NOT NULL,
  PRIMARY KEY (pr_id),
  UNIQUE (pr_page,pr_type)
);
CREATE INDEX IDX_PR_TYPELEVEL ON page_restrictions (pr_type,pr_level);
CREATE INDEX IDX_PR_LEVEL ON page_restrictions (pr_level);
CREATE INDEX IDX_PR_CASCADE ON page_restrictions (pr_cascade);

CREATE TABLE recentchanges (
  rc_id int IDENTITY NOT NULL,
  rc_timestamp varchar(14) NOT NULL DEFAULT '',
  rc_cur_time varchar(14) NOT NULL DEFAULT '',
  rc_user int NOT NULL DEFAULT '0',
  rc_user_text varchar(255) NOT NULL,
  rc_namespace int NOT NULL DEFAULT '0',
  rc_title varchar(255) NOT NULL DEFAULT '',
  rc_comment varchar(255) NOT NULL DEFAULT '',
  rc_minor tinyint NOT NULL DEFAULT '0',
  rc_bot tinyint NOT NULL DEFAULT '0',
  rc_new tinyint NOT NULL DEFAULT '0',
  rc_cur_id int NOT NULL DEFAULT '0',
  rc_this_oldid int NOT NULL DEFAULT '0',
  rc_last_oldid int NOT NULL DEFAULT '0',
  rc_type tinyint NOT NULL DEFAULT '0',
  rc_moved_to_ns tinyint NOT NULL DEFAULT '0',
  rc_moved_to_title varchar(255) NOT NULL DEFAULT '',
  rc_patrolled tinyint NOT NULL DEFAULT '0',
  rc_ip varchar(40) NOT NULL DEFAULT '',
  rc_old_len int DEFAULT NULL,
  rc_new_len int DEFAULT NULL,
  rc_deleted tinyint NOT NULL DEFAULT '0',
  rc_logid int NOT NULL DEFAULT '0',
  rc_log_type varchar(255) DEFAULT NULL,
  rc_log_action varchar(255) DEFAULT NULL,
  rc_params text,
  PRIMARY KEY (rc_id)
);
CREATE INDEX IDX_RC_TIMESTAMP ON recentchanges (rc_timestamp);
CREATE INDEX IDX_RC_NAMESPACE_TITLE ON recentchanges (rc_namespace,rc_title);
CREATE INDEX IDX_RC_CUR_ID ON recentchanges (rc_cur_id);
CREATE INDEX IDX_NEW_NAME_TIMESTAMP ON recentchanges (rc_new,rc_namespace,rc_timestamp);
CREATE INDEX IDX_RC_IP ON recentchanges (rc_ip);
CREATE INDEX IDX_RC_NS_USERTEXT ON recentchanges (rc_namespace,rc_user_text);
CREATE INDEX IDX_RC_USER_TEXT ON recentchanges (rc_user_text,rc_timestamp);

CREATE SEQUENCE revision_rev_id_seq START WITH 1 MINVALUE 1 INCREMENT BY 1;
CREATE TABLE revision (
  rev_id int NOT NULL DEFAULT NEXT VALUE FOR revision_rev_id_seq,
  rev_page int NOT NULL,
  rev_text_id int NOT NULL,
  rev_comment varchar(255) NOT NULL,
  rev_user int NOT NULL DEFAULT '0',
  rev_user_text varchar(255) NOT NULL DEFAULT '',
  rev_timestamp varchar(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  rev_minor_edit tinyint NOT NULL DEFAULT '0',
  rev_deleted tinyint NOT NULL DEFAULT '0',
  rev_len int DEFAULT NULL,
  rev_parent_id int DEFAULT NULL,
  PRIMARY KEY (rev_id),
  UNIQUE (rev_page,rev_id)
);
CREATE INDEX IDX_REV_TIMESTAMP ON revision (rev_timestamp);
CREATE INDEX IDX_PAGE_TIMESTAMP ON revision (rev_page,rev_timestamp);
CREATE INDEX IDX_USER_TIMESTAMP ON revision (rev_user,rev_timestamp);
CREATE INDEX IDX_USERTEXT_TIMESTAMP ON revision (rev_user_text,rev_timestamp);

CREATE SEQUENCE text_old_id_seq START WITH 1 MINVALUE 1 INCREMENT BY 1;
CREATE TABLE text (
  old_id int NOT NULL DEFAULT NEXT VALUE FOR text_old_id_seq,
  old_text varchar(max) NOT NULL,
  old_flags varchar(255) NOT NULL,
  old_page int DEFAULT NULL,
  PRIMARY KEY (old_id)
);


CREATE TABLE useracct (
  user_id int IDENTITY NOT NULL,
  user_name varchar(255) NOT NULL DEFAULT '',
  user_real_name varchar(255) NOT NULL DEFAULT '',
  user_password varchar(255) NOT NULL,
  user_newpassword varchar(255) NOT NULL,
  user_newpass_time varchar(14) DEFAULT NULL,
  user_email varchar(255) NOT NULL,
  user_options varchar(255) NOT NULL,
  user_touched varchar(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  user_token varchar(32) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  user_email_authenticated varchar(255) DEFAULT NULL,
  user_email_token varchar(255) DEFAULT NULL,
  user_email_token_expires varchar(255) DEFAULT NULL,
  user_registration varchar(14) DEFAULT NULL,
  user_editcount int DEFAULT NULL,
  PRIMARY KEY (user_id),
  UNIQUE (user_name)
);
CREATE INDEX IDX_USER_EMAIL_TOKEN ON useracct (user_email_token);

CREATE TABLE user_groups (
  ug_user int NOT NULL DEFAULT '0',
  ug_group varchar(16) NOT NULL DEFAULT '',
  UNIQUE (ug_user,ug_group)
);
CREATE INDEX IDX_UG_GROUP ON user_groups (ug_group);

CREATE TABLE value_backup (
  table_name varchar(255) DEFAULT NULL,
  maxid int DEFAULT NULL
);

CREATE TABLE watchlist (
  wl_user int NOT NULL,
  wl_namespace int NOT NULL DEFAULT '0',
  wl_title varchar(255) NOT NULL DEFAULT '',
  wl_notificationtimestamp varchar(14) DEFAULT NULL,
  UNIQUE (wl_user,wl_namespace,wl_title)
);
CREATE INDEX IDX_WL_NAMESPACE_TITLE ON watchlist (wl_namespace, wl_title);