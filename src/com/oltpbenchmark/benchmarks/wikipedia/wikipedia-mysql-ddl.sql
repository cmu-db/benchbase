DROP TABLE IF EXISTS ipblocks;
CREATE TABLE ipblocks (
  ipb_id int NOT NULL auto_increment,
  ipb_address tinyblob NOT NULL,
  ipb_user int NOT NULL DEFAULT '0',
  ipb_by int NOT NULL DEFAULT '0',
  ipb_by_text varbinary(255) NOT NULL DEFAULT '',
  ipb_reason tinyblob NOT NULL,
  ipb_timestamp binary(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  ipb_auto tinyint(1) NOT NULL DEFAULT '0',
  ipb_anon_only tinyint(1) NOT NULL DEFAULT '0',
  ipb_create_account tinyint(1) NOT NULL DEFAULT '1',
  ipb_enable_autoblock tinyint(1) NOT NULL DEFAULT '1',
  ipb_expiry varbinary(14) NOT NULL DEFAULT '',
  ipb_range_start tinyblob NOT NULL,
  ipb_range_end tinyblob NOT NULL,
  ipb_deleted tinyint(1) NOT NULL DEFAULT '0',
  ipb_block_email tinyint(1) NOT NULL DEFAULT '0',
  ipb_allow_usertalk tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (ipb_id),
  UNIQUE (ipb_address(255),ipb_user,ipb_auto,ipb_anon_only)
);

CREATE INDEX IDX_IPB_USER ON ipblocks (ipb_user);
CREATE INDEX IDX_IPB_RANGE ON ipblocks (ipb_range_start(8),ipb_range_end(8));
CREATE INDEX IDX_IPB_TIMESTAMP ON ipblocks (ipb_timestamp);
CREATE INDEX IDX_IPB_EXPIRY ON ipblocks (ipb_expiry);

DROP TABLE IF EXISTS logging;
CREATE TABLE logging (
  log_id int NOT NULL auto_increment,
  log_type varbinary(32) NOT NULL,
  log_action varbinary(32) NOT NULL,
  log_timestamp binary(14) NOT NULL DEFAULT '19700101000000',
  log_user int NOT NULL DEFAULT '0',
  log_namespace int NOT NULL DEFAULT '0',
  log_title varbinary(255) NOT NULL DEFAULT '',
  log_comment varbinary(255) NOT NULL DEFAULT '',
  log_params blob NOT NULL,
  log_deleted tinyint(3) NOT NULL DEFAULT '0',
  log_user_text varbinary(255) NOT NULL DEFAULT '',
  log_page int DEFAULT NULL,
  PRIMARY KEY (log_id)
);
CREATE INDEX IDX_LOG_TYPE_TIME ON logging (log_type,log_timestamp);
CREATE INDEX IDX_LOG_USER_TIME ON logging (log_user,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_TIME ON logging (log_namespace,log_title,log_timestamp);
CREATE INDEX IDX_LOG_TIMES ON logging (log_timestamp);
CREATE INDEX IDX_LOG_USER_TYPE_TIME ON logging (log_user,log_type,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_ID_TIME ON logging (log_page,log_timestamp);

DROP TABLE IF EXISTS page;
CREATE TABLE page (
  page_id int NOT NULL auto_increment,
  page_namespace int NOT NULL,
  page_title varbinary(255) NOT NULL,
  page_restrictions tinyblob NOT NULL,
  page_counter bigint(20) NOT NULL DEFAULT '0',
  page_is_redirect tinyint(3) NOT NULL DEFAULT '0',
  page_is_new tinyint(3) NOT NULL DEFAULT '0',
  page_random double NOT NULL,
  page_touched binary(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id),
  UNIQUE (page_namespace,page_title)
);
CREATE INDEX IDX_PAGE_RANDOM ON page (page_random);
CREATE INDEX IDX_PAGE_LEN ON page (page_len);

DROP TABLE IF EXISTS page_backup;
CREATE TABLE page_backup (
  page_id int NOT NULL auto_increment,
  page_namespace int NOT NULL,
  page_title varbinary(255) NOT NULL,
  page_restrictions tinyblob NOT NULL,
  page_counter bigint(20) NOT NULL DEFAULT '0',
  page_is_redirect tinyint(3) NOT NULL DEFAULT '0',
  page_is_new tinyint(3) NOT NULL DEFAULT '0',
  page_random double NOT NULL,
  page_touched binary(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id),
  UNIQUE (page_namespace,page_title)
);
CREATE INDEX IDX_PAGE_BACKUP_RANDOM ON page_backup (page_random);
CREATE INDEX IDX_PAGE_BACKUP_LEN ON page_backup (page_len);

DROP TABLE IF EXISTS page_restrictions;
CREATE TABLE page_restrictions (
  pr_page int NOT NULL,
  pr_type varbinary(60) NOT NULL,
  pr_level varbinary(60) NOT NULL,
  pr_cascade tinyint(4) NOT NULL,
  pr_user int DEFAULT NULL,
  pr_expiry varbinary(14) DEFAULT NULL,
  pr_id int NOT NULL,
  PRIMARY KEY (pr_id),
  UNIQUE (pr_page,pr_type)
);
CREATE INDEX IDX_PR_TYPELEVEL ON page_restrictions (pr_type,pr_level);
CREATE INDEX IDX_PR_LEVEL ON page_restrictions (pr_level);
CREATE INDEX IDX_PR_CASCADE ON page_restrictions (pr_cascade);

DROP TABLE IF EXISTS recentchanges;
CREATE TABLE recentchanges (
  rc_id int NOT NULL auto_increment,
  rc_timestamp varbinary(14) NOT NULL DEFAULT '',
  rc_cur_time varbinary(14) NOT NULL DEFAULT '',
  rc_user int NOT NULL DEFAULT '0',
  rc_user_text varbinary(255) NOT NULL,
  rc_namespace int NOT NULL DEFAULT '0',
  rc_title varbinary(255) NOT NULL DEFAULT '',
  rc_comment varbinary(255) NOT NULL DEFAULT '',
  rc_minor tinyint(3) NOT NULL DEFAULT '0',
  rc_bot tinyint(3) NOT NULL DEFAULT '0',
  rc_new tinyint(3) NOT NULL DEFAULT '0',
  rc_cur_id int NOT NULL DEFAULT '0',
  rc_this_oldid int NOT NULL DEFAULT '0',
  rc_last_oldid int NOT NULL DEFAULT '0',
  rc_type tinyint(3) NOT NULL DEFAULT '0',
  rc_moved_to_ns tinyint(3) NOT NULL DEFAULT '0',
  rc_moved_to_title varbinary(255) NOT NULL DEFAULT '',
  rc_patrolled tinyint(3) NOT NULL DEFAULT '0',
  rc_ip varbinary(40) NOT NULL DEFAULT '',
  rc_old_len int DEFAULT NULL,
  rc_new_len int DEFAULT NULL,
  rc_deleted tinyint(3) NOT NULL DEFAULT '0',
  rc_logid int NOT NULL DEFAULT '0',
  rc_log_type varbinary(255) DEFAULT NULL,
  rc_log_action varbinary(255) DEFAULT NULL,
  rc_params blob,
  PRIMARY KEY (rc_id)
);
CREATE INDEX IDX_RC_TIMESTAMP ON recentchanges (rc_timestamp);
CREATE INDEX IDX_RC_NAMESPACE_TITLE ON recentchanges (rc_namespace,rc_title);
CREATE INDEX IDX_RC_CUR_ID ON recentchanges (rc_cur_id);
CREATE INDEX IDX_NEW_NAME_TIMESTAMP ON recentchanges (rc_new,rc_namespace,rc_timestamp);
CREATE INDEX IDX_RC_IP ON recentchanges (rc_ip);
CREATE INDEX IDX_RC_NS_USERTEXT ON recentchanges (rc_namespace,rc_user_text);
CREATE INDEX IDX_RC_USER_TEXT ON recentchanges (rc_user_text,rc_timestamp);

DROP TABLE IF EXISTS revision;
CREATE TABLE revision (
  rev_id int NOT NULL auto_increment,
  rev_page int NOT NULL,
  rev_text_id int NOT NULL,
  rev_comment varchar(1024) NOT NULL,
  rev_user int NOT NULL DEFAULT '0',
  rev_user_text varbinary(255) NOT NULL DEFAULT '',
  rev_timestamp binary(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  rev_minor_edit tinyint(3) NOT NULL DEFAULT '0',
  rev_deleted tinyint(3) NOT NULL DEFAULT '0',
  rev_len int DEFAULT NULL,
  rev_parent_id int DEFAULT NULL,
  PRIMARY KEY (rev_id),
  UNIQUE (rev_page,rev_id)
);
CREATE INDEX IDX_REV_TIMESTAMP ON revision (rev_timestamp);
CREATE INDEX IDX_PAGE_TIMESTAMP ON revision (rev_page,rev_timestamp);
CREATE INDEX IDX_USER_TIMESTAMP ON revision (rev_user,rev_timestamp);
CREATE INDEX IDX_USERTEXT_TIMESTAMP ON revision (rev_user_text,rev_timestamp);

DROP TABLE IF EXISTS text;
CREATE TABLE text (
  old_id int NOT NULL auto_increment,
  old_text mediumblob NOT NULL,
  old_flags tinyblob NOT NULL,
  old_page int DEFAULT NULL,
  PRIMARY KEY (old_id)
);

DROP TABLE IF EXISTS useracct;
CREATE TABLE useracct (
  user_id int NOT NULL auto_increment,
  user_name varbinary(255) NOT NULL DEFAULT '',
  user_real_name varbinary(255) NOT NULL DEFAULT '',
  user_password tinyblob NOT NULL,
  user_newpassword tinyblob NOT NULL,
  user_newpass_time binary(14) DEFAULT NULL,
  user_email tinyblob NOT NULL,
  user_options blob NOT NULL,
  user_touched binary(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  user_token binary(32) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  user_email_authenticated binary(14) DEFAULT NULL,
  user_email_token binary(32) DEFAULT NULL,
  user_email_token_expires binary(14) DEFAULT NULL,
  user_registration binary(14) DEFAULT NULL,
  user_editcount int DEFAULT NULL,
  PRIMARY KEY (user_id),
  UNIQUE (user_name)
);
CREATE INDEX IDX_USER_EMAIL_TOKEN ON useracct (user_email_token);

DROP TABLE IF EXISTS user_groups;
CREATE TABLE user_groups (
  ug_user int NOT NULL DEFAULT '0',
  ug_group varbinary(16) NOT NULL DEFAULT '',
  UNIQUE (ug_user,ug_group)
);
CREATE INDEX IDX_UG_GROUP ON user_groups (ug_group);

DROP TABLE IF EXISTS value_backup;
CREATE TABLE value_backup (
  table_name varchar(255) DEFAULT NULL,
  maxid int DEFAULT NULL
);

DROP TABLE IF EXISTS watchlist;
CREATE TABLE watchlist (
  wl_user int NOT NULL,
  wl_namespace int NOT NULL DEFAULT '0',
  wl_title varbinary(255) NOT NULL DEFAULT '',
  wl_notificationtimestamp varbinary(14) DEFAULT NULL,
  UNIQUE (wl_user,wl_namespace,wl_title)
);
CREATE INDEX IDX_WL_NAMESPACE_TITLE ON watchlist (wl_namespace, wl_title);