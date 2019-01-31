-- ORACLE Wikipedia

ALTER SESSION SET NLS_LENGTH_SEMANTICS=CHAR;

-- Drop All tables
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ipblocks'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE logging'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE page'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE page_restrictions'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE recentchanges'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE revision'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE text'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE useracct'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE user_groups'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE watchlist'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

-- Create tables

CREATE TABLE ipblocks (
ipb_id number(10,0) NOT NULL,
ipb_address varchar2(255) NOT NULL,
ipb_user number(10,0) DEFAULT '0',
ipb_by number(10,0) DEFAULT '0',
ipb_by_text varchar2(255) DEFAULT '',
ipb_reason varchar2(255) NOT NULL,
ipb_timestamp varchar2(14) DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
ipb_auto number(3,0) DEFAULT '0',
ipb_anon_only number(3,0) DEFAULT '0',
ipb_create_account number(3,0) DEFAULT '1',
ipb_enable_autoblock number(3,0) DEFAULT '1',
ipb_expiry varchar2(14) DEFAULT '',
ipb_range_start varchar2(8) NOT NULL,
ipb_range_end varchar2(8) NOT NULL,
ipb_deleted number(3,0) DEFAULT '0',
ipb_block_email number(3,0) DEFAULT '0',
ipb_allow_usertalk number(3,0) DEFAULT '0',
PRIMARY KEY (ipb_id),
UNIQUE (ipb_address,ipb_user,ipb_auto,ipb_anon_only)
);

CREATE TABLE logging (
log_id number(10,0) NOT NULL,
log_type varchar2(32) NOT NULL,
log_action varchar2(32) NOT NULL,
log_timestamp varchar2(14) DEFAULT '19700101000000',
log_user number(10,0) DEFAULT '0',
log_namespace number(10,0) DEFAULT '0',
log_title varchar2(255) DEFAULT '',
log_comment varchar2(255) DEFAULT '',
log_params varchar2(255) NOT NULL,
log_deleted number(3,0) DEFAULT '0',
log_user_text varchar2(255) DEFAULT '',
log_page number(10,0) DEFAULT NULL,
PRIMARY KEY (log_id)
);

CREATE TABLE page (
page_id number(10,0) NOT NULL,
page_namespace number(10,0) NOT NULL,
page_title varchar2(255) NOT NULL,
page_restrictions varchar(255) NOT NULL,
page_counter number(20,0) DEFAULT '0',
page_is_redirect number(3,0) DEFAULT '0',
page_is_new number(3,0) DEFAULT '0',
page_random float(24) NOT NULL,
page_touched varchar2(14) DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
page_latest number(10,0) NOT NULL,
page_len number(10,0) NOT NULL,
PRIMARY KEY (page_id),
UNIQUE (page_namespace,page_title)
);

CREATE TABLE page_restrictions (
pr_page number(10,0) NOT NULL,
pr_type varchar2(60) NOT NULL,
pr_level varchar2(60) NOT NULL,
pr_cascade number(3,0) NOT NULL,
pr_user number(10,0) DEFAULT NULL,
pr_expiry varchar2(14) DEFAULT NULL,
pr_id number(10,0) NOT NULL,
PRIMARY KEY (pr_id),
UNIQUE (pr_page,pr_type)
);

CREATE TABLE recentchanges (
rc_id number(10,0) NOT NULL,
rc_timestamp varchar2(14) DEFAULT '',
rc_cur_time varchar2(14) DEFAULT '',
rc_user number(10,0) DEFAULT '0',
rc_user_text varchar2(255) NOT NULL,
rc_namespace number(10,0) DEFAULT '0',
rc_title varchar2(255) DEFAULT '',
rc_comment varchar2(255) DEFAULT '',
rc_minor number(3,0) DEFAULT '0',
rc_bot number(3,0) DEFAULT '0',
rc_new number(3,0) DEFAULT '0',
rc_cur_id number(10,0) DEFAULT '0',
rc_this_oldid number(10,0) DEFAULT '0',
rc_last_oldid number(10,0) DEFAULT '0',
rc_type number(3,0) DEFAULT '0',
rc_moved_to_ns number(3,0) DEFAULT '0',
rc_moved_to_title varchar2(255) DEFAULT '',
rc_patrolled number(3,0) DEFAULT '0',
rc_ip varchar2(40) DEFAULT '',
rc_old_len number(10,0) DEFAULT NULL,
rc_new_len number(10,0) DEFAULT NULL,
rc_deleted number(3,0) DEFAULT '0',
rc_logid number(10,0) DEFAULT '0',
rc_log_type varchar2(255) DEFAULT NULL,
rc_log_action varchar2(255) DEFAULT NULL,
rc_params varchar2(255),
PRIMARY KEY (rc_id)
);

CREATE TABLE revision (
rev_id number(10,0) NOT NULL,
rev_page number(10,0) NOT NULL,
rev_text_id number(10,0) NOT NULL,
rev_comment varchar2(1024) NOT NULL,
rev_user number(10,0) DEFAULT '0',
rev_user_text varchar2(255) DEFAULT '',
rev_timestamp varchar2(14) DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
rev_minor_edit number(3,0) DEFAULT '0',
rev_deleted number(3,0) DEFAULT '0',
rev_len number(10,0) DEFAULT NULL,
rev_parent_id number(10,0) DEFAULT NULL,
PRIMARY KEY (rev_id),
UNIQUE (rev_page,rev_id)
);

CREATE TABLE text (
old_id number(10,0) NOT NULL,
old_text clob NOT NULL,
old_flags varchar2(30) NOT NULL,
old_page number(10,0) DEFAULT NULL,
PRIMARY KEY (old_id)
);

CREATE TABLE useracct (
user_id number(10,0) NOT NULL,
user_name varchar2(255) DEFAULT '',
user_real_name varchar2(255) DEFAULT '',
user_password varchar2(32) NOT NULL,
user_newpassword varchar2(32) NOT NULL,
user_newpass_time varchar2(14) DEFAULT NULL,
user_email varchar2(40) NOT NULL,
user_options varchar2(255) NOT NULL,
user_touched varchar2(14) DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
user_token varchar2(32) DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
user_email_authenticated varchar2(32) DEFAULT NULL,
user_email_token varchar2(32) DEFAULT NULL,
user_email_token_expires varchar2(14) DEFAULT NULL,
user_registration varchar2(14) DEFAULT NULL,
user_editcount number(10,0) DEFAULT NULL,
PRIMARY KEY (user_id),
UNIQUE (user_name)
);

CREATE TABLE user_groups (
ug_user number(10,0) DEFAULT '0',
ug_group varchar2(16) DEFAULT '',
UNIQUE (ug_user,ug_group)
);

CREATE TABLE watchlist (
wl_user number(10,0) NOT NULL,
wl_namespace number(10,0) DEFAULT '0',
wl_title varchar2(255) DEFAULT '',
wl_notificationtimestamp varchar2(14) DEFAULT NULL,
UNIQUE (wl_user,wl_namespace,wl_title)
);

-- Create indexes

CREATE INDEX IDX_USER_EMAIL_TOKEN ON useracct (user_email_token);
CREATE INDEX IDX_UG_GROUP ON user_groups (ug_group);
CREATE INDEX IDX_WL_NAMESPACE_TITLE ON watchlist (wl_namespace, wl_title);
CREATE INDEX IDX_IPB_USER ON ipblocks (ipb_user);
CREATE INDEX IDX_IPB_RANGE ON ipblocks (ipb_range_start,ipb_range_end);
CREATE INDEX IDX_IPB_TIMESTAMP ON ipblocks (ipb_timestamp);
CREATE INDEX IDX_IPB_EXPIRY ON ipblocks (ipb_expiry);
CREATE INDEX IDX_LOG_TYPE_TIME ON logging (log_type,log_timestamp);
CREATE INDEX IDX_LOG_USER_TIME ON logging (log_user,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_TIME ON logging (log_namespace,log_title,log_timestamp);
CREATE INDEX IDX_LOG_TIMES ON logging (log_timestamp);
CREATE INDEX IDX_LOG_USER_TYPE_TIME ON logging (log_user,log_type,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_ID_TIME ON logging (log_page,log_timestamp);
CREATE INDEX IDX_PR_TYPELEVEL ON page_restrictions (pr_type,pr_level);
CREATE INDEX IDX_PR_LEVEL ON page_restrictions (pr_level);
CREATE INDEX IDX_PR_CASCADE ON page_restrictions (pr_cascade);
CREATE INDEX IDX_REV_TIMESTAMP ON revision (rev_timestamp);
CREATE INDEX IDX_PAGE_TIMESTAMP ON revision (rev_page,rev_timestamp);
CREATE INDEX IDX_USER_TIMESTAMP ON revision (rev_user,rev_timestamp);
CREATE INDEX IDX_USER_TEXT_TIMESTAMP ON revision (rev_user_text,rev_timestamp);
CREATE INDEX IDX_PAGE_RANDOM ON page (page_random);
CREATE INDEX IDX_PAGE_LEN ON page (page_len);
CREATE INDEX IDX_RC_TIMESTAMP ON recentchanges (rc_timestamp);
CREATE INDEX IDX_RC_NAMESPACE_TITLE ON recentchanges (rc_namespace,rc_title);
CREATE INDEX IDX_RC_CUR_ID ON recentchanges (rc_cur_id);
CREATE INDEX IDX_NEW_NAME_TIMESTAMP ON recentchanges (rc_new,rc_namespace,rc_timestamp);
CREATE INDEX IDX_RC_IP ON recentchanges (rc_ip);
CREATE INDEX IDX_RC_NS_USER_TEXT ON recentchanges (rc_namespace,rc_user_text);
CREATE INDEX IDX_RC_USER_TEXT ON recentchanges (rc_user_text,rc_timestamp);

-- Sequences
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'IPBLOCKS_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE IPBLOCKS_SEQ'; END IF; END;;
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'LOGGING_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE LOGGING_SEQ'; END IF; END;;
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'PAGE_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE PAGE_SEQ'; END IF; END;;
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'PAGE_RESTRICTIONS_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE PAGE_RESTRICTIONS_SEQ'; END IF; END;;
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'RECENTCHANGES_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE RECENTCHANGES_SEQ'; END IF; END;;
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'REVISION_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE REVISION_SEQ'; END IF; END;;
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'TEXT_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE TEXT_SEQ'; END IF; END;;
DECLARE cnt NUMBER; BEGIN SELECT count(*) INTO cnt FROM all_sequences WHERE sequence_name = 'USER_SEQ'; IF cnt > 0 THEN EXECUTE IMMEDIATE 'DROP SEQUENCE USER_SEQ'; END IF; END;;

create sequence ipblocks_seq start with 1 increment by 1 nomaxvalue;
create sequence logging_seq start with 1 increment by 1 nomaxvalue;
create sequence page_seq start with 1 increment by 1 nomaxvalue;
create sequence page_restrictions_seq start with 1 increment by 1 nomaxvalue;
create sequence recentchanges_seq start with 1 increment by 1 nomaxvalue;
create sequence revision_seq start with 1 increment by 1 nomaxvalue;
create sequence text_seq start with 1 increment by 1 nomaxvalue;
create sequence user_seq start with 1 increment by 1 nomaxvalue;

-- Sequences' triggers
CREATE OR REPLACE TRIGGER user_seq_tr
BEFORE INSERT ON useracct FOR EACH ROW
WHEN (NEW.user_id IS NULL OR NEW.user_id = 0)
BEGIN
SELECT user_seq.NEXTVAL INTO :NEW.user_id FROM dual;END;;

CREATE OR REPLACE TRIGGER page_seq_tr
BEFORE INSERT ON page FOR EACH ROW
WHEN (NEW.page_id IS NULL OR NEW.page_id = 0)
BEGIN
SELECT page_seq.NEXTVAL INTO :NEW.page_id FROM dual;END;;

CREATE OR REPLACE TRIGGER text_seq_tr
BEFORE INSERT ON text FOR EACH ROW
WHEN (NEW.old_id IS NULL OR NEW.old_id = 0)
BEGIN
SELECT text_seq.NEXTVAL INTO :NEW.old_id FROM dual;END;;

CREATE OR REPLACE TRIGGER revision_seq_tr
BEFORE INSERT ON revision FOR EACH ROW
WHEN (NEW.rev_id IS NULL OR NEW.rev_id = 0)
BEGIN
SELECT revision_seq.NEXTVAL INTO :NEW.rev_id FROM dual;END;;

CREATE OR REPLACE TRIGGER recentchanges_seq_tr
BEFORE INSERT ON recentchanges FOR EACH ROW
WHEN (NEW.rc_id IS NULL OR NEW.rc_id = 0)
BEGIN
SELECT recentchanges_seq.NEXTVAL INTO :NEW.rc_id FROM dual;END;;

CREATE OR REPLACE TRIGGER logging_seq_tr
BEFORE INSERT ON logging FOR EACH ROW
WHEN (NEW.log_id IS NULL OR NEW.log_id = 0)
BEGIN
SELECT logging_seq.NEXTVAL INTO :NEW.log_id FROM dual;END;;

