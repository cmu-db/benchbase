-- Drop all tables
BEGIN EXECUTE IMMEDIATE 'DROP TABLE CONFIG_PROFILE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE REGION CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERACCT CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERACCT_ATTRIBUTES'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE CATEGORY CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE GLOBAL_ATTRIBUTE_GROUP CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE GLOBAL_ATTRIBUTE_VALUE CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ITEM CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ITEM_ATTRIBUTE CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ITEM_IMAGE CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ITEM_COMMENT CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ITEM_BID CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ITEM_MAX_BID CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ITEM_PURCHASE CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERACCT_FEEDBACK CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERACCT_ITEM CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;
BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERACCT_WATCH CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;;

-- Create tables
-- ================================================================ 
-- CONFIG_PROFILE
-- ================================================================
CREATE TABLE CONFIG_PROFILE (
    cfp_scale_factor            FLOAT NOT NULL,
    cfp_loader_start            TIMESTAMP NOT NULL,
    cfp_loader_stop             TIMESTAMP NOT NULL,
    cfp_user_item_histogram     CLOB NOT NULL
);

-- ================================================================
-- REGION
-- Represents regions of users
-- r_id             Region's ID
-- r_name           Region's name
-- ================================================================
CREATE TABLE REGION (
    r_id                INT NOT NULL,
    r_name              VARCHAR(32),
    PRIMARY KEY (r_id)
);

-- ================================================================
-- USERACCT
-- Represents user accounts 
-- u_id             User ID
-- u_firstname      User's first name
-- u_lastname       User's last name
-- u_password       User's password
-- u_email          User's email
-- u_rating         User's rating as a seller
-- u_balance        User's balance
-- u_created        User's create date
-- u_r_id           User's region ID
-- ================================================================
CREATE TABLE USERACCT (
    u_id                INT NOT NULL,
    u_rating            INT NOT NULL,
    u_balance           FLOAT NOT NULL,
    u_comments			INTEGER DEFAULT 0,
    u_r_id              INT NOT NULL REFERENCES REGION (r_id),
    u_created           TIMESTAMP,
    u_updated           TIMESTAMP,
    u_sattr0            VARCHAR(64),
    u_sattr1            VARCHAR(64),
    u_sattr2            VARCHAR(64),
    u_sattr3            VARCHAR(64),
    u_sattr4            VARCHAR(64),
    u_sattr5            VARCHAR(64),
    u_sattr6            VARCHAR(64),
    u_sattr7            VARCHAR(64),
    u_iattr0            INT DEFAULT NULL,
    u_iattr1            INT DEFAULT NULL,
    u_iattr2			INT DEFAULT NULL,
    u_iattr3            INT DEFAULT NULL,
    u_iattr4            INT DEFAULT NULL,
    u_iattr5            INT DEFAULT NULL,
    u_iattr6            INT DEFAULT NULL,
    u_iattr7            INT DEFAULT NULL, 
    PRIMARY KEY (u_id)
);
CREATE INDEX IDX_USERACCT_REGION ON USERACCT (u_id, u_r_id);

-- ================================================================
-- USERACCT_ATTRIBUTES
-- Represents user's attributes 
-- ================================================================
CREATE TABLE USERACCT_ATTRIBUTES (
    ua_id               INT NOT NULL,
    ua_u_id             INT NOT NULL REFERENCES USERACCT (u_id),
    ua_name             VARCHAR(32) NOT NULL,
    ua_value            VARCHAR(32) NOT NULL,
    u_created           TIMESTAMP,
    PRIMARY KEY (ua_id, ua_u_id)
);

-- ================================================================
-- CATEGORY
-- Represents merchandises' categories. Category can be hierarchical aligned using c_parent_id.
-- c_id                Category's ID
-- c_name            Category's name
-- c_parent_id        Parent category's ID
-- ================================================================
CREATE TABLE CATEGORY (
    c_id                INT NOT NULL,
    c_name              VARCHAR(50),
    c_parent_id         INT REFERENCES CATEGORY (c_id),
    PRIMARY KEY (c_id)
);

-- ================================================================
-- GLOBAL_ATTRIBUTE_GROUP
-- Represents merchandises' global attribute groups (for example, brand, material, feature etc.).
-- gag_id            Global attribute group's ID
-- gag_c_id            Associated Category's ID
-- gag_name            Global attribute group's name
-- ================================================================
CREATE TABLE GLOBAL_ATTRIBUTE_GROUP (
    gag_id              INT NOT NULL,
    gag_c_id            INT NOT NULL REFERENCES CATEGORY (c_id),
    gag_name            VARCHAR(100) NOT NULL,
    PRIMARY KEY (gag_id)
);

-- ================================================================
-- GLOBAL_ATTRIBUTE_VALUE
-- Represents merchandises' global attributes within each attribute
-- groups (for example, Rolex, Casio, Seiko within brand)
-- gav_id            Global attribute value's ID
-- gav_gag_id        Associated Global attribute group's ID
-- gav_name            Global attribute value's name
-- ================================================================
CREATE TABLE GLOBAL_ATTRIBUTE_VALUE (
    gav_id              INT NOT NULL,
    gav_gag_id          INT NOT NULL REFERENCES GLOBAL_ATTRIBUTE_GROUP (gag_id),
    gav_name            VARCHAR(100) NOT NULL,
    PRIMARY KEY (gav_id, gav_gag_id)
);

-- ================================================================
-- ITEM
-- Represents merchandises
-- i_id                  Item's ID
-- i_u_id                Seller's ID
-- i_c_id                Category's ID
-- i_name                Item's name
-- i_description      Item's description
-- i_initial_price    Item's initial price
-- i_reserve_price    Item's reserve price
-- i_buy_now            Item's buy now price
-- i_nb_of_bids        Item's number of bids
-- i_max_bid            Item's max bid price
-- i_user_attributes Text field for attributes defined just for this item
-- i_start_date        Item's bid start date
-- i_end_date          Item's bid end date
-- i_status	        Items' status (0 = open, 1 = wait for purchase, 2 = close)
-- ================================================================
CREATE TABLE ITEM (
    i_id                INT NOT NULL,
    i_u_id              INT NOT NULL REFERENCES USERACCT (u_id),
    i_c_id              INT NOT NULL REFERENCES CATEGORY (c_id),
    i_name              VARCHAR(100),
    i_description       VARCHAR(1024),
    i_user_attributes   VARCHAR(255) DEFAULT NULL,
    i_initial_price     FLOAT NOT NULL,
    i_current_price     FLOAT NOT NULL,
    i_num_bids          INT,
    i_num_images        INT,
    i_num_global_attrs  INT,
    i_num_comments      INT,
    i_start_date        TIMESTAMP,
    i_end_date          TIMESTAMP,
    i_status		    INT DEFAULT 0,
    i_created           TIMESTAMP,
    i_updated           TIMESTAMP,
    i_iattr0            INT DEFAULT NULL,
    i_iattr1            INT DEFAULT NULL,
    i_iattr2			INT DEFAULT NULL,
    i_iattr3            INT DEFAULT NULL,
    i_iattr4            INT DEFAULT NULL,
    i_iattr5            INT DEFAULT NULL,
    i_iattr6            INT DEFAULT NULL,
    i_iattr7            INT DEFAULT NULL, 
    PRIMARY KEY (i_id, i_u_id)
);
CREATE INDEX IDX_ITEM_SELLER ON ITEM (i_u_id);

-- ================================================================
-- ITEM_ATTRIBUTE
-- Represents mappings between attribute values and items
-- ia_id            Item attribute's ID
-- ia_i_id            Item's ID
-- ia_gav_id        Global attribute value's ID
-- ================================================================
CREATE TABLE ITEM_ATTRIBUTE (
    ia_id               INT NOT NULL,
    ia_i_id             INT NOT NULL,
    ia_u_id             INT NOT NULL,
    ia_gav_id           INT NOT NULL,
    ia_gag_id           INT NOT NULL,
    ia_sattr0			VARCHAR(64) DEFAULT NULL,
    FOREIGN KEY (ia_i_id, ia_u_id) REFERENCES ITEM (i_id, i_u_id),
    FOREIGN KEY (ia_gav_id, ia_gag_id) REFERENCES GLOBAL_ATTRIBUTE_VALUE (gav_id, gav_gag_id),
    PRIMARY KEY (ia_id, ia_i_id, ia_u_id)
);

-- ================================================================
-- ITEM_IMAGE
-- Represents images of items
-- ii_id            Image's ID
-- ii_i_id            Item's ID
-- ii_path            Image's path
-- ================================================================
CREATE TABLE ITEM_IMAGE (
    ii_id               INT NOT NULL,
    ii_i_id             INT NOT NULL,
    ii_u_id             INT NOT NULL,
    ii_sattr0			VARCHAR(128) NOT NULL,
    FOREIGN KEY (ii_i_id, ii_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (ii_id, ii_i_id, ii_u_id)
);

-- ================================================================
-- ITEM_COMMENT
-- Represents comments provided by buyers
-- ic_id            Comment's ID
-- ic_i_id            Item's ID
-- ic_u_id            Buyer's ID
-- ic_date            Comment's create date
-- ic_question        Comment by buyer
-- ic_response        Response from seller
-- ================================================================
CREATE TABLE ITEM_COMMENT (
    ic_id               INT NOT NULL,
    ic_i_id             INT NOT NULL,
    ic_u_id             INT NOT NULL,
    ic_buyer_id         INT NOT NULL REFERENCES USERACCT (u_id),
    ic_question         VARCHAR(128) NOT NULL,
    ic_response         VARCHAR(128) DEFAULT NULL,
    ic_created          TIMESTAMP,
    ic_updated          TIMESTAMP,
    FOREIGN KEY (ic_i_id, ic_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (ic_id, ic_i_id, ic_u_id)
); 
-- CREATE INDEX IDX_ITEM_COMMENT ON ITEM_COMMENT (ic_i_id, ic_u_id);

-- ================================================================
-- ITEM_BID
-- Represents merchandises' bids
-- ib_id            Bid's ID
-- ib_i_id            Item's ID
-- ib_u_id            Buyer's ID
-- ib_type            Type of transaction (bid or buy_now)
-- ib_bid            Bid's price
-- ib_max_bid        ???
-- ib_date            Bid's date
-- ================================================================
CREATE TABLE ITEM_BID (
    ib_id               INT NOT NULL,
    ib_i_id             INT NOT NULL,
    ib_u_id             INT NOT NULL,
    ib_buyer_id         INT NOT NULL REFERENCES USERACCT (u_id),
    ib_bid		        FLOAT NOT NULL,
    ib_max_bid          FLOAT NOT NULL,
    ib_created          TIMESTAMP,
    ib_updated          TIMESTAMP,
    FOREIGN KEY (ib_i_id, ib_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (ib_id, ib_i_id, ib_u_id)
);

-- ================================================================
-- ITEM_MAX_BID
-- Cross-reference table to the current max bid for an auction
-- ================================================================
CREATE TABLE ITEM_MAX_BID (
    imb_i_id            INT NOT NULL,
    imb_u_id            INT NOT NULL,
    imb_ib_id           INT NOT NULL,
    imb_ib_i_id         INT NOT NULL,
    imb_ib_u_id         INT NOT NULL,
    imb_created         TIMESTAMP,
    imb_updated         TIMESTAMP,
    FOREIGN KEY (imb_i_id, imb_u_id) REFERENCES ITEM (i_id, i_u_id),
    FOREIGN KEY (imb_ib_id, imb_ib_i_id, imb_ib_u_id) REFERENCES ITEM_BID (ib_id, ib_i_id, ib_u_id),
    PRIMARY KEY (imb_i_id, imb_u_id)
);

-- ================================================================
-- ITEM_PURCHASE
-- Represents purchase transaction (buy_now bid or win bid)
-- ip_id            Purchase's ID
-- ip_ib_id            Bid's ID
-- ip_date            Purchase's date
-- ================================================================
CREATE TABLE ITEM_PURCHASE (
    ip_id               INT NOT NULL,
    ip_ib_id            INT NOT NULL,
    ip_ib_i_id          INT NOT NULL,
    ip_ib_u_id          INT NOT NULL,
    ip_date             TIMESTAMP,
    FOREIGN KEY (ip_ib_id, ip_ib_i_id, ip_ib_u_id) REFERENCES ITEM_BID (ib_id, ib_i_id, ib_u_id),
    PRIMARY KEY (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id),
    UNIQUE (ip_ib_id, ip_ib_i_id, ip_ib_u_id)
);

-- ================================================================
-- USERACCT_FEEDBACK
-- Represents feedbacks between buyers and sellers for a transaction
-- uf_id             Feedback's ID
-- uf_u_id           The user receiving the feedback
-- uf_i_id           Item's ID
-- uf_i_u_id         Item's seller id
-- uf_from_id        The other user writing the feedback
-- uf_date           Feedback's create date
-- uf_comment        Feedback by other user
-- ================================================================
CREATE TABLE USERACCT_FEEDBACK (
    uf_u_id             INT NOT NULL REFERENCES USERACCT (u_id),
    uf_i_id             INT NOT NULL,
    uf_i_u_id           INT NOT NULL,
    uf_from_id          INT NOT NULL REFERENCES USERACCT (u_id),
    uf_rating           NUMBER(5,0) NOT NULL,
    uf_date             TIMESTAMP,
    uf_sattr0           VARCHAR(80) NOT NULL,
    FOREIGN KEY (uf_i_id, uf_i_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (uf_u_id, uf_i_id, uf_i_u_id, uf_from_id),
    CHECK (uf_u_id <> uf_from_id)
);

-- ================================================================
-- USERACCT_ITEM
-- The items that a user has recently purchased
-- ================================================================
CREATE TABLE USERACCT_ITEM (
    ui_u_id             INT NOT NULL REFERENCES USERACCT (u_id),
    ui_i_id             INT NOT NULL,
    ui_i_u_id           INT NOT NULL,
    ui_ip_id            INT,
    ui_ip_ib_id         INT,
    ui_ip_ib_i_id       INT,
    ui_ip_ib_u_id       INT,
    ui_created          TIMESTAMP,
    FOREIGN KEY (ui_i_id, ui_i_u_id) REFERENCES ITEM (i_id, i_u_id),
    FOREIGN KEY (ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id) REFERENCES ITEM_PURCHASE (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id),
    PRIMARY KEY (ui_u_id, ui_i_id, ui_i_u_id)
);
-- CREATE INDEX IDX_USERACCT_ITEM_ID ON USERACCT_ITEM (ui_i_id);

-- ================================================================
-- USERACCT_WATCH
-- The items that a user is watching
-- ================================================================
CREATE TABLE USERACCT_WATCH (
    uw_u_id             INT NOT NULL REFERENCES USERACCT (u_id),
    uw_i_id             INT NOT NULL,
    uw_i_u_id           INT NOT NULL,
    uw_created          TIMESTAMP,
    FOREIGN KEY (uw_i_id, uw_i_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (uw_u_id, uw_i_id, uw_i_u_id)
);

