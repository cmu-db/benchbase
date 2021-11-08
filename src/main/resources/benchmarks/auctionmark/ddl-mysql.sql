/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS item_attribute CASCADE;
DROP TABLE IF EXISTS item_image CASCADE;
DROP TABLE IF EXISTS item_comment CASCADE;
DROP TABLE IF EXISTS item_max_bid CASCADE;
DROP TABLE IF EXISTS useracct_item CASCADE;
DROP TABLE IF EXISTS item_purchase CASCADE;
DROP TABLE IF EXISTS item_bid CASCADE;
DROP TABLE IF EXISTS global_attribute_value CASCADE;
DROP TABLE IF EXISTS global_attribute_group CASCADE;
DROP TABLE IF EXISTS config_profile CASCADE;
DROP TABLE IF EXISTS useracct_feedback CASCADE;
DROP TABLE IF EXISTS useracct_watch CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS useracct_attributes CASCADE;
DROP TABLE IF EXISTS useracct CASCADE;
DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS category CASCADE;

-- ================================================================
-- CONFIG_PROFILE
-- ================================================================

CREATE TABLE config_profile (
    cfp_scale_factor        float          NOT NULL,
    cfp_loader_start        timestamp DEFAULT CURRENT_TIMESTAMP,
    cfp_loader_stop         timestamp DEFAULT CURRENT_TIMESTAMP,
    cfp_user_item_histogram text NOT NULL
);

-- ================================================================
-- REGION
-- Represents regions of users
-- r_id             Region's ID
-- r_name           Region's name
-- ================================================================

CREATE TABLE region (
    r_id   bigint NOT NULL,
    r_name varchar(32),
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

CREATE TABLE useracct (
    u_id       varchar(128) NOT NULL,
    u_rating   bigint NOT NULL,
    u_balance  float  NOT NULL,
    u_comments integer   DEFAULT 0,
    u_r_id     bigint NOT NULL,
    u_created  timestamp DEFAULT CURRENT_TIMESTAMP,
    u_updated  timestamp DEFAULT CURRENT_TIMESTAMP,
    u_sattr0   varchar(64),
    u_sattr1   varchar(64),
    u_sattr2   varchar(64),
    u_sattr3   varchar(64),
    u_sattr4   varchar(64),
    u_sattr5   varchar(64),
    u_sattr6   varchar(64),
    u_sattr7   varchar(64),
    u_iattr0   bigint    DEFAULT NULL,
    u_iattr1   bigint    DEFAULT NULL,
    u_iattr2   bigint    DEFAULT NULL,
    u_iattr3   bigint    DEFAULT NULL,
    u_iattr4   bigint    DEFAULT NULL,
    u_iattr5   bigint    DEFAULT NULL,
    u_iattr6   bigint    DEFAULT NULL,
    u_iattr7   bigint    DEFAULT NULL,
    FOREIGN KEY (u_r_id) REFERENCES region (r_id) ON DELETE CASCADE,
    PRIMARY KEY (u_id)
);
CREATE INDEX idx_useracct_region ON useracct (u_id, u_r_id);

-- ================================================================
-- USERACCT_ATTRIBUTES
-- Represents user's attributes 
-- ================================================================

CREATE TABLE useracct_attributes (
    ua_id     bigint      NOT NULL,
    ua_u_id   varchar(128)      NOT NULL,
    ua_name   varchar(32) NOT NULL,
    ua_value  varchar(32) NOT NULL,
    u_created timestamp,
    FOREIGN KEY (ua_u_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    PRIMARY KEY (ua_id, ua_u_id)
);

-- ================================================================
-- CATEGORY
-- Represents merchandises' categories. Category can be hierarchical aligned using c_parent_id.
-- c_id                Category's ID
-- c_name            Category's name
-- c_parent_id        Parent category's ID
-- ================================================================

CREATE TABLE category (
    c_id        bigint NOT NULL,
    c_name      varchar(50),
    c_parent_id bigint,
    PRIMARY KEY (c_id),
    FOREIGN KEY (c_parent_id) REFERENCES category (c_id) ON DELETE CASCADE
);
CREATE INDEX idx_category_parent ON category (c_parent_id);

-- ================================================================
-- GLOBAL_ATTRIBUTE_GROUP
-- Represents merchandises' global attribute groups (for example, brand, material, feature etc.).
-- gag_id            Global attribute group's ID
-- gag_c_id            Associated Category's ID
-- gag_name            Global attribute group's name
-- ================================================================

CREATE TABLE global_attribute_group (
    gag_id   varchar(128)       NOT NULL,
    gag_c_id bigint       NOT NULL,
    gag_name varchar(100) NOT NULL,
    FOREIGN KEY (gag_c_id) REFERENCES category (c_id) ON DELETE CASCADE,
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

CREATE TABLE global_attribute_value (
    gav_id     varchar(128)       NOT NULL,
    gav_gag_id varchar(128)       NOT NULL,
    gav_name   varchar(100) NOT NULL,
    FOREIGN KEY (gav_gag_id) REFERENCES global_attribute_group (gag_id) ON DELETE CASCADE,
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
-- i_status            Items' status (0 = open, 1 = wait for purchase, 2 = close)
-- ================================================================

CREATE TABLE item (
    i_id               varchar(128) NOT NULL,
    i_u_id             varchar(128) NOT NULL,
    i_c_id             bigint NOT NULL,
    i_name             varchar(100),
    i_description      varchar(1024),
    i_user_attributes  varchar(255) DEFAULT NULL,
    i_initial_price    float  NOT NULL,
    i_current_price    float  NOT NULL,
    i_num_bids         bigint,
    i_num_images       bigint,
    i_num_global_attrs bigint,
    i_num_comments     bigint,
    i_start_date       timestamp    DEFAULT '1970-01-01 00:00:01',
    i_end_date         timestamp    DEFAULT '1970-01-01 00:00:01',
    i_status           int          DEFAULT 0,
    i_created          timestamp    DEFAULT CURRENT_TIMESTAMP,
    i_updated          timestamp    DEFAULT CURRENT_TIMESTAMP,
    i_iattr0           bigint       DEFAULT NULL,
    i_iattr1           bigint       DEFAULT NULL,
    i_iattr2           bigint       DEFAULT NULL,
    i_iattr3           bigint       DEFAULT NULL,
    i_iattr4           bigint       DEFAULT NULL,
    i_iattr5           bigint       DEFAULT NULL,
    i_iattr6           bigint       DEFAULT NULL,
    i_iattr7           bigint       DEFAULT NULL,
    FOREIGN KEY (i_u_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    FOREIGN KEY (i_c_id) REFERENCES category (c_id) ON DELETE CASCADE,
    PRIMARY KEY (i_id, i_u_id)
);
CREATE INDEX idx_item_seller ON item (i_u_id);

-- ================================================================
-- ITEM_ATTRIBUTE
-- Represents mappings between attribute values and items
-- ia_id            Item attribute's ID
-- ia_i_id            Item's ID
-- ia_gav_id        Global attribute value's ID
-- ================================================================

CREATE TABLE item_attribute (
    ia_id     varchar(128) NOT NULL,
    ia_i_id   varchar(128) NOT NULL,
    ia_u_id   varchar(128) NOT NULL,
    ia_gav_id varchar(128) NOT NULL,
    ia_gag_id varchar(128) NOT NULL,
    ia_sattr0 varchar(64) DEFAULT NULL,
    FOREIGN KEY (ia_i_id, ia_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
    FOREIGN KEY (ia_gav_id, ia_gag_id) REFERENCES global_attribute_value (gav_id, gav_gag_id),
    PRIMARY KEY (ia_id, ia_i_id, ia_u_id)
);

-- ================================================================
-- ITEM_IMAGE
-- Represents images of items
-- ii_id            Image's ID
-- ii_i_id            Item's ID
-- ii_path            Image's path
-- ================================================================

CREATE TABLE item_image (
    ii_id     varchar(128)       NOT NULL,
    ii_i_id   varchar(128)       NOT NULL,
    ii_u_id   varchar(128)       NOT NULL,
    ii_sattr0 varchar(128) NOT NULL,
    FOREIGN KEY (ii_i_id, ii_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
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

CREATE TABLE item_comment (
    ic_id       bigint       NOT NULL,
    ic_i_id     varchar(128)       NOT NULL,
    ic_u_id     varchar(128)       NOT NULL,
    ic_buyer_id varchar(128)       NOT NULL,
    ic_question varchar(128) NOT NULL,
    ic_response varchar(128) DEFAULT NULL,
    ic_created  timestamp    DEFAULT CURRENT_TIMESTAMP,
    ic_updated  timestamp    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ic_i_id, ic_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
    FOREIGN KEY (ic_buyer_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
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

CREATE TABLE item_bid (
    ib_id       bigint NOT NULL,
    ib_i_id     varchar(128) NOT NULL,
    ib_u_id     varchar(128) NOT NULL,
    ib_buyer_id varchar(128) NOT NULL,
    ib_bid      float  NOT NULL,
    ib_max_bid  float  NOT NULL,
    ib_created  timestamp DEFAULT CURRENT_TIMESTAMP,
    ib_updated  timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ib_i_id, ib_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
    FOREIGN KEY (ib_buyer_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    PRIMARY KEY (ib_id, ib_i_id, ib_u_id)
);

-- ================================================================
-- ITEM_MAX_BID
-- Cross-reference table to the current max bid for an auction
-- ================================================================

CREATE TABLE item_max_bid (
    imb_i_id    varchar(128) NOT NULL,
    imb_u_id    varchar(128) NOT NULL,
    imb_ib_id   bigint NOT NULL,
    imb_ib_i_id varchar(128) NOT NULL,
    imb_ib_u_id varchar(128) NOT NULL,
    imb_created timestamp DEFAULT CURRENT_TIMESTAMP,
    imb_updated timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (imb_i_id, imb_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
    FOREIGN KEY (imb_ib_id, imb_ib_i_id, imb_ib_u_id) REFERENCES item_bid (ib_id, ib_i_id, ib_u_id) ON DELETE CASCADE,
    PRIMARY KEY (imb_i_id, imb_u_id)
);

-- ================================================================
-- ITEM_PURCHASE
-- Represents purchase transaction (buy_now bid or win bid)
-- ip_id               Purchase's ID
-- ip_ib_id            Bid's ID
-- ip_date             Purchase's date
-- ================================================================

CREATE TABLE item_purchase (
    ip_id      bigint NOT NULL,
    ip_ib_id   bigint NOT NULL,
    ip_ib_i_id varchar(128) NOT NULL,
    ip_ib_u_id varchar(128) NOT NULL,
    ip_date    timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ip_ib_id, ip_ib_i_id, ip_ib_u_id) REFERENCES item_bid (ib_id, ib_i_id, ib_u_id) ON DELETE CASCADE,
    PRIMARY KEY (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id)
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

CREATE TABLE useracct_feedback (
    uf_u_id    varchar(128)      NOT NULL,
    uf_i_id    varchar(128)      NOT NULL,
    uf_i_u_id  varchar(128)      NOT NULL,
    uf_from_id varchar(128)      NOT NULL,
    uf_rating  tinyint     NOT NULL,
    uf_date    timestamp DEFAULT CURRENT_TIMESTAMP,
    uf_sattr0  varchar(80) NOT NULL,
    FOREIGN KEY (uf_i_id, uf_i_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
    FOREIGN KEY (uf_u_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    FOREIGN KEY (uf_from_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    PRIMARY KEY (uf_u_id, uf_i_id, uf_i_u_id, uf_from_id),
    CHECK (uf_u_id <> uf_from_id)
);

-- ================================================================
-- USERACCT_ITEM
-- The items that a user has recently purchased
-- ================================================================

CREATE TABLE useracct_item (
    ui_u_id       varchar(128) NOT NULL,
    ui_i_id       varchar(128) NOT NULL,
    ui_i_u_id     varchar(128) NOT NULL,
    ui_ip_id      bigint,
    ui_ip_ib_id   bigint,
    ui_ip_ib_i_id varchar(128),
    ui_ip_ib_u_id varchar(128),
    ui_created    timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ui_u_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    FOREIGN KEY (ui_i_id, ui_i_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
    FOREIGN KEY (ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id) REFERENCES item_purchase (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id) ON DELETE CASCADE,
    PRIMARY KEY (ui_u_id, ui_i_id, ui_i_u_id)
);
-- CREATE INDEX IDX_USERACCT_ITEM_ID ON USERACCT_ITEM (ui_i_id);

-- ================================================================
-- USERACCT_WATCH
-- The items that a user is watching
-- ================================================================

CREATE TABLE useracct_watch (
    uw_u_id    varchar(128) NOT NULL,
    uw_i_id    varchar(128) NOT NULL,
    uw_i_u_id  varchar(128) NOT NULL,
    uw_created timestamp DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (uw_i_id, uw_i_u_id) REFERENCES item (i_id, i_u_id) ON DELETE CASCADE,
    FOREIGN KEY (uw_u_id) REFERENCES useracct (u_id) ON DELETE CASCADE,
    PRIMARY KEY (uw_u_id, uw_i_id, uw_i_u_id)
);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;