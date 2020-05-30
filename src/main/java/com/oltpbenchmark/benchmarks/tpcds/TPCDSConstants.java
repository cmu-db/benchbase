/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.tpcds;

public abstract class TPCDSConstants {
    public static final String TABLENAME_CALLCENTER = "call_center";
    public static final String TABLENAME_CATALOGPAGE = "catalog_page";
    public static final String TABLENAME_CATALOGRETURNS = "catalog_returns";
    public static final String TABLENAME_CATALOGSALES = "catalog_sales";
    public static final String TABLENAME_CUSTOMER = "customer";
    public static final String TABLENAME_CUSTOMERADDRESS = "customer_address";
    public static final String TABLENAME_CUSTOMERDEM = "customer_demographics";
    public static final String TABLENAME_DATEDIM = "date_dim";
    public static final String TABLENAME_HOUSEHOLDDEM = "household_demographics";
    public static final String TABLENAME_INCOMEBAND = "income_band";
    public static final String TABLENAME_INVENTORY = "inventory";
    public static final String TABLENAME_ITEM = "item";
    public static final String TABLENAME_PROMOTION = "promotion";
    public static final String TABLENAME_REASON = "reason";
    public static final String TABLENAME_SHIPMODE = "ship_mode";
    public static final String TABLENAME_STORE = "store";
    public static final String TABLENAME_STORERETURNS = "store_returns";
    public static final String TABLENAME_STORESALES = "store_sales";
    public static final String TABLENAME_TIMEDIM = "time_dim";
    public static final String TABLENAME_WAREHOUSE = "warehouse";
    public static final String TABLENAME_WEBPAGE = "web_page";
    public static final String TABLENAME_WEBRETURNS = "web_returns";
    public static final String TABLENAME_WEBSALES = "web_sales";
    public static final String TABLENAME_WEBSITE = "web_site";

    public enum CastTypes {LONG, DOUBLE, STRING, DATE}

    public static final CastTypes[] callcenterTypes = {
            CastTypes.LONG,   // cc_call_center_sk
            CastTypes.STRING, // cc_call_center_id
            CastTypes.DATE,   // cc_rec_start_date
            CastTypes.DATE,   // cc_rec_end_date
            CastTypes.LONG,   // cc_closed_date_sk
            CastTypes.LONG,   // cc_open_date_sk
            CastTypes.STRING, // cc_name
            CastTypes.STRING, // cc_class
            CastTypes.LONG,   // cc_employees
            CastTypes.LONG,   // cc_sq_ft
            CastTypes.STRING, // cc_hours
            CastTypes.STRING, // cc_manager
            CastTypes.LONG,   // cc_mkt_id
            CastTypes.STRING, // cc_mkt_class
            CastTypes.STRING, // cc_mkt_desc_text
            CastTypes.STRING, // cc_market_manager
            CastTypes.LONG,   // cc_division
            CastTypes.STRING, // cc_division_name
            CastTypes.LONG,   // cc_company
            CastTypes.STRING, // cc_company_name
            CastTypes.STRING, // cc_street_number
            CastTypes.STRING, // cc_street_name
            CastTypes.STRING, // cc_street_type
            CastTypes.STRING, // cc_suite_number
            CastTypes.STRING, // cc_city
            CastTypes.STRING, // s_state
            CastTypes.STRING, // cc_county
            CastTypes.STRING, // cc_state
            CastTypes.STRING, // cc_zip_text
            CastTypes.STRING, // cc_country
            CastTypes.DOUBLE, // cc_gmt_offset
            CastTypes.DOUBLE  // cc_tax_percentage
    };

    public static final CastTypes[] catalogpageTypes = {
            CastTypes.LONG,   // cp_catalog_page_sk
            CastTypes.STRING, // cp_catalog_page_id
            CastTypes.LONG,   // cp_start_date_sk
            CastTypes.LONG,   // cp_end_date_sk
            CastTypes.STRING, // cp_department
            CastTypes.LONG,   // cp_catalog_number
            CastTypes.LONG,   // cp_catalog_page_number
            CastTypes.STRING, // cp_description
            CastTypes.STRING  // cp_type
    };

    public static final CastTypes[] catalogreturnsTypes = {
            CastTypes.LONG,   // cr_returned_date_sk
            CastTypes.LONG,   // cr_returned_time_sk
            CastTypes.LONG,   // cr_item_sk
            CastTypes.LONG,   // cr_refunded_customer_sk
            CastTypes.LONG,   // cr_refunded_cdemo_sk
            CastTypes.LONG,   // cr_refunded_hdemo_sk
            CastTypes.LONG,   // cr_refunded_addr_sk
            CastTypes.LONG,   // cr_returning_customer_sk
            CastTypes.LONG,   // cr_returning_cdemo_sk
            CastTypes.LONG,   // cr_returning_hdemo_sk
            CastTypes.LONG,   // cr_returning_addr_sk
            CastTypes.LONG,   // cr_call_center_sk
            CastTypes.LONG,   // cr_catalog_page_sk
            CastTypes.LONG,   // cr_ship_mode_sk
            CastTypes.LONG,   // cr_warehouse_sk
            CastTypes.LONG,   // cr_reason_sk
            CastTypes.LONG,   // cr_order_number
            CastTypes.LONG,   // cr_return_quantity
            CastTypes.DOUBLE, // cr_return_amount
            CastTypes.DOUBLE, // cr_return_tax
            CastTypes.DOUBLE, // cr_return_amt_inc_tax
            CastTypes.DOUBLE, // cr_fee
            CastTypes.DOUBLE, // cr_return_ship_cost
            CastTypes.DOUBLE, // cr_refunded_cash
            CastTypes.DOUBLE, // cr_reversed_charge
            CastTypes.DOUBLE, // cr_store_credit
            CastTypes.DOUBLE  // cr_net_loss
    };

    public static final CastTypes[] catalogsalesTypes = {
            CastTypes.LONG,   // cs_sold_date_sk
            CastTypes.LONG,   // cs_sold_time_sk
            CastTypes.LONG,   // cs_ship_date_sk
            CastTypes.LONG,   // cs_bill_customer_sk
            CastTypes.LONG,   // cs_bill_cdemo_sk
            CastTypes.LONG,   // cs_bill_hdemo_sk
            CastTypes.LONG,   // cs_bill_addr_sk
            CastTypes.LONG,   // cs_ship_customer_sk
            CastTypes.LONG,   // cs_ship_cdemo_sk
            CastTypes.LONG,   // cs_ship_hdemo_sk
            CastTypes.LONG,   // cs_ship_addr_sk
            CastTypes.LONG,   // cs_call_center_sk
            CastTypes.LONG,   // cs_catalog_page_sk
            CastTypes.LONG,   // cs_ship_mode_sk
            CastTypes.LONG,   // cs_warehouse_sk
            CastTypes.LONG,   // cs_item_sk
            CastTypes.LONG,   // cs_promo_sk
            CastTypes.LONG,   // cs_order_number
            CastTypes.LONG,   // cs_quantity
            CastTypes.DOUBLE, // cs_wholesale_cost
            CastTypes.DOUBLE, // cs_list_price
            CastTypes.DOUBLE, // cs_sales_price
            CastTypes.DOUBLE, // cs_ext_discount_amt
            CastTypes.DOUBLE, // cs_ext_sales_price
            CastTypes.DOUBLE, // cs_ext_wholesale_cost
            CastTypes.DOUBLE, // cs_ext_list_price
            CastTypes.DOUBLE, // cs_ext_tax
            CastTypes.DOUBLE, // cs_coupon_amt
            CastTypes.DOUBLE, // cs_ext_ship_cost
            CastTypes.DOUBLE, // cs_net_paid
            CastTypes.DOUBLE, // cs_net_paid_inc_tax
            CastTypes.DOUBLE, // cs_net_paid_inc_ship
            CastTypes.DOUBLE, // cs_net_paid_inc_ship_tax
            CastTypes.DOUBLE  // cs_net_profit
    };

    public static final CastTypes[] customerTypes = {
            CastTypes.LONG,   // c_customer_sk
            CastTypes.STRING, // c_customer_id
            CastTypes.LONG,   // c_current_cdemo_sk
            CastTypes.LONG,   // c_current_hdemo_sk
            CastTypes.LONG,   // c_current_addr_sk
            CastTypes.LONG,   // c_first_shipto_date_sk
            CastTypes.LONG,   // c_first_sales_date_sk
            CastTypes.STRING, // c_salutation
            CastTypes.STRING, // c_first_name
            CastTypes.STRING, // c_last_name
            CastTypes.STRING, // c_preferred_cust_flag text
            CastTypes.LONG,   // c_birth_day
            CastTypes.LONG,   // c_birth_month
            CastTypes.LONG,   // c_birth_year
            CastTypes.STRING, // c_birth_country
            CastTypes.STRING, // c_login
            CastTypes.STRING, // c_email_address
            CastTypes.LONG    // c_last_review_date_sk
    };

    public static final CastTypes[] customeraddressTypes = {
            CastTypes.LONG,   // ca_address_sk
            CastTypes.STRING, // ca_address_id
            CastTypes.STRING, // ca_street_number
            CastTypes.STRING, // ca_street_name
            CastTypes.STRING, // ca_street_type
            CastTypes.STRING, // ca_suite_number
            CastTypes.STRING, // ca_city
            CastTypes.STRING, // ca_county
            CastTypes.STRING, // ca_state
            CastTypes.STRING, // ca_zip
            CastTypes.STRING, // ca_country
            CastTypes.DOUBLE, // ca_gmt_offset
            CastTypes.STRING  // ca_location_type
    };

    public static final CastTypes[] customerdemTypes = {
            CastTypes.LONG,   // cd_demo_sk
            CastTypes.STRING, // gender
            CastTypes.STRING, // cd_marital_status
            CastTypes.STRING, // cd_education_status
            CastTypes.LONG,   // cd_purchase_estimate
            CastTypes.STRING, // cd_credit_rating
            CastTypes.LONG,   // cd_dep_count
            CastTypes.LONG,   // cd_dep_employed_count
            CastTypes.LONG    // cd_dep_college_count
    };

    public static final CastTypes[] datedimTypes = {
            CastTypes.LONG,   // d_date_sk
            CastTypes.STRING, // d_date_id
            CastTypes.DATE,   // d_date
            CastTypes.LONG,   // d_month_seq
            CastTypes.LONG,   // d_week_seq
            CastTypes.LONG,   // d_quarter_seq
            CastTypes.LONG,   // d_year
            CastTypes.LONG,   // d_dow
            CastTypes.LONG,   // d_moy
            CastTypes.LONG,   // d_dom
            CastTypes.LONG,   // d_qoy
            CastTypes.LONG,   // d_fy_year
            CastTypes.LONG,   // d_fy_quarter_seq
            CastTypes.LONG,   // d_fy_week_seq
            CastTypes.STRING, // d_day_name
            CastTypes.STRING, // d_quarter_name
            CastTypes.STRING, // d_holiday
            CastTypes.STRING, // d_weekend
            CastTypes.STRING, // d_following_holiday
            CastTypes.LONG,   // d_first_dom
            CastTypes.LONG,   // d_last_dom
            CastTypes.LONG,   // d_same_day_ly
            CastTypes.LONG,   // d_same_day_lq
            CastTypes.STRING, // d_current_day
            CastTypes.STRING, // d_quarter_week
            CastTypes.STRING, // d_current_month
            CastTypes.STRING, // d_current_quarter
            CastTypes.STRING // d_current_year
    };

    public static final CastTypes[] householddemTypes = {
            CastTypes.LONG,   // hd_demo_sk
            CastTypes.LONG,   // hd_income_band_sk
            CastTypes.STRING, // hd_buy_potential
            CastTypes.LONG,   // hd_dep_count
            CastTypes.LONG,   // hd_vehicle_count
    };

    public static final CastTypes[] incomebandTypes = {
            CastTypes.LONG,   // ib_income_band_sk
            CastTypes.LONG,   // ib_lower_bound
            CastTypes.LONG    // ib_upper_bound
    };

    public static final CastTypes[] inventoryTypes = {
            CastTypes.LONG,   // inv_date_sk
            CastTypes.LONG,   // inv_item_sk
            CastTypes.LONG,   // inv_warehouse_sk
            CastTypes.LONG    // inv_quantity_on_hand
    };

    public static final CastTypes[] itemTypes = {
            CastTypes.LONG,   // i_item_sk
            CastTypes.STRING, // i_item_id
            CastTypes.DATE,   // i_rec_start_date
            CastTypes.DATE,   // i_rec_end_date
            CastTypes.STRING, // i_item_desc
            CastTypes.DOUBLE, // i_current_price
            CastTypes.DOUBLE, // i_wholesale_cost
            CastTypes.LONG,   // i_brand_id
            CastTypes.STRING, // i_brand
            CastTypes.LONG,   // i_class_id
            CastTypes.STRING, // i_class
            CastTypes.LONG,   // i_category_id
            CastTypes.STRING, // i_category
            CastTypes.LONG,   // i_manufact_id
            CastTypes.STRING, // i_manufact
            CastTypes.STRING, // i_size
            CastTypes.STRING, // i_formulation
            CastTypes.STRING, // i_color
            CastTypes.STRING, // i_units
            CastTypes.STRING, // i_container
            CastTypes.LONG,   // i_manager_id
            CastTypes.STRING  // i_product_name
    };

    public static final CastTypes[] promotionTypes = {
            CastTypes.LONG,   // p_promo_sk
            CastTypes.STRING, // p_promo_id
            CastTypes.LONG,   // p_start_date_sk
            CastTypes.LONG,   // p_end_date_sk
            CastTypes.LONG,   // p_item_sk
            CastTypes.DOUBLE, // p_cost
            CastTypes.LONG,   // p_response_target
            CastTypes.STRING, // p_promo_name
            CastTypes.STRING, // p_channel_dmail
            CastTypes.STRING, // p_channel_email
            CastTypes.STRING, // p_channel_catalog
            CastTypes.STRING, // p_channel_tv
            CastTypes.STRING, // p_channel_radio
            CastTypes.STRING, // p_channel_press
            CastTypes.STRING, // p_channel_event
            CastTypes.STRING, // p_channel_demo
            CastTypes.STRING, // p_channel_details
            CastTypes.STRING, // p_purpose
            CastTypes.STRING  // p_discount_active
    };

    public static final CastTypes[] reasonTypes = {
            CastTypes.LONG,   // r_reason_sk
            CastTypes.STRING, // r_reason_id
            CastTypes.STRING  // r_reason_desc
    };

    public static final CastTypes[] shipmodeTypes = {
            CastTypes.LONG,   // sm_ship_mode_sk
            CastTypes.STRING, // sm_ship_mode_id
            CastTypes.STRING, // sm_type
            CastTypes.STRING, // sm_code
            CastTypes.STRING, // sm_carrier
            CastTypes.STRING  // sm_contract
    };

    public static final CastTypes[] storeTypes = {
            CastTypes.LONG,   // s_store_sk
            CastTypes.STRING, // s_store_id
            CastTypes.DATE,   // s_rec_start_date
            CastTypes.DATE,   // s_rec_end_date
            CastTypes.LONG,   // s_closed_date_sk
            CastTypes.STRING, // s_store_name
            CastTypes.LONG,   // s_number_employees
            CastTypes.LONG,   // s_floor_space
            CastTypes.STRING, // s_hours
            CastTypes.STRING, // s_manager
            CastTypes.LONG,   // s_market_id
            CastTypes.STRING, // s_geography_class
            CastTypes.STRING, // s_market_desc
            CastTypes.STRING, // s_market_manager
            CastTypes.LONG,   // s_division_id
            CastTypes.STRING, // s_division_name
            CastTypes.LONG,   // s_company_id
            CastTypes.STRING, // s_company_name
            CastTypes.STRING, // s_street_number
            CastTypes.STRING, // s_street_name
            CastTypes.STRING, // s_street_type
            CastTypes.STRING, // s_suite_number
            CastTypes.STRING, // s_city
            CastTypes.STRING, // s_county
            CastTypes.STRING, // s_state
            CastTypes.STRING, // s_zip
            CastTypes.STRING, // s_country
            CastTypes.DOUBLE, // s_gmt_offset
            CastTypes.DOUBLE  // s_tax_percentage
    };

    public static final CastTypes[] storereturnsTypes = {
            CastTypes.LONG,   // sr_returned_date_sk
            CastTypes.LONG,   // sr_return_time_sk
            CastTypes.LONG,   // sr_item_sk
            CastTypes.LONG,   // sr_customer_sk
            CastTypes.LONG,   // sr_cdemo_sk
            CastTypes.LONG,   // sr_hdemo_sk
            CastTypes.LONG,   // sr_addr_sk
            CastTypes.LONG,   // sr_store_sk
            CastTypes.LONG,   // sr_reason_sk
            CastTypes.LONG,   // sr_ticket_number
            CastTypes.LONG,   // sr_return_quantity
            CastTypes.DOUBLE, // sr_return_amt
            CastTypes.DOUBLE, // sr_return_tax
            CastTypes.DOUBLE, // sr_return_amt_inc_tax
            CastTypes.DOUBLE, // sr_fee
            CastTypes.DOUBLE, // sr_return_ship_cost
            CastTypes.DOUBLE, // sr_refunded_cash
            CastTypes.DOUBLE, // sr_reversed_charge
            CastTypes.DOUBLE, // sr_store_credit
            CastTypes.DOUBLE  // sr_net_loss
    };

    public static final CastTypes[] storesalesTypes = {
            CastTypes.LONG,   // ss_sold_date_sk
            CastTypes.LONG,   // ss_sold_time_sk
            CastTypes.LONG,   // ss_item_sk
            CastTypes.LONG,   // ss_customer_sk
            CastTypes.LONG,   // ss_cdemo_sk
            CastTypes.LONG,   // ss_hdemo_sk
            CastTypes.LONG,   // ss_addr_sk
            CastTypes.LONG,   // ss_store_sk
            CastTypes.LONG,   // ss_promo_sk
            CastTypes.LONG,   // ss_ticket_number
            CastTypes.LONG,   // ss_quantity
            CastTypes.DOUBLE, // ss_wholesale_cost
            CastTypes.DOUBLE, // ss_list_price
            CastTypes.DOUBLE, // ss_sales_price
            CastTypes.DOUBLE, // ss_ext_discount_amt
            CastTypes.DOUBLE, // ss_ext_sales_price
            CastTypes.DOUBLE, // ss_ext_wholesale_cost
            CastTypes.DOUBLE, // ss_ext_list_price
            CastTypes.DOUBLE, // ss_ext_tax
            CastTypes.DOUBLE, // ss_coupon_amt
            CastTypes.DOUBLE, // ss_net_paid
            CastTypes.DOUBLE, // ss_net_paid_inc_tax
            CastTypes.DOUBLE  // ss_net_profit
    };

    public static final CastTypes[] timedimTypes = {
            CastTypes.LONG,   // t_time_sk
            CastTypes.STRING, // t_time_id
            CastTypes.LONG,   // t_time
            CastTypes.LONG,   // t_hour
            CastTypes.LONG,   // t_minute
            CastTypes.LONG,   // t_second
            CastTypes.STRING, // t_am_pm
            CastTypes.STRING, // t_shift
            CastTypes.STRING, // t_sub_shift
            CastTypes.STRING  // t_meal_time
    };

    public static final CastTypes[] warehouseTypes = {
            CastTypes.LONG,   // w_warehouse_sk
            CastTypes.STRING, // w_warehouse_id
            CastTypes.STRING, // w_warehouse_name
            CastTypes.LONG,   // w_warehouse_sq_ft
            CastTypes.STRING, // w_street_number
            CastTypes.STRING, // w_street_name
            CastTypes.STRING, // w_street_type
            CastTypes.STRING, // w_suite_number
            CastTypes.STRING, // w_city
            CastTypes.STRING, // w_county
            CastTypes.STRING, // w_state
            CastTypes.STRING, // w_zip
            CastTypes.STRING, // w_country
            CastTypes.DOUBLE  // w_gmt_offset
    };

    public static final CastTypes[] webpageTypes = {
            CastTypes.LONG,   // wp_web_page_sk
            CastTypes.STRING, // wp_web_page_id
            CastTypes.DATE,   // wp_rec_start_date
            CastTypes.DATE,   // wp_rec_end_date
            CastTypes.LONG,   // wp_creation_date_sk
            CastTypes.LONG,   // wp_access_date_sk
            CastTypes.STRING, // wp_autogen_flag
            CastTypes.LONG,   // wp_customer_sk
            CastTypes.STRING, // wp_url
            CastTypes.STRING, // wp_type
            CastTypes.LONG,   // wp_char_count
            CastTypes.LONG,   // wp_link_count
            CastTypes.LONG,   // wp_image_count
            CastTypes.LONG    // wp_max_ad_count
    };

    public static final CastTypes[] webreturnsTypes = {
            CastTypes.LONG,   // wr_returned_date_sk
            CastTypes.LONG,   // wr_return_time_sk
            CastTypes.LONG,   // wr_item_sk
            CastTypes.LONG,   // wr_refunded_customer_sk
            CastTypes.LONG,   // wr_refunded_cdemo_sk
            CastTypes.LONG,   // wr_refunded_hdemo_sk
            CastTypes.LONG,   // wr_refunded_addr_sk
            CastTypes.LONG,   // wr_returning_customer_sk
            CastTypes.LONG,   // wr_returning_cdemo_sk
            CastTypes.LONG,   // wr_returning_hdemo_sk
            CastTypes.LONG,   // wr_returning_addr_sk
            CastTypes.LONG,   // wr_web_page_sk
            CastTypes.LONG,   // wr_reason_sk
            CastTypes.LONG,   // wr_order_number
            CastTypes.LONG,   // wr_return_quantity
            CastTypes.DOUBLE, // wr_return_amt
            CastTypes.DOUBLE, // wr_return_tax
            CastTypes.DOUBLE, // wr_return_amt_inc_tax
            CastTypes.DOUBLE, // wr_fee
            CastTypes.DOUBLE, // wr_return_ship_cost
            CastTypes.DOUBLE, // wr_refunded_cash
            CastTypes.DOUBLE, // wr_reversed_charge
            CastTypes.DOUBLE, // wr_store_credit
            CastTypes.DOUBLE  // wr_net_loss
    };

    public static final CastTypes[] websalesTypes = {
            CastTypes.LONG,   // ws_sold_date_sk
            CastTypes.LONG,   // ws_sold_time_sk
            CastTypes.LONG,   // ws_ship_date_sk
            CastTypes.LONG,   // ws_item_sk
            CastTypes.LONG,   // ws_bill_customer_sk
            CastTypes.LONG,   // ws_bill_cdemo_sk
            CastTypes.LONG,   // ws_bill_hdemo_sk
            CastTypes.LONG,   // ws_bill_addr_sk
            CastTypes.LONG,   // ws_ship_customer_sk
            CastTypes.LONG,   // ws_ship_cdemo_sk
            CastTypes.LONG,   // ws_ship_hdemo_sk
            CastTypes.LONG,   // ws_ship_addr_sk
            CastTypes.LONG,   // ws_web_page_sk
            CastTypes.LONG,   // ws_web_site_sk
            CastTypes.LONG,   // ws_ship_mode_sk
            CastTypes.LONG,   // ws_warehouse_sk
            CastTypes.LONG,   // ws_promo_sk
            CastTypes.LONG,   // ws_order_number
            CastTypes.LONG,   // ws_quantity
            CastTypes.DOUBLE, // ws_wholesale_cost
            CastTypes.DOUBLE, // ws_list_price
            CastTypes.DOUBLE, // ws_sales_price
            CastTypes.DOUBLE, // ws_ext_discount_amt
            CastTypes.DOUBLE, // ws_ext_sales_price
            CastTypes.DOUBLE, // ws_ext_wholesale_cost
            CastTypes.DOUBLE, // ws_ext_list_price
            CastTypes.DOUBLE, // ws_ext_tax
            CastTypes.DOUBLE, // ws_coupon_amt
            CastTypes.DOUBLE, // ws_ext_ship_cost
            CastTypes.DOUBLE, // ws_net_paid
            CastTypes.DOUBLE, // ws_net_paid_inc_tax
            CastTypes.DOUBLE, // ws_net_paid_inc_ship
            CastTypes.DOUBLE, // ws_net_paid_inc_ship_tax
            CastTypes.DOUBLE  // ws_net_profit
    };

    public static final CastTypes[] websiteTypes = {
            CastTypes.LONG,   // web_site_sk
            CastTypes.STRING, // web_site_id
            CastTypes.DATE,   // web_rec_start_date
            CastTypes.DATE,   // web_rec_end_date
            CastTypes.STRING, // web_name
            CastTypes.LONG,   // web_open_date_sk
            CastTypes.LONG,   // web_close_date_sk
            CastTypes.STRING, // web_class
            CastTypes.STRING, // web_manager
            CastTypes.LONG,   // web_mkt_id
            CastTypes.STRING, // web_mkt_class
            CastTypes.STRING, // web_mkt_desc
            CastTypes.STRING, // web_market_manager
            CastTypes.LONG,   // web_company_id
            CastTypes.STRING, // web_company_name
            CastTypes.STRING, // web_street_number
            CastTypes.STRING, // web_street_name
            CastTypes.STRING, // web_street_type
            CastTypes.STRING, // web_suite_number
            CastTypes.STRING, // web_city
            CastTypes.STRING, // web_county
            CastTypes.STRING, // web_state
            CastTypes.STRING, // web_zip
            CastTypes.STRING, // web_country
            CastTypes.DOUBLE, // web_gmt_offset
            CastTypes.DOUBLE // web_tax_percentage
    };

}
