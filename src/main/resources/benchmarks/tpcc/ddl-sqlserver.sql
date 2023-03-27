--  drop existing tables
if object_id('warehouse') is not null drop table warehouse;
if object_id('stock') is not null drop table stock;
if object_id('order_line') is not null drop table order_line;
if object_id('oorder') is not null drop table oorder;
if object_id('new_order') is not null drop table new_order;
if object_id('item') is not null drop table item;
if object_id('history') is not null drop table history;
if object_id('district') is not null drop table district;
if object_id('customer') is not null drop table customer;

-- create tables

create table [dbo].[warehouse](
	[w_id] [int] not null,
	[w_ytd] [decimal](12, 2) not null,
	[w_tax] [decimal](4, 4) not null,
	[w_name] [varchar](10) not null,
	[w_street_1] [varchar](20) not null,
	[w_street_2] [varchar](20) not null,
	[w_city] [varchar](20) not null,
	[w_state] [char](2) not null,
	[w_zip] [char](9) not null,
	primary key (w_id)
);

create table [dbo].[stock](
	[s_w_id] [int] not null,
	[s_i_id] [int] not null,
	[s_quantity] [decimal](4, 0) not null,
	[s_ytd] [decimal](8, 2) not null,
	[s_order_cnt] [int] not null,
	[s_remote_cnt] [int] not null,
	[s_data] [varchar](50) not null,
	[s_dist_01] [char](24) not null,
	[s_dist_02] [char](24) not null,
	[s_dist_03] [char](24) not null,
	[s_dist_04] [char](24) not null,
	[s_dist_05] [char](24) not null,
	[s_dist_06] [char](24) not null,
	[s_dist_07] [char](24) not null,
	[s_dist_08] [char](24) not null,
	[s_dist_09] [char](24) not null,
	[s_dist_10] [char](24) not null,
	primary key (s_w_id,s_i_id)
);

create table [dbo].[order_line](
	[ol_w_id] [int] not null,
	[ol_d_id] [int] not null,
	[ol_o_id] [int] not null,
	[ol_number] [int] not null,
	[ol_i_id] [int] not null,
	[ol_delivery_d] [datetime] null,
	[ol_amount] [decimal](6, 2) not null,
	[ol_supply_w_id] [int] not null,
	[ol_quantity] [decimal](2, 0) not null,
	[ol_dist_info] [char](24) not null,
	primary key (ol_w_id,ol_d_id,ol_o_id,ol_number)
); 

create table [dbo].[oorder](
	[o_w_id] [int] not null,
	[o_d_id] [int] not null,
	[o_id] [int] not null,
	[o_c_id] [int] not null,
	[o_carrier_id] [int] null,
	[o_ol_cnt] [decimal](2, 0) not null,
	[o_all_local] [decimal](1, 0) not null,
	[o_entry_d] [datetime] null,
	primary key (o_w_id,o_d_id,o_id),
    unique (o_w_id,o_d_id,o_c_id,o_id)
);

create table [dbo].[new_order](
	[no_w_id] [int] not null,
	[no_d_id] [int] not null,
	[no_o_id] [int] not null,
	primary key (no_w_id,no_d_id,no_o_id)
);

create table [dbo].[item](
	[i_id] [int] not null,
	[i_name] [varchar](24) not null,
	[i_price] [decimal](5, 2) not null,
	[i_data] [varchar](50) not null,
	[i_im_id] [int] not null,
	primary key (i_id)
);

create table [dbo].[history](
	[h_c_id] [int] not null,
	[h_c_d_id] [int] not null,
	[h_c_w_id] [int] not null,
	[h_d_id] [int] not null,
	[h_w_id] [int] not null,
	[h_date] [datetime] null,
	[h_amount] [decimal](6, 2) not null,
	[h_data] [varchar](24) not null
);

create table [dbo].[district](
	[d_w_id] [int] not null,
	[d_id] [int] not null,
	[d_ytd] [decimal](12, 2) not null,
	[d_tax] [decimal](4, 4) not null,
	[d_next_o_id] [int] not null,
	[d_name] [varchar](10) not null,
	[d_street_1] [varchar](20) not null,
	[d_street_2] [varchar](20) not null,
	[d_city] [varchar](20) not null,
	[d_state] [char](2) not null,
	[d_zip] [char](9) not null,
	primary key (d_w_id,d_id)
);


create table [dbo].[customer](
	[c_w_id] [int] not null,
	[c_d_id] [int] not null,
	[c_id] [int] not null,
	[c_discount] [decimal](4, 4) not null,
	[c_credit] [char](2) not null,
	[c_last] [varchar](16) not null,
	[c_first] [varchar](16) not null,
	[c_credit_lim] [decimal](12, 2) not null,
	[c_balance] [decimal](12, 2) not null,
	[c_ytd_payment] [real] not null,
	[c_payment_cnt] [int] not null,
	[c_delivery_cnt] [int] not null,
	[c_street_1] [varchar](20) not null,
	[c_street_2] [varchar](20) not null,
	[c_city] [varchar](20) not null,
	[c_state] [char](2) not null,
	[c_zip] [char](9) not null,
	[c_phone] [char](16) not null,
	[c_since] [datetime] null,
	[c_middle] [char](2) not null,
	[c_data] [varchar](500) not null,
	primary key (c_w_id,c_d_id,c_id)
);

-- create indexes
create index idx_customer_name on [dbo].[customer] (c_w_id,c_d_id,c_last,c_first);
