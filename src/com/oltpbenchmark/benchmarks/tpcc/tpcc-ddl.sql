-- MySQL dump 10.13  Distrib 5.1.49, for debian-linux-gnu (x86_64)
--
-- Host: 127.0.0.1    Database: tpcc
-- ------------------------------------------------------
-- Server version	5.5.7-rc

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `cputable`
--

DROP TABLE IF EXISTS `cputable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cputable` (
  `empid` int(11) NOT NULL,
  `passwd` char(255) NOT NULL,
  PRIMARY KEY (`empid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `customer`
--

DROP TABLE IF EXISTS `customer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `customer` (
  `c_w_id` int(11) NOT NULL,
  `c_d_id` int(11) NOT NULL,
  `c_id` int(11) NOT NULL,
  `c_discount` decimal(4,4) NOT NULL,
  `c_credit` char(2) NOT NULL,
  `c_last` varchar(16) NOT NULL,
  `c_first` varchar(16) NOT NULL,
  `c_credit_lim` decimal(12,2) NOT NULL,
  `c_balance` decimal(12,2) NOT NULL,
  `c_ytd_payment` float NOT NULL,
  `c_payment_cnt` int(11) NOT NULL,
  `c_delivery_cnt` int(11) NOT NULL,
  `c_street_1` varchar(20) NOT NULL,
  `c_street_2` varchar(20) NOT NULL,
  `c_city` varchar(20) NOT NULL,
  `c_state` char(2) NOT NULL,
  `c_zip` char(9) NOT NULL,
  `c_phone` char(16) NOT NULL,
  `c_since` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `c_middle` char(2) NOT NULL,
  `c_data` varchar(500) NOT NULL,
  PRIMARY KEY (`c_w_id`,`c_d_id`,`c_id`),
  KEY `ndx_customer_name` (`c_w_id`,`c_d_id`,`c_last`,`c_first`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `district`
--

DROP TABLE IF EXISTS `district`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `district` (
  `d_w_id` int(11) NOT NULL,
  `d_id` int(11) NOT NULL,
  `d_ytd` decimal(12,2) NOT NULL,
  `d_tax` decimal(4,4) NOT NULL,
  `d_next_o_id` int(11) NOT NULL,
  `d_name` varchar(10) NOT NULL,
  `d_street_1` varchar(20) NOT NULL,
  `d_street_2` varchar(20) NOT NULL,
  `d_city` varchar(20) NOT NULL,
  `d_state` char(2) NOT NULL,
  `d_zip` char(9) NOT NULL,
  PRIMARY KEY (`d_w_id`,`d_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `history`
--

DROP TABLE IF EXISTS `history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `history` (
  `h_c_id` int(11) NOT NULL,
  `h_c_d_id` int(11) NOT NULL,
  `h_c_w_id` int(11) NOT NULL,
  `h_d_id` int(11) NOT NULL,
  `h_w_id` int(11) NOT NULL,
  `h_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `h_amount` decimal(6,2) NOT NULL,
  `h_data` varchar(24) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `iotable`
--

DROP TABLE IF EXISTS `iotable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `iotable` (
  `empid` int(11) NOT NULL,
  `data1` char(255) NOT NULL,
  `data2` char(255) NOT NULL,
  `data3` char(255) NOT NULL,
  `data4` char(255) NOT NULL,
  `data5` char(255) NOT NULL,
  `data6` char(255) NOT NULL,
  `data7` char(255) NOT NULL,
  `data8` char(255) NOT NULL,
  `data9` char(255) NOT NULL,
  `data10` char(255) NOT NULL,
  `data11` char(255) NOT NULL,
  `data12` char(255) NOT NULL,
  `data13` char(255) NOT NULL,
  `data14` char(255) NOT NULL,
  `data15` char(255) NOT NULL,
  `data16` char(255) NOT NULL,
  PRIMARY KEY (`empid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `iotableSmallrow`
--

DROP TABLE IF EXISTS `iotableSmallrow`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `iotableSmallrow` (
  `empid` int(11) NOT NULL,
  `flag1` int(11) NOT NULL,
  PRIMARY KEY (`empid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item`
--

DROP TABLE IF EXISTS `item`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `item` (
  `i_id` int(11) NOT NULL,
  `i_name` varchar(24) NOT NULL,
  `i_price` decimal(5,2) NOT NULL,
  `i_data` varchar(50) NOT NULL,
  `i_im_id` int(11) NOT NULL,
  PRIMARY KEY (`i_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `locktable`
--

DROP TABLE IF EXISTS `locktable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `locktable` (
  `empid` int(11) NOT NULL,
  `salary` int(11) NOT NULL,
  PRIMARY KEY (`empid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `new_order`
--

DROP TABLE IF EXISTS `new_order`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `new_order` (
  `no_w_id` int(11) NOT NULL,
  `no_d_id` int(11) NOT NULL,
  `no_o_id` int(11) NOT NULL,
  PRIMARY KEY (`no_w_id`,`no_d_id`,`no_o_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `oorder`
--

DROP TABLE IF EXISTS `oorder`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `oorder` (
  `o_w_id` int(11) NOT NULL,
  `o_d_id` int(11) NOT NULL,
  `o_id` int(11) NOT NULL,
  `o_c_id` int(11) NOT NULL,
  `o_carrier_id` int(11) DEFAULT NULL,
  `o_ol_cnt` decimal(2,0) NOT NULL,
  `o_all_local` decimal(1,0) NOT NULL,
  `o_entry_d` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`o_w_id`,`o_d_id`,`o_id`),
  UNIQUE KEY `ndx_oorder_c_id` (`o_w_id`,`o_d_id`,`o_c_id`,`o_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `order_line`
--

DROP TABLE IF EXISTS `order_line`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `order_line` (
  `ol_w_id` int(11) NOT NULL,
  `ol_d_id` int(11) NOT NULL,
  `ol_o_id` int(11) NOT NULL,
  `ol_number` int(11) NOT NULL,
  `ol_i_id` int(11) NOT NULL,
  `ol_delivery_d` timestamp NULL DEFAULT NULL,
  `ol_amount` decimal(6,2) NOT NULL,
  `ol_supply_w_id` int(11) NOT NULL,
  `ol_quantity` decimal(2,0) NOT NULL,
  `ol_dist_info` char(24) NOT NULL,
  PRIMARY KEY (`ol_w_id`,`ol_d_id`,`ol_o_id`,`ol_number`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock`
--

DROP TABLE IF EXISTS `stock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stock` (
  `s_w_id` int(11) NOT NULL,
  `s_i_id` int(11) NOT NULL,
  `s_quantity` decimal(4,0) NOT NULL,
  `s_ytd` decimal(8,2) NOT NULL,
  `s_order_cnt` int(11) NOT NULL,
  `s_remote_cnt` int(11) NOT NULL,
  `s_data` varchar(50) NOT NULL,
  `s_dist_01` char(24) NOT NULL,
  `s_dist_02` char(24) NOT NULL,
  `s_dist_03` char(24) NOT NULL,
  `s_dist_04` char(24) NOT NULL,
  `s_dist_05` char(24) NOT NULL,
  `s_dist_06` char(24) NOT NULL,
  `s_dist_07` char(24) NOT NULL,
  `s_dist_08` char(24) NOT NULL,
  `s_dist_09` char(24) NOT NULL,
  `s_dist_10` char(24) NOT NULL,
  PRIMARY KEY (`s_w_id`,`s_i_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `warehouse`
--

DROP TABLE IF EXISTS `warehouse`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `warehouse` (
  `w_id` int(11) NOT NULL,
  `w_ytd` decimal(12,2) NOT NULL,
  `w_tax` decimal(4,4) NOT NULL,
  `w_name` varchar(10) NOT NULL,
  `w_street_1` varchar(20) NOT NULL,
  `w_street_2` varchar(20) NOT NULL,
  `w_city` varchar(20) NOT NULL,
  `w_state` char(2) NOT NULL,
  `w_zip` char(9) NOT NULL,
  PRIMARY KEY (`w_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2011-11-06 23:49:43
