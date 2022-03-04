package com.oltpbenchmark.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oltpbenchmark.api.config.Configuration;
import com.oltpbenchmark.api.config.Database;
import com.oltpbenchmark.api.config.Dialect;
import com.oltpbenchmark.api.config.Workload;
import junit.framework.TestCase;

public class TestConfigurationLoad extends TestCase {


    private static final String DATABASE_CONFIGURATION = """
            <?xml version="1.0" encoding="utf-8"?>
            <database xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="database.xsd">
                <type>HSQLDB</type>
                <driverClass>org.hsqldb.jdbc.JDBCDriver</driverClass>
                <url>jdbc:hsqldb:mem:benchbase;sql.syntax_mys=true</url>
                <username>admin</username>
                <password>password</password>
                <transactionIsolation>TRANSACTION_SERIALIZABLE</transactionIsolation>
                <batchSize>128</batchSize>
                <retries>3</retries>
            </database>
            """;

    private static final String WORKLOAD_CONFIGURATION = """
            <?xml version="1.0" encoding="utf-8"?>
            <configuration xsi:noNamespaceSchemaLocation="workload.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                 
                 <workloads>       
                <workload benchmarkClass="com.oltpbenchmark.benchmarks.chbenchmark.CHBenCHmark">
                    <scaleFactor>1</scaleFactor>
                    <terminals>1</terminals>
                        
                    <phases>
                        <phase>
                            <time>60</time>
                            <rateType>LIMITED</rateType>
                            <rate>200</rate>
                            <weight>3, 2, 3, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5</weight>
                        </phase>
                    </phases>
                        
                    <transactions>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1">
                            <name>Q1</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q2">
                            <name>Q2</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q3">
                            <name>Q3</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q4">
                            <name>Q4</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q5">
                            <name>Q5</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q6">
                            <name>Q6</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q7">
                            <name>Q7</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q8">
                            <name>Q8</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q9">
                            <name>Q9</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q10">
                            <name>Q10</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q11">
                            <name>Q11</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q12">
                            <name>Q12</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q13">
                            <name>Q13</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q14">
                            <name>Q14</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q15">
                            <name>Q15</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q16">
                            <name>Q16</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q17">
                            <name>Q17</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q18">
                            <name>Q18</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q19">
                            <name>Q19</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q20">
                            <name>Q20</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q21">
                            <name>Q21</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.chbenchmark.queries.Q22">
                            <name>Q22</name>
                        </transaction>
                    </transactions>
                        
                </workload>
                        
                <workload benchmarkClass="com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark">
                    <scaleFactor>1</scaleFactor>
                    <terminals>1</terminals>
                        
                    <phases>
                        <phase>
                            <time>60</time>
                            <rateType>LIMITED</rateType>
                            <rate>200</rate>
                            <weight>45,43,4,4,4</weight>
                        </phase>
                    </phases>
                        
                    <transactions>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder">
                            <name>NewOrder</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.tpcc.procedures.Payment">
                            <name>Payment</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus">
                            <name>OrderStatus</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery">
                            <name>Delivery</name>
                        </transaction>
                        <transaction procedureClass="com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel">
                            <name>StockLevel</name>
                        </transaction>
                    </transactions>
                </workload>
            </workloads>
            </configuration>
            """;

    private static final String DIALECTS = """
            <?xml version="1.0" encoding="utf-8"?>
            <dialect type="POSTGRES">
                <procedures>
                    <procedure name="CloseAuctions">
                        <statements>
                            <statement name="updateItemStatus">UPDATE item SET i_status = ?, i_updated = ? WHERE i_id = ? AND i_u_id = ?</statement>
                            <statement name="getDueItems">SELECT i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status FROM item WHERE (i_start_date BETWEEN ? AND ?) AND i_status = 0 ORDER BY i_id ASC LIMIT 100</statement>
                            <statement name="getMaxBid">SELECT imb_ib_id, ib_buyer_id FROM item_max_bid, item_bid WHERE imb_i_id = ? AND imb_u_id = ? AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id</statement>
                            <statement name="insertUserItem">INSERT INTO useracct_item(ui_u_id, ui_i_id, ui_i_u_id, ui_created) VALUES(?, ?, ?, ?)</statement>
                        </statements>
                    </procedure>
                    <procedure name="GetItem">
                        <statements>
                            <statement name="getUser">SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name FROM useracct, region WHERE u_id = ? AND u_r_id = r_id</statement>
                            <statement name="getItem">SELECT i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status FROM item WHERE i_id = ? AND i_u_id = ?</statement>
                        </statements>
                    </procedure>
                    <procedure name="GetUserInfo">
                        <statements>
                            <statement name="getUser">SELECT u_id, u_rating, u_created, u_balance, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name FROM useracct, region WHERE u_id = ? AND u_r_id = r_id</statement>
                            <statement name="getUserFeedback">SELECT u_id, u_rating, u_sattr0, u_sattr1, uf_rating, uf_date, uf_sattr0 FROM useracct, useracct_feedback WHERE u_id = ? AND uf_u_id = u_id ORDER BY uf_date DESC LIMIT 25</statement>
                            <statement name="getBuyerItems">SELECT i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status FROM useracct_item, item WHERE ui_u_id = ? AND ui_i_id = i_id AND ui_i_u_id = i_u_id ORDER BY i_end_date DESC LIMIT 25</statement>
                            <statement name="getSellerItems">SELECT i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status FROM item WHERE i_u_id = ? ORDER BY i_end_date DESC LIMIT 25</statement>
                            <statement name="getWatchedItems">SELECT i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status, uw_u_id, uw_created FROM useracct_watch, item WHERE uw_u_id = ? AND uw_i_id = i_id AND uw_i_u_id = i_u_id ORDER BY i_end_date DESC LIMIT 25</statement>
                            <statement name="getItemComments">SELECT i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status, ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_created FROM item, item_comment WHERE i_u_id = ? AND i_status = ? AND i_id = ic_i_id AND i_u_id = ic_u_id AND ic_response IS NULL ORDER BY ic_created DESC LIMIT 25</statement>
                        </statements>
                    </procedure>
                    <procedure name="LoadConfig">
                        <statements>
                            <statement name="getConfigProfile">SELECT * FROM config_profile</statement>
                            <statement name="getPendingComments">SELECT ic_id, ic_i_id, ic_u_id, ic_buyer_id FROM item_comment WHERE ic_response IS NULL</statement>
                            <statement name="getCategoryCounts">SELECT i_c_id, COUNT(i_id) FROM item GROUP BY i_c_id</statement>
                            <statement name="getAttributes">SELECT gag_id FROM global_attribute_group</statement>
                            <statement name="getPastItems">SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status FROM item, config_profile WHERE i_status = ? AND i_end_date &lt;= cfp_loader_start ORDER BY i_end_date ASC LIMIT 5000</statement>
                            <statement name="getFutureItems">SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status FROM item, config_profile WHERE i_status = ? AND i_end_date &gt; cfp_loader_start ORDER BY i_end_date ASC LIMIT 5000</statement>
                        </statements>
                    </procedure>
                    <procedure name="NewBid">
                        <statements>
                            <statement name="updateItemMaxBid">UPDATE item_max_bid SET imb_ib_id = ?, imb_ib_i_id = ?, imb_ib_u_id = ?, imb_updated = ? WHERE imb_i_id = ? AND imb_u_id = ?</statement>
                            <statement name="getMaxBidId">SELECT MAX(ib_id) FROM item_bid WHERE ib_i_id = ? AND ib_u_id = ?</statement>
                            <statement name="getItem">SELECT i_initial_price, i_current_price, i_num_bids, i_end_date, i_status FROM item WHERE i_id = ? AND i_u_id = ?</statement>
                            <statement name="getItemMaxBid">SELECT imb_ib_id, ib_bid, ib_max_bid, ib_buyer_id FROM item_max_bid, item_bid WHERE imb_i_id = ? AND imb_u_id = ? AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id</statement>
                            <statement name="insertItemMaxBid">INSERT INTO item_max_bid(imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated ) VALUES (?, ?, ?, ?, ?, ?, ? )</statement>
                            <statement name="insertItemBid">INSERT INTO item_bid(ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated ) VALUES (?, ?, ?, ?, ?, ?, ?, ? )</statement>
                            <statement name="updateBid">UPDATE item_bid SET ib_bid = ?, ib_max_bid = ?, ib_updated = ? WHERE ib_id = ? AND ib_i_id = ? AND ib_u_id = ?</statement>
                            <statement name="updateItem">UPDATE item SET i_num_bids = i_num_bids + 1, i_current_price = ?, i_updated = ? WHERE i_id = ? AND i_u_id = ?</statement>
                        </statements>
                    </procedure>
                    <procedure name="NewComment">
                        <statements>
                            <statement name="insertItemComment">INSERT INTO item_comment(ic_id,ic_i_id,ic_u_id,ic_buyer_id,ic_question, ic_created,ic_updated ) VALUES (?,?,?,?,?,?,?)</statement>
                            <statement name="updateItemComments">UPDATE item SET i_num_comments = i_num_comments + 1 WHERE i_id = ? AND i_u_id = ?</statement>
                            <statement name="getItemComments">SELECT i_num_comments FROM item WHERE i_id = ? AND i_u_id = ?</statement>
                            <statement name="updateUser">UPDATE useracct SET u_comments = u_comments + 1, u_updated = ? WHERE u_id = ?</statement>
                        </statements>
                    </procedure>
                    <procedure name="NewCommentResponse">
                        <statements>
                            <statement name="updateComment">UPDATE item_comment SET ic_response = ?, ic_updated = ? WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ?</statement>
                            <statement name="updateUser">UPDATE useracct SET u_comments = u_comments - 1, u_updated = ? WHERE u_id = ?</statement>
                        </statements>
                    </procedure>
                    <procedure name="NewFeedback">
                        <statements>
                            <statement name="checkUserFeedback">SELECT uf_i_id, uf_i_u_id, uf_from_id FROM useracct_feedback WHERE uf_u_id = ? AND uf_i_id = ? AND uf_i_u_id = ? AND uf_from_id = ?</statement>
                            <statement name="insertFeedback">INSERT INTO useracct_feedback( uf_u_id, uf_i_id,uf_i_u_id,uf_from_id,uf_rating,uf_date,uf_sattr0) VALUES (?,?,?,?,?,?,?)</statement>
                            <statement name="updateUser">UPDATE useracct SET u_rating = u_rating + ?, u_updated = ? WHERE u_id = ?</statement>
                        </statements>
                    </procedure>
                    <procedure name="NewItem">
                        <statements>
                            <statement name="insertImage">INSERT INTO item_image(ii_id,ii_i_id,ii_u_id,ii_sattr0) VALUES(?, ?, ?, ?)</statement>
                            <statement name="getCategory">SELECT * FROM category WHERE c_id = ?</statement>
                            <statement name="insertItemAttribute">INSERT INTO item_attribute(ia_id,ia_i_id,ia_u_id,ia_gav_id,ia_gag_id) VALUES(?, ?, ?, ?, ?)</statement>
                            <statement name="getCategoryParent">SELECT * FROM category WHERE c_parent_id = ?</statement>
                            <statement name="updateUserBalance">UPDATE useracct SET u_balance = u_balance - 1, u_updated = ? WHERE u_id = ?</statement>
                            <statement name="getGlobalAttribute">SELECT gag_name, gav_name, gag_c_id FROM global_attribute_group, global_attribute_value WHERE gav_id = ? AND gav_gag_id = ? AND gav_gag_id = gag_id</statement>
                            <statement name="insertItem">INSERT INTO item(i_id,i_u_id,i_c_id,i_name,i_description,i_user_attributes,i_initial_price,i_current_price,i_num_bids,i_num_images,i_num_global_attrs,i_start_date,i_end_date,i_status,i_created,i_updated,i_iattr0) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)</statement>
                        </statements>
                    </procedure>
                    <procedure name="NewPurchase">
                        <statements>
                            <statement name="getItemMaxBid">SELECT * FROM item_max_bid WHERE imb_i_id = ? AND imb_u_id = ?</statement>
                            <statement name="insertItemMaxBid">INSERT INTO item_max_bid (imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated ) VALUES (?, ?, ?, ?, ?, ?, ? )</statement>
                            <statement name="getMaxBid">SELECT * FROM item_bid WHERE imb_i_id = ? AND imb_u_id = ? ORDER BY ib_bid DESC LIMIT 1</statement>
                            <statement name="insertPurchase">INSERT INTO item_purchase(ip_id,ip_ib_id,ip_ib_i_id,ip_ib_u_id,ip_date) VALUES(?,?,?,?,?)</statement>
                            <statement name="getBuyerInfo">SELECT u_id, u_balance FROM useracct WHERE u_id = ?</statement>
                            <statement name="getItemInfo">SELECT i_num_bids, i_current_price, i_end_date, ib_id, ib_buyer_id, u_balance FROM item, item_max_bid, item_bid, useracct WHERE i_id = ? AND i_u_id = ? AND imb_i_id = i_id AND imb_u_id = i_u_id AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id AND ib_buyer_id = u_id</statement>
                            <statement name="updateUserBalance">UPDATE useracct SET u_balance = u_balance + ? WHERE u_id = ?</statement>
                            <statement name="insertUserItem">INSERT INTO useracct_item (ui_u_id, ui_i_id, ui_i_u_id, ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id, ui_created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)</statement>
                            <statement name="updateUserItem">UPDATE useracct_item SET ui_ip_id = ?, ui_ip_ib_id = ?, ui_ip_ib_i_id = ?, ui_ip_ib_u_id = ? WHERE ui_u_id = ? AND ui_i_id = ? AND ui_i_u_id = ?</statement>
                            <statement name="updateItem">UPDATE item SET i_status = 3, i_updated = ? WHERE i_id = ? AND i_u_id = ?</statement>
                        </statements>
                    </procedure>
                    <procedure name="ResetDatabase">
                        <statements>
                            <statement name="deleteItemPurchases">DELETE FROM item_purchase WHERE ip_date &gt; ?</statement>
                            <statement name="resetItems">UPDATE item SET i_status = ?, i_updated = ? WHERE i_status != ? AND i_updated &gt; ?</statement>
                            <statement name="getLoaderStop">SELECT cfp_loader_stop FROM config_profile</statement>
                        </statements>
                    </procedure>
                    <procedure name="UpdateItem">
                        <statements>
                            <statement name="getMaxItemAttributeId">SELECT MAX(ia_id) FROM item_attribute WHERE ia_i_id = ? AND ia_u_id = ?</statement>
                            <statement name="insertItemAttribute">INSERT INTO item_attribute(ia_id,ia_i_id,ia_u_id,ia_gav_id,ia_gag_id) VALUES (?, ?, ?, ?, ?)</statement>
                            <statement name="deleteItemAttribute">DELETE FROM item_attribute WHERE ia_id = ? AND ia_i_id = ? AND ia_u_id = ?</statement>
                            <statement name="updateItem">UPDATE item SET i_description = ?, i_updated = ? WHERE i_id = ? AND i_u_id = ?</statement>
                        </statements>
                    </procedure>
                </procedures>
            </dialect>
            """;

    public void testLoadDatabase() throws JsonProcessingException {
        XmlMapper mapper = new XmlMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        Database database = mapper.readValue(DATABASE_CONFIGURATION, Database.class);

        System.out.println(database);

        String s = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(database);

        System.out.println(s);


    }

    public void testLoadWorkloads() throws JsonProcessingException {
        XmlMapper mapper = new XmlMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        Configuration configuration = mapper.readValue(WORKLOAD_CONFIGURATION, Configuration.class);

        for (Workload workload : configuration.workloads()) {
            System.out.println(workload);

            String s = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(workload);

            System.out.println(s);
        }


    }

    public void testLoadDialect() throws JsonProcessingException {
        XmlMapper mapper = new XmlMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        Dialect configuration = mapper.readValue(DIALECTS, Dialect.class);

        System.out.println(configuration);

        String s = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(configuration);

        System.out.println(s);


    }
}
