package com.oltpbenchmark.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.oltpbenchmark.api.config.Configuration;
import com.oltpbenchmark.api.config.Dialect;
import junit.framework.TestCase;

public class TestConfigurationLoad extends TestCase {


    private static final String CONFIGURATION = """
            <?xml version="1.0"?>
            <configuration>
                <database>
                    <type>NOISEPAGE</type>
                    <driverClass>org.postgresql.Driver</driverClass>
                    <url>jdbc:postgresql://localhost:15721/noisepage?sslmode=disable&amp;ApplicationName=epinions&amp;reWriteBatchedInserts=true</url>
                    <username>noisepage</username>
                    <password></password>
                    <transactionIsolation>TRANSACTION_SERIALIZABLE</transactionIsolation>
                    <batchSize>128</batchSize>
                </database>
                        
                <workloads>
                    <workload benchmarkClass="com.oltpbenchmark.benchmarks.twitter.TwitterBenchmark">
                        <scaleFactor>1</scaleFactor>
                        <terminals>1</terminals>
                        <dataDirectory>/foo/bar</dataDirectory>
                        <fileFormat>CSV</fileFormat>
                        <phases>
                            <phase>
                                <serial>false</serial>
                                <warmup>300</warmup>
                                <time>300</time>
                                <rate>10000</rate>
                                <weight>50,50</weight>
                                <activeTerminals>1</activeTerminals>
                                <rateType>DISABLED</rateType>
                                <arrival>REGULAR</arrival>
                            </phase>
                        </phases>
                        <transactions>
                            <transaction procedureClass="com.oltpbenchmark.benchmarks.twitter.procedures.GetTweet">
                                <id>99</id>
                                <name>GetTweet</name>
                            </transaction>
                            <transaction procedureClass="com.oltpbenchmark.benchmarks.twitter.procedures.GetFollowers">
                                <name>GetFollowers</name>
                                <supplemental>true</supplemental>
                                <preExecutionWait>1000</preExecutionWait>
                                <postExecutionWait>2222</postExecutionWait>
                            </transaction>
                        </transactions>
                    </workload>
                    <workload benchmarkClass="com.oltpbenchmark.benchmarks.smallbank.SmallBankBenchmark">
                        <scaleFactor>1</scaleFactor>
                        <terminals>1</terminals>
                        <dataDirectory>/foo/bar</dataDirectory>
                        <fileFormat>CSV</fileFormat>
                        <phases>
                            <phase>
                                <serial>false</serial>
                                <warmup>300</warmup>
                                <time>300</time>
                                <rate>10000</rate>
                                <weight>100</weight>
                                <activeTerminals>1</activeTerminals>
                                <rateType>DISABLED</rateType>
                                <arrival>REGULAR</arrival>
                            </phase>
                        </phases>
                        <transactions>
                            <transaction procedureClass="com.oltpbenchmark.benchmarks.smallbank.procedures.Amalgamate">
                                <name>Amalgamate</name>
                                <supplemental>true</supplemental>
                                <preExecutionWait>1000</preExecutionWait>
                                <postExecutionWait>2222</postExecutionWait>
                            </transaction>
                        </transactions>
                    </workload>
                </workloads>
            </configuration>
            """;

    private static final String DIALECTS = """
            <?xml version="1.0"?>
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

    public void testLoadConfiguration() throws JsonProcessingException {
        ObjectMapper mapper = new XmlMapper();
        Configuration configuration = mapper.readValue(CONFIGURATION, Configuration.class);

        System.out.println(configuration);

        String s = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(configuration);

        System.out.println(s);


    }

    public void testLoadDialect() throws JsonProcessingException {
        ObjectMapper mapper = new XmlMapper();
        Dialect configuration = mapper.readValue(DIALECTS, Dialect.class);

        System.out.println(configuration);

        String s = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(configuration);

        System.out.println(s);


    }
}