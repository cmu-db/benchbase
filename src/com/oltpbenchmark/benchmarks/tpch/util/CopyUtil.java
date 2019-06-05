/******************************************************************************
 * Copyright 2015 by OLTPBenchmark Project                                   *
 * *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 * *
 * http://www.apache.org/licenses/LICENSE-2.0                              *
 * *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpch.util;

import com.oltpbenchmark.WorkloadConfiguration;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * copyDATABASE methods return null if they weren't successful, and return
 * a possibly empty String[] of SQL statements to execute otherwise
 */
public class CopyUtil {

    private static String getTablePath(WorkloadConfiguration workConf, String tableName) {
        String fileFormat = workConf.getXmlConfig().getString("fileFormat");
        String fileName = String.format("%s.%s", tableName.toLowerCase(), fileFormat);
        Path p = Paths.get(workConf.getDataDir(), fileName);
        return p.toAbsolutePath().toString();
    }

    private static String[] fillTablePaths(WorkloadConfiguration workConf, String[] copySQL) {
        String[] result = new String[copySQL.length];
        result[0] = String.format(copySQL[0], getTablePath(workConf, "region"));
        result[1] = String.format(copySQL[1], getTablePath(workConf, "nation"));
        result[2] = String.format(copySQL[2], getTablePath(workConf, "part"));
        result[3] = String.format(copySQL[3], getTablePath(workConf, "supplier"));
        result[4] = String.format(copySQL[4], getTablePath(workConf, "partsupp"));
        result[5] = String.format(copySQL[5], getTablePath(workConf, "customer"));
        result[6] = String.format(copySQL[6], getTablePath(workConf, "orders"));
        result[7] = String.format(copySQL[7], getTablePath(workConf, "lineitem"));
        return result;
    }

    public static String[] copyPELOTON(WorkloadConfiguration workConf) {
        return fillTablePaths(workConf, new String[]{
                "COPY region FROM '%s' WITH (DELIMITER '|')",
                "COPY nation FROM '%s' WITH (DELIMITER '|')",
                "COPY part FROM '%s' WITH (DELIMITER '|')",
                "COPY supplier FROM '%s' WITH (DELIMITER '|')",
                "COPY partsupp FROM '%s' WITH (DELIMITER '|')",
                "COPY customer FROM '%s' WITH (DELIMITER '|')",
                "COPY orders FROM '%s' WITH (DELIMITER '|')",
                "COPY lineitem FROM '%s' WITH (DELIMITER '|')",
        });
    }

    public static String[] copyPOSTGRES(WorkloadConfiguration workConf, Connection conn, Logger LOG) {
        String[] copyFiles = new String[8];
        for (int i = 0; i < 8; i++) {
            copyFiles[i] = "%s";
        }
        copyFiles = fillTablePaths(workConf, copyFiles);

        String[] copyCommands = new String[]{
                "COPY region FROM STDIN WITH (DELIMITER '|')",
                "COPY nation FROM STDIN WITH (DELIMITER '|')",
                "COPY part FROM STDIN WITH (DELIMITER '|')",
                "COPY supplier FROM STDIN WITH (DELIMITER '|')",
                "COPY partsupp FROM STDIN WITH (DELIMITER '|')",
                "COPY customer FROM STDIN WITH (DELIMITER '|')",
                "COPY orders FROM STDIN WITH (DELIMITER '|')",
                "COPY lineitem FROM STDIN WITH (DELIMITER '|')",
        };

        try {
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            for (int i = 0; i < 8; i++) {
                StripEndInputStream inputStream = new StripEndInputStream(new FileInputStream(copyFiles[i]));
                String command = copyCommands[i];
                LOG.info(String.format("Executing %s", command));
                copyManager.copyIn(command, inputStream);
                conn.commit();
            }
        } catch (IOException e) {
            // failed
            return null;
        } catch (SQLException e) {
            // failed
            return null;
        }

        LOG.info("COPY complete.");
        return new String[]{};
    }

    public static String[] copyMEMSQL(WorkloadConfiguration workConf) {
        return fillTablePaths(workConf, new String[]{
                "LOAD DATA INFILE \"%s\" INTO TABLE region FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (r_regionkey, r_name, r_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE nation FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (n_nationkey, n_name, n_regionkey, n_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE part FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE supplier FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE partsupp FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE customer FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE orders FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, @);",
                "LOAD DATA INFILE \"%s\" INTO TABLE lineitem FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, @);",
        });
    }

    public static String[] copyMYSQL(WorkloadConfiguration workConf) {
        return fillTablePaths(workConf, new String[]{
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE region FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (r_regionkey, r_name, r_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE nation FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (n_nationkey, n_name, n_regionkey, n_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE part FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE supplier FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE partsupp FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE customer FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE orders FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, @DUMMY);",
                "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE lineitem FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, @DUMMY);",
        });
    }

    private static class StripEndInputStream extends FilterInputStream {
        /**
         * To remove the | at the end of each line DBGEN creates.
         */

        private Integer curr = null;
        private Integer next = null;

        /**
         * Creates a <code>FilterInputStream</code>
         * by assigning the  argument <code>in</code>
         * to the field <code>this.in</code> so as
         * to remember it for later use.
         *
         * @param in the underlying input stream, or <code>null</code> if
         *           this instance is to be created without an underlying stream.
         */
        protected StripEndInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            if (next == null) {
                next = super.read();
            }

            curr = next;
            next = super.read();

            if (next == '\n') {
                next = super.read();
                return '\n';
            } else {
                return curr;
            }
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            // identical to InputStream code, just that we want to use our read()
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }

            int c = read();
            if (c == -1) {
                return -1;
            }
            b[off] = (byte) c;

            int i = 1;
            try {
                for (; i < len; i++) {
                    c = read();
                    if (c == -1) {
                        break;
                    }
                    b[off + i] = (byte) c;
                }
            } catch (IOException ee) {
            }
            return i;
        }
    }
}
