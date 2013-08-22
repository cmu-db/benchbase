package com.oltpbenchmark.benchmarks.sibench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class SILoader extends Loader {
    private static final Logger LOG = Logger.getLogger(SILoader.class);
    private final int num_record;

    public SILoader(SIBenchmark benchmark, Connection c) {
        super(benchmark, c);
        this.num_record = (int) Math.round(this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of RECORDS:  " + this.num_record);
        }
    }

    @Override
    public void load() throws SQLException {
        Random rand = new Random(System.currentTimeMillis());
        Table catalog_tbl = this.getTableCatalog("SITEST");
        assert (catalog_tbl != null);
        
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement stmt = this.conn.prepareStatement(sql);
        long total = 0;
        int batch = 0;
        for (int i = 0; i < this.num_record; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, rand.nextInt(Integer.MAX_VALUE));
            stmt.addBatch();
            total++;
            if (++batch >= SIConstants.configCommitCount) {
                int result[] = stmt.executeBatch();
                assert (result != null);
                conn.commit();
                batch = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
            }
        } // FOR
        if (batch > 0) {
            stmt.executeBatch();
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
        }
        stmt.close();
        if (LOG.isDebugEnabled()) LOG.debug("Finished loading " + catalog_tbl.getName());
    }
}
