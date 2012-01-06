package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class YCSBLoader extends Loader{

	private static final Logger LOG = Logger.getLogger(YCSBLoader.class);

	public YCSBLoader(YCSBBenchmark benchmark, Connection c) {
		super(benchmark, c);
	}

	@Override
	public void load() throws SQLException {
		
	       Table catalog_tbl = this.getTableCatalog("usertable");
	        assert(catalog_tbl != null);
	        String sql = SQLUtil.getInsertSQL(catalog_tbl);
	        PreparedStatement stmt = this.conn.prepareStatement(sql);
	        System.out.println(sql);
	        long total = 0;
	        for (int i = 0; i < YCSBConstants.RECORD_COUNT; i++) {
	            stmt.setInt(1, i);
	            for(int j=2;j<=11;j++)
	            {
	            	stmt.setString(j,LoaderUtil.randomStr(100));
	            }  
	            stmt.addBatch();
	            if ((++total % YCSBConstants.configCommitCount) == 0) {
	                int result[] = stmt.executeBatch();
	                assert(result != null);
	                conn.commit();
	                if (LOG.isDebugEnabled())
	                    LOG.debug(String.format("Users %d / %d", total, YCSBConstants.RECORD_COUNT));
	            }
	        } // FOR
	        stmt.executeBatch();
	        conn.commit();
	        if (LOG.isDebugEnabled()) LOG.debug(String.format("Records Loaded [%d]", total));
	}
}
