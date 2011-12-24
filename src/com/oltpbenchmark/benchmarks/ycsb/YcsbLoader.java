package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.twitter.TwitterLoader;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.catalog.Table;

public class YcsbLoader extends Loader{

	private static final Logger LOG = Logger.getLogger(YcsbLoader.class);

	
	private static final int RECORD_COUNT = 1000;
	private static final int NUN_FIELDS = 10;
	private static final int configCommitCount = 10;

	public YcsbLoader(Connection c, WorkloadConfiguration workConf,
			Map<String, Table> tables) {
		super(c, workConf, tables);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void load() throws SQLException {
		
	       Table catalog_tbl = this.getTableCatalog("usertable");
	        assert(catalog_tbl != null);
	        String sql = catalog_tbl.getInsertSQL(1);
	        PreparedStatement stmt = this.conn.prepareStatement(sql);
	        System.out.println(sql);
	        long total = 0;
	        for (int i = 0; i < RECORD_COUNT; i++) {
	            stmt.setInt(1, i);
	            for(int j=2;j<=11;j++)
	            {
	            	stmt.setString(j,LoaderUtil.randomStr(100));
	            }  
	            stmt.addBatch();
	            if ((++total % configCommitCount) == 0) {
	                int result[] = stmt.executeBatch();
	                assert(result != null);
	                conn.commit();
	                if (LOG.isDebugEnabled())
	                    LOG.debug(String.format("Users %d / %d", total, RECORD_COUNT));
	            }
	        } // FOR
	        stmt.executeBatch();
	        conn.commit();
	        if (LOG.isDebugEnabled()) LOG.debug(String.format("Records Loaded [%d]", total));
	}
}
