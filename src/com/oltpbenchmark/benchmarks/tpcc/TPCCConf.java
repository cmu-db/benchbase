package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.WorkLoadConfiguration;

public class TPCCConf extends WorkLoadConfiguration {

	public int getNumWarehouses() {
		return xmlConfig.getInt("numWarehouses", 0);
	}
}
