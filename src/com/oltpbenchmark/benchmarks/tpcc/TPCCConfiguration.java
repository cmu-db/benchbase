package com.oltpbenchmark.benchmarks.tpcc;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkloadConfiguration;

public class TPCCConfiguration {

    private final XMLConfiguration xmlConfig;
    
    public TPCCConfiguration(WorkloadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }
    
	public int getNumWarehouses() {
		return xmlConfig.getInt("numWarehouses", 0);
	}
}
