package com.oltpbenchmark.benchmarks.twitter;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkloadConfiguration;

public class TwitterConfiguration {
    
    private final XMLConfiguration xmlConfig;
    
    public TwitterConfiguration(WorkloadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }
	
	public String getTracefile() {
		return xmlConfig.getString("tracefile",null);
	}
	public String getTracefile2() {
		return xmlConfig.getString("tracefile2",null);
	}
}
