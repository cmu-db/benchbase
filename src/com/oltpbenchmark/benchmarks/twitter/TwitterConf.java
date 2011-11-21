package com.oltpbenchmark.benchmarks.twitter;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkLoadConfiguration;

public class TwitterConf {
    
    private final XMLConfiguration xmlConfig;
    
    public TwitterConf(WorkLoadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }
	
	public String getTracefile() {
		return xmlConfig.getString("tracefile",null);
	}
	public String getTracefile2() {
		return xmlConfig.getString("tracefile2",null);
	}
}
