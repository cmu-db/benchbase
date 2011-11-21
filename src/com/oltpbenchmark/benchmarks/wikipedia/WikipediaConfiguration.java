package com.oltpbenchmark.benchmarks.wikipedia;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkloadConfiguration;

public class WikipediaConfiguration {

    private final XMLConfiguration xmlConfig;
    
    public WikipediaConfiguration(WorkloadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }
    
	public String getTracefile() {
		return xmlConfig.getString("tracefile",null);
	}
	
	public String getBaseIP(){
		return xmlConfig.getString("baseip",null);
	}

}
