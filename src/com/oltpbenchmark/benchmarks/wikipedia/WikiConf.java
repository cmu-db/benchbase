package com.oltpbenchmark.benchmarks.wikipedia;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkLoadConfiguration;

public class WikiConf {

    private final XMLConfiguration xmlConfig;
    
    public WikiConf(WorkLoadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }
    
	public String getTracefile() {
		return xmlConfig.getString("tracefile",null);
	}
	
	public String getBaseIP(){
		return xmlConfig.getString("baseip",null);
	}

}
