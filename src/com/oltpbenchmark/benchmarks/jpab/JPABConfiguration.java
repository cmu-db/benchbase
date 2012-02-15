package com.oltpbenchmark.benchmarks.jpab;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkloadConfiguration;

public class JPABConfiguration {
    
    private final XMLConfiguration xmlConfig;
    
    public JPABConfiguration(WorkloadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }
	
	public String getPersistanceUnit() {
		return xmlConfig.getString("persistence-unit",null);
	}

    public String getTestName() {
        // TODO Auto-generated method stub
        return xmlConfig.getString("testClass",null);
    }
}
