package com.oltpbenchmark.benchmarks.seats;

import java.io.File;

import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkloadConfiguration;

public class SEATSConfiguration {

    private final XMLConfiguration xmlConfig;
    
    public SEATSConfiguration(WorkloadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }
    
    public File getDataDirectory() {
        return new File(xmlConfig.getString("datadir",null));
    }
}
