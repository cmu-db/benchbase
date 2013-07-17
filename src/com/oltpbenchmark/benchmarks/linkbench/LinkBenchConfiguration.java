package com.oltpbenchmark.benchmarks.linkbench;


import org.apache.commons.configuration.XMLConfiguration;

import com.oltpbenchmark.WorkloadConfiguration;

public class LinkBenchConfiguration {

    private final XMLConfiguration xmlConfig;

    public LinkBenchConfiguration(WorkloadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }

    public String getConfigFile() {
        return xmlConfig.getString("configfile");
    }

}
