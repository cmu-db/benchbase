package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.WorkLoadConfiguration;

public class TwitterConf extends WorkLoadConfiguration{
	
	public String getTracefile() {
		return xmlConfig.getString("tracefile",null);
	}
	public String getTracefile2() {
		return xmlConfig.getString("tracefile2",null);
	}
}
