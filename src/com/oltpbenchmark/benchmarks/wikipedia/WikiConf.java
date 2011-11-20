package com.oltpbenchmark.benchmarks.wikipedia;

import com.oltpbenchmark.WorkLoadConfiguration;

public class WikiConf extends WorkLoadConfiguration {

	public String getTracefile() {
		return xmlConfig.getString("tracefile",null);
	}
	
	public String getBaseIP(){
		return xmlConfig.getString("baseip",null);
	}

}
