package com.liquidlabs.log.space.agg;

import com.liquidlabs.orm.Id;

public class AggEngineState {
	@Id
	public String hostname;
	public String criteria;
	public String group;
	
	public AggEngineState() {
	}
	public AggEngineState(String hostname, String criteria, String group){
		this.hostname = hostname;
		this.criteria = criteria;
		this.group = group;
	}
}
