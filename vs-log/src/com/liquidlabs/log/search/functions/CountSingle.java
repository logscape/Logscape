package com.liquidlabs.log.search.functions;

import org.apache.log4j.Logger;


/**
 * 
 * CountSingle/CountDistinct instances of a group - 
 * useful for ascertaining membership where each member writes to its own log line.
 * i.e. 
 * engine:3301 joining
 * engine:3301 joining
 * engine:3302 joining..
 * engine:3302 joining..
 * engine:3302 joining..
 * 
 * Resulting value 
 *  engine:3301:1 
 *  engine:3302:1 
 *
 */
public class CountSingle extends Count implements Function, FunctionFactory {
	
	private final static Logger LOGGER = Logger.getLogger(CountSingle.class);
	
	
	public CountSingle(){
	}
	public CountSingle(String tag, String groupByGroup, String applyToGroup) {
		super(tag,groupByGroup, applyToGroup);
	}
	
	protected int increment(IntValue integer) {
		if (integer.value > 0) return integer.value;
		return integer.increment();
	}
	protected int updateIncrement(IntValue one, IntValue two) {
		if (one.value > 0 || two.value > 0) return 1;
		return 0;
	}
	public Function create() {
		return new CountSingle(tag, groupByGroup, applyToGroup);
	}
	
	

}
	