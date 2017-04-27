package com.liquidlabs.common.regex;

public class Part {
	boolean isLast;
	boolean grouped;
	StringBuilder regexp = new StringBuilder();
	
	public String regexp() {
		return regexp.toString();
	}
	
	public String toString() {
		return String.format("[%s] g:%b l:%b", regexp(), grouped, isLast);
	}

}
