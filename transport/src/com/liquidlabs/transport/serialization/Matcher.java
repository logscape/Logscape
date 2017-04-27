package com.liquidlabs.transport.serialization;

public interface Matcher {

	boolean isApplicable(String type);

	boolean match(String arg1, String arg2, int column);

	String getKey();
	
	boolean isColumnBased();

	int columnCount(String string);

}
