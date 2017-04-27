package com.liquidlabs.space.raw;

public interface Updater {

	boolean isApplicable(String type);

	String update(String arg1, String arg2);

}
