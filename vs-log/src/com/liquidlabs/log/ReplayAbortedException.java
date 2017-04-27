package com.liquidlabs.log;

public class ReplayAbortedException extends Exception {

	public ReplayAbortedException(String format) {
		super(format);
	}

	public ReplayAbortedException(String string, Throwable t) {
		super(string, t);
	}

	private static final long serialVersionUID = 1L;

}
