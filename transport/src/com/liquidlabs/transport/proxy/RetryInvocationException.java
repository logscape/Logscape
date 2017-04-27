package com.liquidlabs.transport.proxy;


public class RetryInvocationException extends Exception {

	private static final long serialVersionUID = 1L;

	public RetryInvocationException(String info) {
		super(info);
	}
	public RetryInvocationException(String info, Exception e) {
		super(info, e);
	}
	public RetryInvocationException(Throwable e) {
		super(e);
	}
	public RetryInvocationException(String msg, Throwable t) {
		super(msg, t);
	}

}
