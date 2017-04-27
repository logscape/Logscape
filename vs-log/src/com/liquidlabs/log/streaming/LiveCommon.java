package com.liquidlabs.log.streaming;

import org.apache.log4j.Logger;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.space.LogRequest;

public class LiveCommon {
	
	private final LogRequest request;
	private volatile short failureCount;
	String hostname = NetworkUtils.getHostname();
	private String lastTouch;
	private final Logger logger;

	public LiveCommon(LogRequest request, Logger logger) {
		this.request = request;
		this.logger = logger;
	}

	public boolean isExpired() {
		boolean finished = request.isCancelled() || request.isExpired();
		if (!finished) lastTouch = DateUtil.shortDateTimeFormat4.print(System.currentTimeMillis()) + " Cancel:" + request.isCancelled() + " Expire:"  + request.isExpired();
		return finished;
	}

	public boolean bail() {
		if (failureCount > 50 || isExpired()) {
			logger.info("LiveHasTooManyErrrors : Cancel:" + request.isCancelled() + " Expire:" + request.isExpired());
			request.cancel();
			return true;
		}
		return false;

	}

	public void incrementFailure() {
		failureCount++;
	}

	public void resetFailure() {
		failureCount = 0;
	}
	public String toString() {
		 return " " + request.subscriber() + " touch:" + lastTouch +"\n\t";	}

}
