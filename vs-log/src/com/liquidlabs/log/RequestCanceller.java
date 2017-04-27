package com.liquidlabs.log;

import org.apache.log4j.Logger;

import com.liquidlabs.log.space.AggSpace;

public class RequestCanceller implements CancellerListener {
	private final static Logger LOGGER = Logger.getLogger(RequestCanceller.class);
	private String id = getClass().getSimpleName();
	private final AggSpace tailerSearchAggregator;

	public RequestCanceller(AggSpace tailerSearchAggregator, String resourceId){
		this.tailerSearchAggregator = tailerSearchAggregator;
		this.id = getClass().getName() + resourceId;
		
	}
	public void cancel(String subscriberId) {
		try {
			this.tailerSearchAggregator.cancel(subscriberId);
		} catch (Throwable t) {
			LOGGER.error(t);
			
		}
	}

	public String getId() {
		return id;
	}

}
