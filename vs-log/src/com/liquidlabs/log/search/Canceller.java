package com.liquidlabs.log.search;

public interface Canceller {
	void cancelRequest(String subscriberId);

	void flush(String subscriber, boolean finished);

	int status(String subscriber, String resourceId, long amount, int fileScanPercentComplete);
}
