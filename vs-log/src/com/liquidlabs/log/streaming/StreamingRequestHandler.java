package com.liquidlabs.log.streaming;

import com.liquidlabs.log.space.LogRequest;

public interface StreamingRequestHandler {

	void start(LogRequest request);

}
