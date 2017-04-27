package com.liquidlabs.log.space;

import java.util.List;
import java.util.Map;

import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.Remotable;

public interface LogReplayHandler extends Remotable {
	
//	String getId();
	
	@FailFastAndDisable
	void handle(ReplayEvent event);
	
	@FailFastAndDisable
	void handle(Bucket event);
	
	@FailFastAndDisable
	void handleSummary(Bucket bucketToSend);
	
	
	@FailFastAndDisable
	int handle(String providerId, String subscriber, int size, Map<String, Object> histo);

	@FailFastAndDisable
	int handle(List<ReplayEvent> events);

	@FailFastAndDisable
	int status(String provider, String subscriber, String msg);


}
