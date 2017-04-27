package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.vso.SpaceService;

public interface Trigger extends LogReplayHandler {

	void attach(LogSpace logSpace, AggSpace aggSpace,
			SpaceService spaceService, AdminSpace adminSpace);


	void fireTrigger(ReplayEvent replayEvent);

	void stop();

	boolean isReplayOnly();


	int getMaxReplays();

}
