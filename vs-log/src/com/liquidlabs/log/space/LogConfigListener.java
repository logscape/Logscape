package com.liquidlabs.log.space;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastOnce;
import com.liquidlabs.transport.proxy.Remotable;

public interface LogConfigListener extends Remotable {
	
	String getId();

    @FailFastOnce
	void setFilters(LogFilters filters);

    @FailFastOnce
	void removeWatch(WatchDirectory watch);

    @FailFastOnce
	void addWatch(WatchDirectory watch);

	@FailFastOnce
	void updateWatch(WatchDirectory watch);

}
