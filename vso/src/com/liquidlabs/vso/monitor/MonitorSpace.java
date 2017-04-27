package com.liquidlabs.vso.monitor;

import com.liquidlabs.common.LifeCycle;

public interface MonitorSpace extends LifeCycle {
	String NAME = MonitorSpace.class.getSimpleName();
	
	void registerMetricsListener(MetricListener listener, String query, String listenerId);
	void unregisterMetricsListener(String listenerId);
	
	void write(Metrics metrics);

}
