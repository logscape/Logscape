package com.liquidlabs.util;

import java.util.List;
import java.util.TreeMap;

import com.liquidlabs.common.plot.XYPoint;

public interface EventStore {

	List<AccountEvent> retrieveEventsFrom(String bundleId, String service, long fromTime, long toTime);

	List<XYPoint> retrieveAccumulatedEventsFrom(String bundleId, String service, long fromTime, long toTime, long granularityMs);

	void store(AccountEvent event);

	List<XYPoint> accumulate(long fromTimeMs, long toTimeMs, long granularityMs, List<AccountEvent> eventsToFilter, TreeMap<Long, XYPoint> workingStore);

	List<XYPoint> retrieveEventsCountFrom(String bundleId, String serviceName, long fromTimeMs, long toTimeMs, int i);

	List<XYPoint> count(long fromTimeMs, long toTimeMs, long granularityMs, List<AccountEvent> eventsToFilter, TreeMap<Long, XYPoint> hashMap);

}
