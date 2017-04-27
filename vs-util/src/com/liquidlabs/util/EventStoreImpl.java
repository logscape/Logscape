package com.liquidlabs.util;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.common.plot.XYPoint;
import com.liquidlabs.orm.ORMapperClient;
import com.liquidlabs.orm.ORMapperFactory;


/**
 * Stores Completed event by App, Owner in TimeSeries
 * 
 * TODO: need to warehouse data
 */
public class EventStoreImpl implements EventStore {
	
	private final ORMapperFactory mapperFactory;
	private ORMapperClient orm;
	DateTimeFormatter formatter = DateTimeFormat.mediumTime();

	public EventStoreImpl(ORMapperFactory mapperFactory, boolean clustered, boolean persistent) {
		this.mapperFactory = mapperFactory;
		orm = this.mapperFactory.getORMapperClient("UtilEventStore", null, clustered, persistent);
	}
	
	/**
	 * Completed events are dropped in here
	 */
	public void store(AccountEvent event) {
		orm.store(event);
		
	}
	
	public List<AccountEvent> retrieveEventsFrom(String bundleId, String service, long startTime, long endTime){
		String query = "bundleId equals %s AND serviceName equals %s AND startTime <= %d AND endTime >= %d";
		if (service == null || service.length() == 0) query = "bundleId equals %s AND startTime <= %d AND endTime >= %d";
		return orm.findObjects(AccountEvent.class, String.format(query, bundleId, service, endTime, startTime), false, -1);
	}
	
	public List<XYPoint> retrieveEventsCountFrom(String bundleId, String serviceName, long fromTimeMs, long toTimeMs, int granularityMs) {
		List<AccountEvent> retrieveEventsFrom = this.retrieveEventsFrom(bundleId, serviceName, fromTimeMs, toTimeMs);
		TreeMap<Long, XYPoint> hashMap = new TreeMap<Long,XYPoint>();
		makeBucketsInMap(hashMap, fromTimeMs, toTimeMs, granularityMs);
		return count(fromTimeMs, toTimeMs, granularityMs, retrieveEventsFrom, hashMap);
	}
	
	/**
	 * Note - in this case - endTime is initially used to indicate total (accumulated) time by all services in that bucket - start time is the begining of the bucketed time
	 * 
	 */
	public List<XYPoint> retrieveAccumulatedEventsFrom(String bundleId, String service, long fromTime, long toTime, long granularityMs){
		List<AccountEvent> retrieveEventsFrom = this.retrieveEventsFrom(bundleId, service, fromTime, toTime);
		TreeMap<Long, XYPoint> hashMap = new TreeMap<Long,XYPoint>();
		makeBucketsInMap(hashMap, fromTime, toTime, granularityMs);
		return accumulate(fromTime, toTime, granularityMs, retrieveEventsFrom, hashMap);
	}

	private void makeBucketsInMap(TreeMap<Long, XYPoint> hashMap, long fromTime, long toTime, long granularityMs) {
		for (long bucketTime = fromTime ; bucketTime < toTime; bucketTime += granularityMs) {
			XYPoint accountEvent = new XYPoint(formatter.print(bucketTime), bucketTime);
			hashMap.put(bucketTime, accountEvent);
		}
	}

	public List<XYPoint> accumulate(long fromTime, long toTime, long granularityMs, List<AccountEvent> eventsToFilter,	TreeMap<Long, XYPoint> hashMap) {
		for (AccountEvent accountEvent : eventsToFilter) {
			for (long bucketTime = fromTime ; bucketTime < toTime; bucketTime += granularityMs) {
				
				long startTime = Math.max(bucketTime, accountEvent.getStartTime());
				long endTime = accountEvent.getEndTime() == 0 ? bucketTime+granularityMs : Math.min(bucketTime+granularityMs, accountEvent.getEndTime());
				
				if (endTime > DateTimeUtils.currentTimeMillis()) endTime = DateTimeUtils.currentTimeMillis();
				
				long timeDelta = endTime - startTime;
				if (timeDelta <= 0) continue;
				
				// accumulate time spent
				if (!hashMap.containsKey(bucketTime)) {
					hashMap.put(bucketTime, new XYPoint(formatter.print(bucketTime), bucketTime));
				}
				XYPoint accumlationEvent = hashMap.get(bucketTime);
				accumlationEvent.setY(accumlationEvent.getY() + timeDelta);
			}
		}
		return new ArrayList<XYPoint>(hashMap.values());
	}
	public List<XYPoint> count(long fromTime, long toTime, long granularityMs, List<AccountEvent> eventsToFilter,	TreeMap<Long, XYPoint> hashMap) {
		for (AccountEvent accountEvent : eventsToFilter) {
//			int count = 0;
			for (long bucketTime = fromTime ; bucketTime < toTime; bucketTime += granularityMs) {
				
//				DateTime bucketTimeS = new DateTime(bucketTime);
				DateTime bucketTimeE = new DateTime(bucketTime+granularityMs);
				DateTime eventTime = new DateTime(accountEvent.getStartTime());
//				DateTime eventETime = new DateTime(accountEvent.getEndTime());
				long startTime = Math.max(bucketTime, accountEvent.getStartTime());
				long endTime = accountEvent.getEndTime() == 0 ? bucketTime+granularityMs : Math.min(bucketTime+granularityMs, accountEvent.getEndTime());
				
//				DateTime bEventETime = new DateTime(endTime);

				if (accountEvent.getEndTime() != 0) {
					long delta = endTime - startTime;
					if (delta <= 0) continue;
				}
				if (eventTime.getMillis() > bucketTimeE.getMillis()) continue;
				
				// accumulate time spent
				if (!hashMap.containsKey(bucketTime)) {
					hashMap.put(bucketTime, new XYPoint(formatter.print(bucketTime), bucketTime));
				}
				XYPoint accumlationEvent = hashMap.get(bucketTime);
				accumlationEvent.setY(accumlationEvent.getY() + 1);
//				count++;
			}
		}
		return new ArrayList<XYPoint>(hashMap.values());
	}


}
