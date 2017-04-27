package com.liquidlabs.log.search;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.agg.HistoManager;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.List;

public class HistoEvent {
	
	private static final Logger LOGGER = Logger.getLogger(HistoEvent.class);
	
	private static final long serialVersionUID = 1L;
    private String requestHash;
    private LogRequest request;
    private int logId;
    private long startTime;
	private long endTime;
	private String hostname = "";
	private String sourceURI = "";
	private String subscriberId = "";
	private Query query;
	private AggSpace aggSpace;
	private boolean isIndexOnly;
	Bucket currentBucket;
	int currentIndex = -2;
	private int bucketsSent;
	
	transient HistoManager histoManager = new HistoManager();
	transient private List<Long> bucketTimes;
	private int bucketCount;
    private long eventTime;

    public HistoEvent(){}
	
	public HistoEvent(LogRequest request, int logId, long startTime, long endTime, int bucketCount, String hostname, String sourceURI, String subscriberId, Query query, AggSpace aggSpace, boolean isIndexOnly) {
        this.request = request;
        this.requestHash = MurmurHash3.hashString(request.toString(), 10) + "";
        this.logId = logId;
        this.startTime = startTime;
		this.endTime  = endTime;
		this.hostname = hostname;
		this.sourceURI = sourceURI;
		this.subscriberId = subscriberId;
		this.aggSpace = aggSpace;
		this.query = query;
		this.isIndexOnly = isIndexOnly;
		this.bucketCount = bucketCount;
		bucketTimes = histoManager.getBucketTimes(new DateTime(startTime), new DateTime(endTime), bucketCount, false);
	}
		
	private List<Function> createFunctions() {
		return query.functions();
	}
	public long getBucketWidthMs() {
        if (bucketTimes.size() ==1) {
            return (this.endTime - this.startTime);
        }
		return bucketTimes.get(1) - bucketTimes.get(0);
	}
    long bucketMinute = -1;


	public void add(FieldSet fieldSet, String[] fields, String lineData, long eventTime, String filenameOnly, String filename, int lineNumber, boolean verbose, long fileStartTime, long fileEndTime, MatchResult matchResult, long requestStartMs, long requestEndMs) {
        this.eventTime = eventTime;
            int bucketIndex = histoManager.getBucketIndex(eventTime, bucketTimes);
            if (bucketIndex == currentIndex && currentBucket != null) {
                currentBucket.increment(fieldSet, fields, filenameOnly, filename, eventTime, fileStartTime, fileEndTime, lineNumber, lineData, matchResult, false, requestStartMs, requestEndMs);
                return;
            }

            writeBucket(verbose);

            // prepend OR extend OR use existing histo-bucket
            long[] startEndBucketTime =  histoManager.getStartEndBucketTime(bucketTimes, eventTime, getBucketWidthMs());
            currentBucket = new Bucket(startEndBucketTime[0], startEndBucketTime[1], createFunctions(), query.position(), query.pattern(), sourceURI, subscriberId, filename);
            currentIndex = bucketIndex;
            currentBucket.increment(fieldSet, fields, filenameOnly, filename, eventTime, fileStartTime, fileEndTime, lineNumber, lineData, matchResult, false, requestStartMs, requestEndMs);
	}

    public void writeBucket(boolean verbose) {
		if (currentBucket == null) {
			return ;
		}
		bucketsSent++;
		currentBucket.convertFuncResults(verbose);
		if (!isIndexOnly) {
            if (verbose) LOGGER.info("LOGGER Writing:" + this.toString());
            aggSpace.write(currentBucket, true, requestHash, logId, bucketMinute);
		}
        currentBucket = null;
	}


	public int hits() {
		if (currentBucket == null) return -1;
		return currentBucket.hits();
	}
	final public long getStartTime() {
		if (currentBucket == null) return -1;
		return currentBucket.getStart();
	}

	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("[HistoEvent:");
		buffer.append(" start:");
		buffer.append(DateUtil.shortDateTimeFormat3.print(startTime));
		buffer.append(" end:");
		buffer.append(DateUtil.shortDateTimeFormat3.print(endTime));
		buffer.append(" sub:");
		buffer.append(subscriberId);
		buffer.append(" cBucket:");
		buffer.append(currentBucket);
		buffer.append(" hits:");
		buffer.append(hits());
		buffer.append("]");
		return buffer.toString();
	}

	public HistoEvent copy() {
		return new HistoEvent(request, logId, this.startTime, this.endTime, this.bucketCount, this.hostname, this.sourceURI, this.subscriberId, query.copy(query.hitCounter), this.aggSpace, this.isIndexOnly);
	}

	public void incrementScanned(int amount) {
		if (currentBucket != null) currentBucket.incrementScanned(amount);
	}
	public Bucket bucket() {
		return currentBucket;
	}

    transient TLongLongMap timeCache = new TLongLongHashMap();
	public long getBucketTime(long time) {
		if (this.bucketTimes == null) return this.startTime;
        if (time <= 0) return -1;

        long found = timeCache.get(time);
		if (found != 0) return found;
        else {

            long lastTime = bucketTimes.get(0);
            for (long bTime : this.bucketTimes) {
                if (bTime > time) {
                    timeCache.putIfAbsent(time, lastTime);
                    return lastTime;
                }
                lastTime = bTime;

            }
            //System.out.println("Return:" + time + " -> " + lastTime);
            try {
                timeCache.putIfAbsent(time, lastTime);
            } catch (Throwable t ){
                LOGGER.warn("BadTime:" + time + " -> :" + lastTime);

            }
            return lastTime;
        }
	}
}
