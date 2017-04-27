package com.liquidlabs.log.space;

import com.liquidlabs.admin.User;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.handlers.SummaryBucket;
import com.liquidlabs.log.search.summaryindex.PersistingSummaryIndex;
import com.liquidlabs.orm.Id;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.resource.BloomMatcher;
import org.apache.commons.lang3.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LogRequest {
	@Id
	private String subscriber;

	private long submittedTime = System.currentTimeMillis();
	private short timeToLiveMins = LogProperties.getRequestTTLMins();
	private long fromTimeMs = DateTimeUtils.currentTimeMillis() - 60 * 1000;
	private long toTimeMs = DateTimeUtils.currentTimeMillis();
	private boolean search;
	private BloomMatcher hosts;
	@Deprecated
	private int maxItems;
	private List<Query> queries = new ArrayList<Query>();
	private User user;
    private Map<String,String> context = new HashMap<String, String>();

	private Replay replay = new Replay();
	private int minReplay;

	volatile boolean cancelled = false;
    volatile AtomicBoolean cancelledAtomic = new AtomicBoolean();
	boolean streaming = false;
	boolean ignoreCase = false;

	// used when a search is being run to heat up cache (ie. dont want to see
	// the results)
	private boolean indexOnly = false;
	private int bucketCount = LogProperties.getDefaultBuckets();
	
	// if specified
	private int bucketWidthSec;

	// used when we want to ignore results of indexed data
	private boolean isSummaryRequired = true;
	private boolean verbose = false;

	transient Map<String, Map<Long, AtomicLong>> hitCounter = new ConcurrentHashMap<String, Map<Long, AtomicLong>>();;
	transient Bucket summaryBucket;

    private long offsetValueMs;
    private List<String> errors = new ArrayList<String>();
	private String cacheKey;

	public LogRequest() {
	}

	public LogRequest(String subscriber, long fromTimeMs, long toTimeMs) {
		this.subscriber = subscriber;

		try {
//			long duration =  (toTimeMs - fromTimeMs);
			
			// if the duration is longer than X hours, roll back and forth to the
			// nearest 10 minutes to
			// make clean/repeatable start/end points
//			if (duration/DateUtil.HOUR > 4){
//				this.toTimeMs = rollforward(toTimeMs, 10);
//				this.fromTimeMs = rollbackward(fromTimeMs, 10);
//			} else if (duration/DateUtil.HOUR > 0) {
//				this.toTimeMs = rollforward(toTimeMs, 1);
//				this.fromTimeMs = rollbackward(fromTimeMs, 1);
//			} else {
//				this.fromTimeMs = fromTimeMs;
//				this.toTimeMs = toTimeMs;
//			}

			this.fromTimeMs = fromTimeMs;
			this.toTimeMs = toTimeMs;
		} catch (Throwable t) {
			throw new RuntimeException("Failed to get Times:" + fromTimeMs + " - " + toTimeMs);
		}

		this.submittedTime = DateTimeUtils.currentTimeMillis();
	}

	long rollforward(long timeMs, int i) {
		DateTime time = new DateTime(timeMs);

		int currentMin = time.getMinuteOfHour();
		int currentHour = time.getHourOfDay();
		int currentDay = time.getDayOfMonth();

		int leftOver = currentMin % i;
		if (leftOver == 0)
			return time.getMillis();
		int mod = currentMin / i;

		int newMin = (mod + 1) * i;
		if (newMin >= 60) {
			newMin = 0;
			currentHour += 1;
		}
		if (currentHour > 23) {
			currentHour = 0;
			currentDay++;
		}
		DateTime result = new DateTime(time.getYear(), time.getMonthOfYear(), currentDay, currentHour, newMin, 0, 0);
		return result.getMillis();
	}

	long rollbackward(long timeMs, int i) {
		DateTime time = new DateTime(timeMs);
		int currentMin = time.getMinuteOfHour();
		int currentHour = time.getHourOfDay();
		int currentDay = time.getDayOfMonth();
		int mod = currentMin / i;
		int newMin = (mod) * i;
		if (newMin < 0) {
			newMin = 0;
			// newMin = 50;
			// currentHour--;
		}
		DateTime result = new DateTime(time.getYear(), time.getMonthOfYear(), currentDay, currentHour, newMin, 0, 0);
		return result.getMillis();
	}

	public void setSummaryRequired(boolean isSummaryRequired) {
		this.isSummaryRequired = isSummaryRequired;
	}
    public void addContext(String key, String value) {
        this.context.put(key, value);
    }

	public boolean isSummaryRequired() {
		return isSummaryRequired && summaryBucket != null;
	}

	final public long getStartTimeMs() {
		return fromTimeMs;
	}

	final public String subscriber() {
		return subscriber;
	}
    final void setSubscriber(String subscriber) {
        this.subscriber = subscriber;
    }

	final public long getEndTimeMs() {
		return toTimeMs;
	}

	public void setSearch(boolean search) {
		this.search = search;
	}

	public boolean isSearch() {
		return search;
	}

	public void addQuery(Query query) {
		queries.add(query);
	}

	public void setIgnoreCase(boolean ignoreCase) {
		this.ignoreCase = ignoreCase;
		for (Query query : queries) {
			query.ignoreCase = this.ignoreCase;
		}
	}

	public void setHosts(BloomMatcher hosts) {
		this.hosts = hosts;
	}

	public Query query(int i) {
		if (i >= queries.size())
			throw new RuntimeException("QueryCount is:" + queries.size() + " OutOfBounds access to item:" + i);
		return queries.get(i);
	}

	public int queryCount() {
		return queries.size();
	}

	final public List<Query> queries() {
		setIgnoreCase(this.ignoreCase);
		return queries;
	}

	public void setReplay(Replay replay) {
		this.replay = replay;
	}

	public Replay getReplay() {
		if (replay == null || queries.size() > 0 && queries.get(0).isReplay() == false) return null;
		return replay;
	}
    public boolean isReplayRequired() {
        return replay != null && replay.maxItems() > 0;
    }

	public void setStartTimeMs(long startTimeMs) {
		this.fromTimeMs = startTimeMs;
	}

	public void setEndTimeMs(long toTimeMs) {
		this.toTimeMs = toTimeMs;
	}

	public void setIndexOnly(boolean indexOnly) {
		this.indexOnly = indexOnly;
	}

	public boolean isIndexOnly() {
		return indexOnly;
	}

	public LogRequest copy() {
		return copy(fromTimeMs, toTimeMs);
	}

	public LogRequest copy(long startMs, long endMs) {
		LogRequest request = new LogRequest(subscriber, startMs, endMs);
		copy(request);
        request.setStartTimeMs(startMs);
        request.setEndTimeMs(endMs);
		return request;
	}

	private void copy(LogRequest copy) {

        if (this.summaryBucket != null) copy.summaryBucket = this.summaryBucket.copy();
        copy.context = this.context;
        copy.cancelledAtomic = this.cancelledAtomic;

		if (this.hitCounter == null)
			this.hitCounter = new ConcurrentHashMap<String, Map<Long, AtomicLong>>();
		copy.hitCounter = this.hitCounter;

		for (Query query : queries) {
			query.hitCounter = this.hitCounter;
			copy.addQuery(query.copy(this.hitCounter));
		}
        copy.cacheKey = cacheKey();
		copy.multiTypeSearch = multiTypeSearch;
		copy.bucketCount = bucketCount;
		copy.bucketWidthSec = bucketWidthSec;
		copy.hosts = this.hosts;
		copy.fromTimeMs = this.fromTimeMs;
		copy.indexOnly = this.indexOnly;
		copy.maxItems = this.maxItems;
		copy.minReplay = this.minReplay;
		copy.fromTimeMs = this.fromTimeMs;
		copy.submittedTime = submittedTime;
		copy.subscriber = subscriber;
		copy.search = this.search;
		copy.timeToLiveMins = timeToLiveMins;
		copy.toTimeMs = this.toTimeMs;
		copy.indexOnly = this.indexOnly;
		copy.user = this.user;
		copy.verbose = this.verbose;
		copy.ignoreCase = this.ignoreCase;
		copy.streaming = this.streaming;
		copy.isSummaryRequired = this.isSummaryRequired;

		if (replay != null) {
        copy.replay = new Replay(replay);
	}
	}

	public LogRequest copy(long requestStart, long requestEnd, int queryIndex) {
		LogRequest request = new LogRequest(subscriber, requestStart, requestEnd);
		copy(request);
		request.queries.clear();
		request.queries.add(query(queryIndex));

		return request;
	}

	public void setBucketCount(int bucketCount) {
		if (bucketCount < LogProperties.getMaxBucketThreshold())
			this.bucketCount = bucketCount;
	}

	final public int getBucketCount() {
		if (bucketCount == 0)
			bucketCount = 1;
		if (bucketCount < 1) bucketCount = 100;
		return bucketCount;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public User getUser() {
		return user;
	}

	@Override
	public String toString() {
		return String.format("LogRequest Sub:%s Q:%s Buckets:%d Times[%s=>%s] LIVE:%b", subscriber, queries.toString(), bucketCount, DateUtil.shortDateTimeFormat1_1.print(getStartTimeMs()),
				DateUtil.shortDateTimeFormat1_1.print(getEndTimeMs()), isStreaming());
	}

	public String toStringId() {
		StringBuilder sb = new StringBuilder();
		for (Query q : queries()) {
			sb.append(q.toStringId());
		}
		return String.format("q:%s h:%s", sb.toString(), hosts);

	}



	final public boolean isCancelled() {
		return this.cancelled || this.cancelledAtomic != null && this.cancelledAtomic.get() == true;
	}

	public void setCancelled(boolean cancelled) {
		this.cancelled = cancelled;
	}

	final public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public void setSubmittedTime(long submittedTime) {
		this.submittedTime = submittedTime;
	}

	public short getTimeToLiveMins() {
		return timeToLiveMins;
	}

	final public boolean isExpired() {
		return DateTimeUtils.currentTimeMillis() > expiresAt();
	}

	public long expiresAt() {
		return (submittedTime + (timeToLiveMins * 60 * 1000));
	}

	public long getLastChanceTime() {
		return submittedTime + (timeToLiveMins * 60 * 1000) - 3000;
	}

	public short autocancel() {
		return timeToLiveMins;
	}

	public void setTimeToLive(short timetoLiveMins) {
		this.timeToLiveMins = timetoLiveMins;
	}

	public int minReplay() {
		return minReplay;
	}

	public void setMinReplay(int minReplay) {
		this.minReplay = minReplay;
	}

	public long getSubmittedTime() {
		return submittedTime;
	}


	public boolean isWithinTime(long lineTime) {
		return lineTime > fromTimeMs && lineTime < toTimeMs;
	}

	/**
	 * Cancel will cascade to all users of copies who check isCompleteSent,
	 * isCanceller then short circuit
	 */
	public void cancel() {
		this.cancelled = true;
        this.cancelledAtomic.set(true);
	}

	public LogRequest copyWithBucketCalc(long start, long end, int granularityMins) {
		LogRequest result = this.copy(start, end);
		long delta = end - start;
		int mins = (int) (delta / (60 * 1000));
		result.setBucketCount(mins / granularityMins);
		return result;
	}

	public String toTimeString() {
		return String.format(super.toString() + " %s %s -> %s", queries, new DateTime(getStartTimeMs()), new DateTime(getEndTimeMs()));
	}

	public boolean isStreaming() {
		return streaming;
	}

	public void setStreaming(boolean isStreaming) {
		this.streaming = isStreaming;
	}

	public long getBucketSizeMs() {
		int bucketCount = getBucketCount();
		long timeDiff = getEndTimeMs() - getStartTimeMs();
		long bucketSize = (timeDiff / bucketCount) < 1000 ? 1000 : timeDiff / bucketCount;
		return bucketSize;

	}

	public String getStartTime() {
		return DateUtil.shortDateTimeFormat22.print(getStartTimeMs());
	}

	public String getEndTime() {
		return DateUtil.shortDateTimeFormat22.print(getEndTimeMs());
	}

	public void createSummaryBucket(AggSpace aggSpace) {
		if (this.isSummaryRequired && this.summaryBucket == null) {
			this.summaryBucket = new SummaryBucket(this.subscriber, aggSpace);
		}
	}

	final public Bucket summaryBucket() {
		return this.summaryBucket;
	}

	/**
	 * Matches the file filter tags
	 * 
	 * @param logFile
	 * @return
	 */
	public boolean isSearchable(LogFile logFile, String currentHostname) {

        String fileHost = logFile.getFileHost(currentHostname);
        if (this.hosts != null) {
            if (!this.hosts.isMatch(fileHost)) return false;

        }
        Map<String,String> systemFields = new HashMap<String,String>();
        systemFields.put(FieldSet.DEF_FIELDS._agent.name(),VSOProperties.getResourceType());
        systemFields.put(FieldSet.DEF_FIELDS._filename.name(),logFile.getFileNameOnly());

        systemFields.put(FieldSet.DEF_FIELDS._host.name(), fileHost);
        systemFields.put(FieldSet.DEF_FIELDS._path.name(),logFile.getFileName());
        systemFields.put(FieldSet.DEF_FIELDS._tag.name(),logFile.getTags());
        systemFields.put(FieldSet.DEF_FIELDS._type.name(),logFile.getFieldSetId());

        if (!isSearchable(systemFields, queries())) return false;



        if (user == null) return true;
		else return  user.isFileAllowed(fileHost, logFile.getFileName(), logFile.getTags());
	}
    public static boolean isSearchable(Map<String, String> systemFields, Collection<Query> queries) {
        int excludeFailed = 0;
        int includeFailed = 0;
        for (Query query : queries) {
            if (!query.isFilter(false, systemFields)) excludeFailed++;
            if (!query.isFilter(true, systemFields)) includeFailed++;
        }

        if (excludeFailed == queries.size()) return false;
        if (includeFailed == queries.size()) return false;
        return true;

    }


	public void applyOffset(String offset) {
		if (offset == null || offset.length() == 0)
			return;
		if (offset.startsWith("-"))
			offset = offset.substring(1);
		long duration = this.toTimeMs - this.fromTimeMs;
        long totalOffset = 0;
		if (StringUtil.isIntegerFast(offset)) {
            int offsetSepcified = Integer.parseInt(offset);
			totalOffset = duration * offsetSepcified;
            // value of 0 telling it to skip to now
            if (offsetSepcified == 0) {
                this.toTimeMs = System.currentTimeMillis();
                this.fromTimeMs = this.toTimeMs - duration;
                offsetValueMs = 0;
                return;
            }
			// wind bac the clock
			totalOffset *= -1;
			this.fromTimeMs += totalOffset;
			this.toTimeMs += totalOffset;
		} else {
			long multiplier = getOffsetMultiplier(offset);
			Integer offsetAmount = StringUtil.isInteger(offset.substring(0, offset.length() - 1));
			if (offsetAmount == null)
				return;
			totalOffset = offsetAmount * multiplier;
			// wind back the clock
			totalOffset *= -1;
			this.fromTimeMs += totalOffset;
			this.toTimeMs += totalOffset;
		}
        this.offsetValueMs = totalOffset;
	}

	private long getOffsetMultiplier(String offset) {
		String unit = offset.substring(offset.length() - 1, offset.length());
		if (unit.equalsIgnoreCase("d"))
			return DateUtil.DAY;
		if (unit.equalsIgnoreCase("w"))
			return DateUtil.WEEK;
		if (unit.equalsIgnoreCase("h"))
			return DateUtil.HOUR;
		return 0;
	}

	public void setTimePeriodMins(short ttl) {
		long now = DateUtils.round(new Date(DateTimeUtils.currentTimeMillis()), Calendar.MINUTE).getTime();
		this.setStartTimeMs(now - (ttl * DateUtil.MINUTE));
		this.setEndTimeMs(now);
	}

	public String getGroupByFieldname() {
		for (Query query : this.queries) {
			List<Function> functions = query.functions();
			for (Function function : functions) {
				String applyToField = function.getApplyToField();
				String group = function.groupByGroup();
				if (group == null || group.length() == 0)
					return applyToField;
				else
					return group;
			}
		}
		return null;
	}

	long taskElapsedMs = -1;

	private boolean multiTypeSearch;

	public int elapsedSeconds() {
		if (taskElapsedMs == -1)
			taskElapsedMs = System.currentTimeMillis();
		return (int) (System.currentTimeMillis() - taskElapsedMs) / 1000;
	}

	/**
	 * Run in BG when
	 * - we have taken ages to get this item started; > 10 secs
	 * - we have specified a ttl > 3 mins
	 * - we are searching for > 1 day of data
	 * @return
	 */
	public boolean isNowBGWork() {
		// search more than LONG-Duration(2) days or long ttl or is taking a long time already
        boolean longSearch = getDurationMins() > LogProperties.getSlowDurationDays() * (24 * 60);
		return elapsedSeconds() > LogProperties.slowSearchThresholdSeconds() && longSearch  ||
                timeToLiveMins >= LogProperties.getBackgroundTTLThreshold() ||
                longSearch;
	}

	private long getDurationMins() {
		return DateUtil.getMinutesDiff(fromTimeMs, toTimeMs);
	}

	public void setBucketWidth(int bucketWidth) {
		if (bucketWidth > 0) {
			this.bucketWidthSec = bucketWidth;
			toTimeMs = rollforward(toTimeMs, 1);
			long durationMin = (toTimeMs - fromTimeMs)/DateUtil.MINUTE;
			toTimeMs = rollforward(toTimeMs, bucketWidth);
			bucketCount = (int) (durationMin/bucketWidth);
//			fromTimeMs = toTimeMs - ((long)this.bucketWidthSec * (long)DateUtil.SECOND * (long)bucketCount);
		}
	}

	public boolean isBucketWidthHit(long eventTime, long startTime, long endTime) {
		if (eventTime == 0 || bucketWidthSec <= 0 || eventTime < startTime || eventTime > endTime) return false;
		// need to enforce bucket width
		return (eventTime - startTime > DateUtil.SECOND * bucketWidthSec);
	}

	public int getBucketWidthSecs() {
		return bucketWidthSec;
	}

	public void setMultiTypeSearchFlag() {
		this.multiTypeSearch = true;
	}
	public boolean isMutliTypes() {
		return multiTypeSearch;
	}

	public long getTimeSinceSubmitted() {
		return System.currentTimeMillis() - submittedTime;
	}

    public long getOffsetValueMs(){
        return this.offsetValueMs;
    }

    public boolean isHitLimitDone(long bucketTime) {
        if (bucketTime == -1) return false;
        if (queries.size() ==1) {
            return queries.get(0).isHitLimitExceeded(bucketTime);
        }
        int done = 0;
        for (Query query : queries) {
            if (query.isHitLimitExceeded(bucketTime)) done++;
        }
        return done == queries.size();
    }
    public int getHits(long bucketTime) {
        if (queries.size() == 1) return query(0).getHits(bucketTime);
        int result = 0;
        for (Query query : queries) {
            result += query.getHits(bucketTime);
        }

        return result;
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public void addError(String error) {
        errors.add(error);
    }

    public List<String> getErrors() {
        return errors;
    }

    public void removeSystemFieldFilters() {
        if (queries.size() == 1) {
            for (Query query : queries) {
                query.removeSystemFieldFilters();
            }
        }
    }


	public String cacheKey() {
		if (this.cacheKey == null) {
			this.cacheKey = "I";
			for (Query query : queries) {
				this.cacheKey += Integer.toString(MurmurHash3.hashString(query.toStringId(), 10));
			}
		}
		return cacheKey;
	}

	public void makeSummaryIndex() {
		this.fromTimeMs = DateUtil.nearestMin(this.fromTimeMs, PersistingSummaryIndex.INCR);
		this.toTimeMs = DateUtil.nearestMin(this.toTimeMs, PersistingSummaryIndex.INCR);
		this.bucketWidthSec = 60 * PersistingSummaryIndex.INCR;
		this.bucketCount = (int) ((this.toTimeMs - this.fromTimeMs) / (this.bucketWidthSec * 1000));
    }
}
