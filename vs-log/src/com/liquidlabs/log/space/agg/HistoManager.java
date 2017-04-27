package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.functions.FunctionBase;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class HistoManager {

	DateTimeFormatter formatter = DateUtil.shortDateTimeFormat2;

	private final static Logger LOGGER = Logger.getLogger(HistoManager.class);

	public List<Map<String, Bucket>> newHistogram(DateTime fromTime,
			DateTime toTime, int bucketCount, List<Query> queries,
			String subscriber, String hostname, String sourceURI) {

        if (queries.size() == 0) {
            LOGGER.warn("No Search Expressions Provided request.query.size() == 0 Adding'*'");
            queries.add(new Query(0,"*"));
        }

		try {
			List<Map<String, Bucket>> histogram = new CopyOnWriteArrayList<Map<String, Bucket>>();
			List<Long> times = getBucketTimes(fromTime, toTime, bucketCount, false);
            if (times.size() == 1) {
                HashMap<String, Bucket> histoItem = new HashMap<String, Bucket>();
                for (Query query : queries) {
                    Bucket bucket = new Bucket(fromTime.getMillis(), toTime.getMillis(), query.functions(),
                            query.position(), query.pattern(),
                            sourceURI, subscriber, "");
                    histoItem.put(query.key(), bucket);
                }
                histogram.add(histoItem);
            } else {

                for (int i = 0; i < times.size() - 1; i++) {
                    long time = times.get(i);
                    long timeTo = times.get(i + 1);
                    HashMap<String, Bucket> histoItem = new HashMap<String, Bucket>();
                    for (Query query : queries) {
                        Bucket bucket = new Bucket(time, timeTo, query.functions(),
                                query.position(), query.pattern(),
                                sourceURI, subscriber, "");
                        histoItem.put(query.key(), bucket);
                    }
                    histogram.add(histoItem);
                }
            }
			return histogram;
		} catch (Throwable t) {
			throw new RuntimeException(String.format(
					"Failed to create Histo from:%s - %s/ buckets:%d",
					fromTime, toTime, bucketCount), t);
		}
	}

	/**
	 * Break down into day level buckets, hour level buckets or not!
	 * 
	 * @param bucketCount
	 * @return
	 */
	public List<Long> getBucketTimes(final DateTime fTime, final DateTime tTime, int bucketCount, boolean fixStartEnd) {
		if (bucketCount == 0)
			bucketCount = 1;
		DateTime fromTime = fTime;
		DateTime toTime = tTime;
        if (bucketCount > 1000) bucketCount = 1000;
		if (fixStartEnd) {
            if (fromTime.getSecondOfMinute() != 0 ) fromTime = fromTime.minusSeconds(fromTime.getSecondOfMinute());
			if (toTime.getSecondOfMinute() != 0) toTime = toTime.plusSeconds(60 - toTime.getSecondOfMinute());
		}

        if (bucketCount == 1) {
            return Arrays.asList(DateUtil.floorMin(fromTime.getMillis()));//, toTime.getMillis());
        }

		long timeWidth = (toTime.getMillis() - fromTime.getMillis()) / bucketCount;
		
		if (timeWidth == 0) {
			LOGGER.error("Given BAD TimeSpan:" + fromTime + " - " + toTime + " b:" + bucketCount);
			timeWidth = DateUtil.HOUR;
		}
		if (isDailyRolled(fromTime, toTime, bucketCount, timeWidth)) {
			return makeDailyBuckets(fromTime, toTime, bucketCount);

		}
		if (isHourlyRolled(fromTime, toTime, bucketCount, timeWidth)) {
			return makeHourlyBuckets(fromTime, toTime, bucketCount);

		}
		ArrayList<Long> times = new ArrayList<Long>();
        int minutesDuration = (int) ((toTime.getMillis() - fromTime.getMillis()) / DateUtil.MINUTE);
        if (minutesDuration > 5) {
            // roll to seconds accuracy
            if (fromTime.getSecondOfMinute() != 0 ) fromTime = fromTime.minusSeconds(fromTime.getSecondOfMinute());
            if (toTime.getSecondOfMinute() != 0) toTime = toTime.plusSeconds(60 - toTime.getSecondOfMinute());

        }


        if (timeWidth != DateUtil.MINUTE && timeWidth <= LogProperties.getBucketEventDetailThresholdSecs() * 1000) {
			timeWidth = (tTime.getMillis() - fTime.getMillis()) / bucketCount;
			// MS Threshold
			if (toTime.getMillis() - fromTime.getMillis() <= 10 * 1000) {
				for (long i = fromTime.getMillis(); i < toTime.getMillis(); i += timeWidth) {
					if (times.size() > 1000) throw new RuntimeException("BOOM:" + fromTime + " - " + toTime + " w:" + timeWidth);
					times.add(i);
				}

			} else {
				// Sec
                if (timeWidth < 1000) timeWidth = 1000;
                int bucket = (int) ( (toTime.getMillis() - fromTime.getMillis()) / timeWidth);

				for (long i = fromTime.getMillis(); i < toTime.getMillis(); i += timeWidth) {
					if (times.size() > 1000) throw new RuntimeException("BOOM:" + fromTime + " - " + toTime + " w:" + timeWidth);
                    safeAdd(times, i);
                }
			}

		} else {
			// Minute
            if (timeWidth < DateUtil.MINUTE) timeWidth = 1;
            else {
                double timeWidthD = (double) timeWidth / (double) DateUtil.MINUTE;
                timeWidth = timeWidth / DateUtil.MINUTE;
                if (timeWidthD - timeWidth > 0) {
                    timeWidth = timeWidth + 1;
                }
            }
            if (fromTime.getSecondOfMinute() != 0 ) fromTime = fromTime.minusSeconds(fromTime.getSecondOfMinute());
            if (toTime.getSecondOfMinute() != 0) toTime = toTime.plusSeconds(60 - toTime.getSecondOfMinute());

            if (bucketCount == 1) {
                return Arrays.asList( DateUtil.floorSec(fromTime.getMillis()),  DateUtil.floorSec(toTime.getMillis()));
            }

            long buckets = (toTime.getMillis() - fromTime.getMillis()) /(timeWidth * DateUtil.MINUTE);
            while (buckets > 1000) {
                timeWidth += 1;
                buckets = (toTime.getMillis() - fromTime.getMillis()) / (timeWidth * DateUtil.MINUTE);
            }

			for (long i = fromTime.getMillis(); i < toTime.getMillis(); i += timeWidth * DateUtil.MINUTE) {
                safeAdd(times, i);
			}
		}
		//times.add(toTime.getMillis());
		return times;
	}

    private void safeAdd(ArrayList<Long> times, long newTime) {
        newTime =  DateUtil.floorSec(newTime);
        if (times.size() > 0) {
          if (newTime != times.get(times.size()-1)) times.add(newTime);
        } else times.add(newTime);
    }

    private List<Long> makeDailyBuckets(DateTime fromTime, DateTime toTime, int bucketCount) {

        // LOGGER.info("Creating DAY based histo");
        ArrayList<Long> times = new ArrayList<Long>();

        if (bucketCount == 1) {
            times.add(DateUtil.floorDay(fromTime.getMillis()));
            times.add(DateUtil.floorDay(toTime.plusDays(1).getMillis()));
            return times;
        } else {
            // roll to hourly boundary
            fromTime = new DateTime(DateUtil.floorDay(fromTime.getMillis()));
            toTime = new DateTime(DateUtil.floorDay(toTime.plusDays(1).getMillis()));
        }
		long timeWidth = (toTime.getMillis() - fromTime.getMillis())
				/ bucketCount;

		int pos = 0;
        if (timeWidth < DateUtil.DAY) timeWidth = DateUtil.DAY;
		for (long i = fromTime.getMillis(); i < toTime.getMillis(); i += timeWidth) {
            safeAdd(times, DateUtil.floorDay(i));
			pos++;
		}
		return times;
	}

	private List<Long> makeHourlyBuckets(DateTime fromTime, DateTime toTime,
			int bucketCount) {
		// LOGGER.info("Creating HOUR based histo");
		ArrayList<Long> times = new ArrayList<Long>();

        if (bucketCount == 1) {
            times.add(DateUtil.floorHour(fromTime.getMillis()));
            times.add(DateUtil.floorHour(toTime.plusHours(1).getMillis()));
            return times;
        } else {
            // roll to hourly boundary
            fromTime = new DateTime(DateUtil.floorHour(fromTime.getMillis()));
            toTime = new DateTime(DateUtil.floorHour(toTime.plusHours(1).getMillis()));
        }
		long timeWidth = (toTime.getMillis() - fromTime.getMillis())
				/ bucketCount;

		int pos = 0;
        if (timeWidth < DateUtil.HOUR) timeWidth = 1;
        else {
            // need to use a consistent bucket width
            timeWidth = (timeWidth / DateUtil.HOUR);
        }
		for (long i = fromTime.getMillis(); i < toTime.getMillis(); i += timeWidth * DateUtil.HOUR) {
            safeAdd(times, i);
			pos++;
		}


		return times;
	}

	private boolean isDailyRolled(DateTime fromTime, DateTime toTime,
			int bucketCount, long bucketWidthMs) {
		if (bucketWidthMs >= DateUtil.DAY) {
            return true;
//			Days between = Days.daysBetween(fromTime, toTime);
//			if (between.getDays() > 5) {
//				return true;
//			}
		}
		DateTime fTime = fromTime.minusMinutes(fromTime.getMinuteOfDay());
		DateTime tTime = toTime.plusMinutes(24 * 60 - toTime.getMinuteOfDay());
		long bW = (tTime.getMillis() - fTime.getMillis())/bucketCount;
		if (bW >= DateUtil.DAY) {
			Days between = Days.daysBetween(fromTime, toTime);
			if (between.getDays() > 5) {
				return true;
			}
		}

		return false;
	}

	private boolean isHourlyRolled(DateTime fromTime, DateTime toTime,
			int bucketCount, long bucketWidthMs) {
		if (bucketWidthMs >= DateUtil.HOUR) {
			Hours hoursBetween = Hours.hoursBetween(fromTime, toTime);
			if (hoursBetween.getHours() > 23) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Note: Incoming feed from multiple network senders, to support a level of
	 * concurrency we sync the agg sourceBucket - so incomings on different buckets
	 * can execute simultaneously
	 * 
	 * @return
	 */
	public Bucket handle(List<Map<String, Bucket>> aggregateHisto, Bucket sourceBucket, LogRequest request, int handledSize) {

		if (sourceBucket.hits() == 0)
			return null;

		Bucket aggBucket = getTargetBucket(aggregateHisto, sourceBucket);
        if (aggBucket == null) {
            LOGGER.warn("Failed to find sourceBucket:" + sourceBucket.getPattern());
            return null;
        }
		synchronized (aggBucket) {
			handle(aggBucket, sourceBucket, handledSize, request.isVerbose(), true);
		}
		return aggBucket;
	}


	private Bucket getLastBucket(List<Map<String, Bucket>> aggregateHisto) {
		return aggregateHisto.get(aggregateHisto.size() - 1).values()
				.iterator().next();
	}

    public Bucket getTargetBucket(List<Map<String, Bucket>> histo, Bucket bucket) {
        Map<String, Bucket> buckets = null;
		int bucketNumber = getBucketIndex(histo, bucket);
        Bucket firstBucket = histo.get(0).values().iterator().next();
        long start = firstBucket.getStart();
        long end = histo.get(histo.size()-1).values().iterator().next().getStart();
        long width = firstBucket.getEnd() - firstBucket.getStart();
        if (bucketNumber < -10000) {
            LOGGER.warn("CRAP BUCKET:" + bucket);
//            return   histo.get(0).values().iterator().next();
        }

        if (bucketNumber < 0) {
            //System.out.println("BucketPrepending:" + (histo.size() - bucketNumber));
            for (int i = bucketNumber; i < 0; i++) {
                long bStart = histo.get(0).values().iterator().next().getStart();
                long time = bStart - width;
                long timeTo = bStart-1;
                Map<String,Bucket> head = histo.get(0);
                HashMap<String, Bucket> histoItem = new HashMap<String, Bucket>();
                for (String quid : head.keySet()) {
                    Bucket bucketCopy =  head.get(quid).copy();
                    bucketCopy.setTimes(time, timeTo);
                    bucketCopy.resetAll();
                    histoItem.put(quid, bucketCopy);
                }
                histo.add(0, histoItem);
                // grab the first one
                if (buckets == null) {
                    buckets = histoItem;
                }
            }
        } else if (bucketNumber > 2000) {
            // doing some madness - do just give it the first bucket

            return buckets.get(0);
        } else if (bucketNumber >= histo.size())  {
            //System.out.println("BucketAppending:" + (histo.size() - bucketNumber));
            int ss = histo.size();
            for (int i = ss-1; i < bucketNumber; i++) {
                long bStart = histo.get(histo.size()-1).values().iterator().next().getStart();
                long time = bStart + width;
                //System.out.println("Size:" + histo.size() + "Last:" + new DateTime(bStart));
                Map<String,Bucket> head = histo.get(0);
                HashMap<String, Bucket> histoItem = new HashMap<String, Bucket>();
                for (String quid : head.keySet()) {
                    Bucket bucketCopy =  head.get(quid).copy();
                    bucketCopy.setTimes(time, time + width);
                    bucketCopy.resetAll();
                    histoItem.put(quid, bucketCopy);
                }
                histo.add(histoItem);
                // grab the last one
                buckets = histoItem;
            }
        } else {
            buckets = histo.get(bucketNumber);
        }
		return buckets.get(bucket.getQueryPos() + bucket.getPattern());
	}

	private int getBucketIndex(List<Map<String, Bucket>> histo, Bucket bucket) {
        List<Long> bucketTimes = new ArrayList<Long>();
        long end = -1;
        for (Map<String, Bucket> item : histo) {
            Bucket next = item.values().iterator().next();
            bucketTimes.add(next.getStart());
            end = next.getEnd();
        }
        bucketTimes.add(end);
        return getBucketIndex(bucket.getStart(), bucketTimes);
	}

	public void handle(Bucket aggBucket, Bucket sourceBucket, int handledSize, boolean verbose, boolean allowPostAggs) {

		aggBucket.aggregate(sourceBucket);

		// preserve source file hit count
		try {
			if (handledSize > 0 && verbose && sourceBucket.hits() > 0)
				LOGGER.info(String.format(
                        "LOGGER b:%d sub:%s GROUP Source[%s] QueryPos[%s] Pattern[%s] Hits[%d][%s=>%s]",
                        handledSize,
                        aggBucket.subscriber(),
                        sourceBucket.getSourceURI(),
                        sourceBucket.getQueryPos(),
                        sourceBucket.getPattern(),
                        sourceBucket.hits(),
                        formatter
                                .print(sourceBucket.getStart()),
                        formatter.print(sourceBucket.getEnd())));
			aggregateGroupingFunctionForBucket(aggBucket, sourceBucket, verbose, allowPostAggs);

		} catch (Throwable t) {
			LOGGER.info(String.format(
                    "LOGGER Bucket Error %s hits[%d] pattern[%s]", sourceBucket
                            .subscriber(), sourceBucket.hits(), sourceBucket
                            .getPattern()));
			LOGGER.warn("Failed to handle Bucket", t);
            throw new RuntimeException(t);
		}
	}

	@SuppressWarnings("unchecked")
	void aggregateGroupingFunctionForBucket(final Bucket aggBucket, Bucket sourceBucket, boolean verbose, boolean allowPostAggs) {

		for (Function aggFunction : aggBucket.functions()) {

			Map<String, Object> sourceBucketFunctionResults = sourceBucket.getAggregateResult(aggFunction.toStringId(), verbose);

			if (sourceBucketFunctionResults == null) {
				if (verbose) {
					LOGGER.warn(String.format(
                            "AggResults==NULL Fun:[%s] Tag:%s, sourceBucket:%s",
                            aggFunction.toStringId(),
                            aggFunction.getTag(), sourceBucket
                            .toString2()));
					LOGGER.warn(String.format("\t\t       %s", sourceBucket
							.getAggregateResults().keySet()));
				}
				continue;
			}

//			boolean failed = true;
//			while (failed) {
				try {
					applyFunctionResults(aggBucket, sourceBucket, verbose,
							aggFunction, sourceBucketFunctionResults, allowPostAggs);
//					failed = false;
				} catch (ConcurrentModificationException ce) {
					LOGGER.info("ce:" + ce.getMessage());
                    ce.printStackTrace();
				}
//			}
		}
	}


    /**
     * Syncd because concurrent hashmap updates can cause infinite loops!
     * @param aggBucket
     * @param sourceBucket
     * @param verbose
     * @param aggFunction
     * @param sourceBucketFunctionResults
     * @param allowPostAggs
     */
	synchronized private void applyFunctionResults(final Bucket aggBucket,
                                                   Bucket sourceBucket, boolean verbose, Function aggFunction,
                                                   Map<String, Object> sourceBucketFunctionResults, boolean allowPostAggs) {

		if (verbose) {
			LOGGER.info("LOGGER >> Aggregating:" + sourceBucket.toStringTime()
					+ " func:" + aggFunction.getTag() + " sourceData:"
					+ sourceBucketFunctionResults.toString());
		}
        if (allowPostAggs && FunctionBase.isPostAggFunction(aggFunction, aggBucket.functions())) return;

		for (String groupName : sourceBucketFunctionResults.keySet()) {
			Object object = sourceBucketFunctionResults.get(groupName);
			if (object instanceof Double) {
				// sum, average, etc
				aggFunction.updateResult(groupName, (Double) object);
				if (verbose)
					LOGGER.info(String.format(
                            "LOGGER >>> Aggregate DOUBLE Tag[%s] t[%s] aggHits[%d] hits[%d] == t[%s] hits[%d] source[%s] pattern[%s] groupName[%s]",
                            aggFunction.getTag(),
                            formatter.print(aggBucket
                                    .getStart()), aggBucket
                            .hits(), sourceBucket
                            .hits(), formatter
                            .print(sourceBucket
                                    .getStart()),
                            aggBucket.hits(), sourceBucket
                            .hits(), sourceBucket
                            .getSourceURI(), aggBucket
                            .getPattern(), groupName));

			} else if (object instanceof Map) {
				// count - groupBy(x) count(y)
				if (verbose)
					LOGGER.info(String.format(
                            "LOGGER >>> Aggregate MAP(a) Tag[%s] t[%s] aggHits[%d] hits[%d] == t[%s] hits[%d] source[%s] pattern[%s] groupName[%s]",
                            aggFunction.getTag(),
                            formatter.print(aggBucket
                                    .getStart()), aggBucket
                            .hits(), sourceBucket
                            .hits(), formatter
                            .print(sourceBucket
                                    .getStart()),
                            aggBucket.hits(), sourceBucket
                            .hits(), sourceBucket
                            .getSourceURI(), aggBucket
                            .getPattern(), groupName));
				if (aggFunction.isBucketLevel())
					aggFunction.updateResult(groupName, (Map) object);
			} else if (object instanceof Set) {
				// count - groupBy(x) count(y)
				if (verbose)
					LOGGER.info(String.format(
                            "LOGGER >>> Aggregate SET(b) Tag[%s] t[%s] aggHits[%d] hits[%d] == t[%s] hits[%d] source[%s] pattern[%s] groupName[%s]",
                            aggFunction.getTag(),
                            formatter.print(aggBucket
                                    .getStart()), aggBucket
                            .hits(), sourceBucket
                            .hits(), formatter
                            .print(sourceBucket
                                    .getStart()),
                            aggBucket.hits(), sourceBucket
                            .hits(), sourceBucket
                            .getSourceURI(), aggBucket
                            .getPattern(), groupName));
				aggFunction.updateResult(groupName, (Set) object);

			} else {
                if (verbose) {
                    LOGGER.info(String.format(
                            "LOGGER >>> Aggregate MAP(c) Tag[%s] t[%s] aggHits[%d] hits[%d] == t[%s] hits[%d] source[%s] pattern[%s] groupName[%s] srcBId[%s]",
                            aggFunction.getTag(),
                            formatter.print(aggBucket
                                    .getStart()), aggBucket
                                    .hits(), sourceBucket
                                    .hits(), formatter
                                    .print(sourceBucket
                                            .getStart()),
                            aggBucket.hits(), sourceBucket
                                    .hits(), sourceBucket
                                    .getSourceURI(), aggBucket
                                    .getPattern(), groupName, sourceBucket.id() ));
                    LOGGER.info("LOGGER >>> Aggregate MAP(bucket):" + sourceBucket);
                }

                aggFunction.updateResult(groupName, sourceBucketFunctionResults);
            }
		}
	}

	public int updateFunctionResults(List<Map<String, Bucket>> histo,boolean verbose) {
		int result = 0;
		for (Map<String, Bucket> histoItem : histo) {
			for (Bucket bucket : histoItem.values()) {
				bucket.convertFuncResults(verbose);
				result += bucket.hits();
			}
		}
		return result;
	}

	public boolean isBucketOverlap(Bucket targetMaybe, Bucket sourceMaybe) {
		return (targetMaybe.getStart() <= sourceMaybe.getEnd() && targetMaybe
				.getEnd() >= sourceMaybe.getStart());
	}

	public void copy(List<Map<String, Bucket>> destination,
			List<Map<String, Bucket>> source, long bucketWidthMins) {
		for (Map<String, Bucket> sourceBucketSet : source) {
			Bucket sourceBucket = sourceBucketSet.values().iterator().next();
			putBucket(destination, sourceBucket, sourceBucketSet,
					bucketWidthMins);
		}
	}

	void putBucket(List<Map<String, Bucket>> destHisto, Bucket sourceBucket,
			Map<String, Bucket> sourceBucketSet, long bucketWidthMins) {
		long destHistoStart = destHisto.get(0).values().iterator().next()
				.getStart();
		int index = 0;
		long sourceBucketStart = sourceBucket.getStart();
		if (destHistoStart == sourceBucketStart)
			index = 0;
		else
			index = (int) ((sourceBucket.getStart() - destHistoStart) / bucketWidthMins);

		if (index < 0)
			return;
		if (index >= destHisto.size())
			return;
		// LOGGER.info(new DateTime(sourceBucketStart) + " replace>>>:" + index
		// + " size:" + destHisto.size());
		destHisto.remove(index);
		destHisto.add(index, sourceBucketSet);
	}

    public int getBucketIndex(long eventTime, List<Long> bucketTimes) {
        int bucketTimesSize = bucketTimes.size();
        if (bucketTimesSize == 1) return 0;
        int lastClose = 0;

        for (int i = 0; i < bucketTimesSize - 1; i++) {
            long cTime = bucketTimes.get(i);
            long nTime = bucketTimes.get(i + 1) - 1;

            if (eventTime >= cTime && eventTime < nTime)
                return i;
            if (eventTime >= cTime)
                lastClose = i;
        }
        long first = bucketTimes.get(0);
        long last = bucketTimes.get(bucketTimesSize -1);

        long bucketWidth = bucketTimes.get(1) - first;
        // before histo
        if (eventTime < first) {
            return (int)  ( ((first - eventTime) / bucketWidth) * -1) -1;
        }
        // after histo
        if (eventTime > last) {
            return (int) ((eventTime - last) / bucketWidth + bucketTimesSize);

        }
        if (eventTime == bucketTimes.get(bucketTimesSize -1)) return bucketTimesSize -1;
        // sometimes we need to allow for clock drift
        return -1;// lastClose;
    }


    // need to calculate bucket times when they are outside of this histo
    public long[] getStartEndBucketTime(List<Long> histo, long eventTime, long bucketWidth) {

        long first = histo.get(0);
        long last = histo.get(histo.size() - 1) + bucketWidth;


        // int buckets = histo.size();
        if (eventTime > last) {
            // get delta time
            long delta = eventTime - first;
            long mod = delta / bucketWidth;
            long bStart = mod * bucketWidth;
            long bEnd = bStart + bucketWidth;
            return new long[] { bStart + first, bEnd + first };
        }
        for (int i = 0; i < histo.size() - 1; i++) {
            long bS = histo.get(i);
            long bE = histo.get(i + 1);
            if (eventTime >= bS && eventTime < bE)
                return new long[] { bS, bE };
        }
        if (eventTime <= last) {
            return new long[] {histo.get(histo.size()-1), last};
        }
        return new long[] { first, last };

    }

	public void resetHisto(List<Map<String, Bucket>> histo) {
		for (Map<String, Bucket> map : histo) {
			for (Bucket aggBucket : map.values()) {
				aggBucket.resetAll();
			}
		}
	}

	public void handle(List<Map<String, Bucket>> aggHist, List<Map<String, Bucket>> histo, LogRequest request) {

		for (Map<String, Bucket> map : histo) {
			Collection<Bucket> buckets = map.values();
			for (Bucket bucket : buckets) {
				this.handle(aggHist, bucket, request, 1);
			}
		}
	}

	public List<Map<String, Bucket>> newHistogram(LogRequest request) {
		return newHistogram(new DateTime(request.getStartTimeMs()),
				new DateTime(request.getEndTimeMs()), request.getBucketCount(),
				request.copy().queries(), request.subscriber(), null,
				"sourceURI");
	}

	public void clearFunctionResults(List<Map<String, Bucket>> aggHisto) {
		for (Map<String, Bucket> map : aggHisto) {
			Collection<Bucket> buckets = map.values();
			for (Bucket bucket : buckets) {
				bucket.getAggregateResults().clear();
			}
		}
	}

}
