package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.agg.ClientHistoItem.SeriesValue;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class HistogramHandler {

	private final static Logger LOGGER = Logger.getLogger(HistogramHandler.class);
    private List<Map<String, Bucket>> histogram;
    DateTimeFormatter formatter3 = DateTimeFormat.shortDateTime();
	

	private ScheduledExecutorService scheduler;
    private LogRequest request;
    private LogSpace logspace;
    private HistoManager histoReducer = new HistoManager();

    public HistogramHandler() {
	}
	
	public HistogramHandler(ScheduledExecutorService scheduler, LogRequest request, LogSpace logspace) {
		this.scheduler = scheduler;
        this.request = request;
        this.logspace = logspace;
        this.histogram = new HistoManager().newHistogram(request);
	}
	
	public void handle(String providerId, final String subscriber, int histoSize, List<Map<String, Bucket>> histo) {
        synchronized (histogram) {
            histoReducer.handle(histogram, histo, request);
        }
	}
	
	public List<ClientHistoItem> getHistogramForClient(List<Map<String, Bucket>> histogram, LogRequest request) {
		
		// if we have -LIVE- AND it is single bucket request.. - then it would have been turned into a 60 bucket histo to support scrolling
		if (request.subscriber().contains("-LIVE-") && request.queries().get(0).sourceQuery().contains("buckets(1)")){
			// we have to aggregate this histo into a single bucket one.
			HistoManager histoManager = new HistoManager();
			LogRequest copy = request.copy();
			copy.setBucketCount(1);
			List<Map<String, Bucket>> newHistogram = histoManager.newHistogram(copy);
			
			histoManager.handle(newHistogram, histogram, request);
			histogram = newHistogram;
		}
		
		
		HashMap<Integer,Set<String>> functionTags = new HashMap<Integer,Set<String>>();
		String subscriber = request.subscriber();
		
		List<FieldSet> fieldSets = logspace.fieldSets();
		
		List<ClientHistoItem> rawResult = new ClientHistoAssembler().getClientHistoFromRawHisto(request, histogram, functionTags, fieldSets);
		
		return getHistogramForClient(subscriber, request, rawResult, functionTags, fieldSets);
	}

	public List<ClientHistoItem> getHistogramForClient(String subscriber, LogRequest request, List<ClientHistoItem> result, Map<Integer,Set<String>> functionTags, List<FieldSet> fieldSets) {

		if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("loadClientHistogram LOGGER SEARCH[%s] from[%s] to[%s]", subscriber, formatter3.print(request.getStartTimeMs()), formatter3.print(request.getEndTimeMs())));

		if (result.size() == 0) return result;
		
		// We dont want to evict when there is a table which needs to be linked
		String fullPattern = request.query(0).sourceQuery();
		boolean isEvictingUsingTimeSeries = !fullPattern.contains("chart(table)");
		
		boolean isVerbose = request.isVerbose();

		int qPos = 0;

        if (isEvictingUsingTimeSeries) {
            evictTopOrBottomItems(request, result, getGroupBasedHitMap(result, request.queries().size(), request.query(0).topLimit() > 0), functionTags, qPos);
        }
		
		final ClientHistoItem firstItem = result.get(0);
		getSearchSeriesFromQueriesAndSetupFirstItemWithSeriesNames(request.queries(), result, firstItem, functionTags, isVerbose, fieldSets);
		if (isVerbose) LOGGER.info("LOGGER Series:" +firstItem.meta.allSeriesNames);
		
		List<String> topSortedFields = new ArrayList<String>(firstItem.meta.allSeriesNames);
		if (request.getBucketCount() == 1){// pie or table
            try {
			Collections.sort(topSortedFields, new Comparator<String>(){

				public int compare(String o1, String o2) {
                    try {
					    return Double.valueOf(firstItem.series.get(o2).value).compareTo(firstItem.series.get(o1).value);
                    } catch (Throwable t) {
                        return o1.compareTo(o2);
                    }
				}
				
			});
            } catch (Throwable t) {
                LOGGER.warn("Failed to sort:", t);
            }
		}
		
		updateHistogramForTable(result, fullPattern, firstItem.meta.allSeriesNames, request, fieldSets, topSortedFields);
		
		updateHistogramForPieChart(result, fullPattern, firstItem);
		
		for (ClientHistoItem clientHistoItem : result) {
			clientHistoItem.escapeForPresentation();
			clientHistoItem.getMaxValue(firstItem.meta.maxValues);
		}
		
		return result;
	}

    void evictTopOrBottomItems(LogRequest request, List<ClientHistoItem> clientHistogram, List<List<String>> evictOrderByGroup,  Map<Integer, Set<String>> functionTags, int qPos) {

        for (List<String> evictListForQuery : evictOrderByGroup) {

            Query query = request.query(qPos++);
            int topLimitForQuery = query.topLimit();
            if (topLimitForQuery < 0) topLimitForQuery *= -1;

            int position = query.getSourcePos();

            if (topLimitForQuery > evictListForQuery.size()) continue;

            List<String> seriesToEvict = new ArrayList<>();

            // if regexp grouping
            if (query.isGroupBy()) {
                // start again!
                seriesToEvict.clear();
                Set<String> tags = functionTags.get(position);
                if (tags == null) {
                    LOGGER.warn("No TAGs for query.position:" + position + " TAGS:" + functionTags);
                    continue;
                }
                Map<String, AtomicInteger> evictCount = new HashMap<>();
                for (String evictItem : evictListForQuery) {
                    String thisTag = "";
                    for (String tag : tags) {
                        if (evictItem.contains(tag)) thisTag = tag;
                    }
                    if (!evictCount.containsKey(thisTag)) evictCount.put(thisTag, new AtomicInteger());
                    evictCount.get(thisTag).incrementAndGet();
                    int seriesForQuery = evictCount.get(thisTag).intValue();
                    if (seriesForQuery > topLimitForQuery) {
                        seriesToEvict.add(evictItem);
                    }
                }
            }
            for (ClientHistoItem item : clientHistogram) {
               item.evict(seriesToEvict, query.isTopOther());
            }
        }
    }

    private void updateHistogramForPieChart(List<ClientHistoItem> result, String fullPattern, final ClientHistoItem firstItem) {
		// Pie Chart requested
		if (fullPattern.contains("chart(pie)")) {
			firstItem.applyPieChartData(result);
		}
	}
	private void updateHistogramForTable(List<ClientHistoItem> result, String fullPattern, List<String> seriesList, LogRequest request, List<FieldSet> fieldSets, List<String> topSortedFields) {
        int topLimitForTable = Integer.getInteger("table.top.limit", 5000);
		if (fullPattern.contains("chart(table)")) {
			String topFieldName = getTopFieldName(fullPattern);
			ClientHistoItem clientHistoItem = result.get(0);
			if (result.size() == 1) {
				new ClientHistoBuilder().buildTableXml(clientHistoItem, request.subscriber(), fullPattern.contains("groupBy"), request.getGroupByFieldname(), topFieldName, topLimitForTable, seriesList, topSortedFields, scheduler, !fullPattern.contains("bottom"), fullPattern.contains("chart(table)"));
			} else {
				clientHistoItem.adaptTimeSeriesToXML(result);				
			}
		}
	}
	
	private String getTopFieldName(String fullPattern) {
		int topIndex = fullPattern.indexOf("top(") + "top(".length();
		if (fullPattern.indexOf("top(") == -1) topIndex = fullPattern.indexOf("bottom(") + "bottom(".length();
		String topValue = fullPattern.substring(topIndex, fullPattern.indexOf(")", topIndex));
		// allow top to contains a fieldName
		if (topValue.contains(",")) {
			String[] split = topValue.split(",");
			if (split.length > 1) return split[1];
		}
		return "";
	}

	void getSearchSeriesFromQueriesAndSetupFirstItemWithSeriesNames(List<Query> queries, List<ClientHistoItem> result, ClientHistoItem firstItem, Map<Integer,Set<String>> functionTags, boolean verbose, List<FieldSet> fieldSets) {
		// simple mode has groupId spanning queries (groupBy == false)
		// regexp mode has groupId within a query (groupBy == true)
		int groupId = 0;
		int lastSimpleQPos = 0;
		LinkedHashSet<String> allSeries = new LinkedHashSet<String>();
		FieldSet fieldSet = null;
		for (Query query : queries) {
			boolean isSimpleSearch = !query.isGroupBy();

			
			List<String> seriesForQuery = getSeriesForQuery(query.position(), result);
			
			Map<String, Integer> seriesGroupMapping = getSeriesGroupMap(seriesForQuery, functionTags.get(query.getSourcePos()));
			
			
			if (verbose) {
				int position = query.getSourcePos();
				LOGGER.info(position + "." + groupId + " Series:" + seriesForQuery);
				LOGGER.info(position + "." + groupId + " Tags:" + functionTags.get(query.getSourcePos()));
				LOGGER.info(position + "." + groupId + " SeriesToGroupIds:" + seriesGroupMapping);
			}
			// setup/reset counters based upon if this is a simple or regexp
			
			// reset groupId when RegExp or we are using a different sourcePos
			if (!isSimpleSearch || query.getSourcePos() != lastSimpleQPos)  groupId = 0;
			
			
			// build the series for this query
			for (String seriesName : seriesForQuery) {
				if (!isSimpleSearch) {
					if (seriesGroupMapping.containsKey(seriesName)) {
						groupId = seriesGroupMapping.get(seriesName);
					} else {
						LOGGER.warn("Failed to get GroupId for series[" + seriesName +"]");
					}
				}
				SeriesValue sv = firstItem.incrementSpecial(seriesName, 0, query.getSourcePos(), groupId);
				String fieldName = getFieldNameForSeries(seriesName, result);
				if (fieldSet != null) {
					FieldI field = fieldSet.getField(fieldName);
					if (field != null) {
						sv._view = field.description();
					}
				}
				
			}
			allSeries.addAll(seriesForQuery);
			
			if (isSimpleSearch)  {
				groupId++;
				lastSimpleQPos = query.getSourcePos();
			} else groupId = 0;
		}
		firstItem.meta.allSeriesNames = new ArrayList<String>(allSeries);
	}
	
	private String getFieldNameForSeries(String seriesName, List<ClientHistoItem> result) {
		for (ClientHistoItem clientHistoItem : result) {
			SeriesValue sv = clientHistoItem.series.get(seriesName);
			if (sv != null) return sv.fieldName;
		}
		return null;
	}
	public int getGroupIndex(int querySource, int queryNumber, List<Query> queries) {
		if (querySource == 0) return queryNumber;
		int groupId = 0;
		for (Query query : queries) {
			if (query.getSourcePos() < querySource) {
				groupId = 0;
				continue;
			}
			if (query.position() == queryNumber) return groupId;
			groupId++;
		}
		return 0;
	}

	List<List<String>> getGroupBasedHitMap(List<ClientHistoItem> result, int queryCount, final boolean descending) {
		
		Set<String> series = new HashSet<String>();
		Map<String, SeriesValue> hitMap = new HashMap<String, SeriesValue>();
		for (ClientHistoItem clientHistoItem : result) {
			Set<String> keySet = clientHistoItem.series.keySet();
			series.addAll(keySet);
			for (String key : keySet) {
				SeriesValue histoServiceValue = clientHistoItem.series.get(key);
				if (!hitMap.containsKey(key)) {
					SeriesValue sv = new SeriesValue(key);
					sv.queryPos = histoServiceValue.queryPos;
					hitMap.put(key, sv);
				}
				hitMap.get(key).increment(histoServiceValue.value);
			}
		}

		ArrayList<SeriesValue> seriesSortedByValueLists = new ArrayList<SeriesValue>(hitMap.values());
		Collections.sort(seriesSortedByValueLists, new Comparator<SeriesValue>(){
			public int compare(SeriesValue o1, SeriesValue o2) {
                if (descending) return new Double(o2.value).compareTo(o1.value);
                else return new Double(o1.value).compareTo(o2.value);
			}
		});
		ArrayList<List<String>> results = new ArrayList<List<String>>();
		for (int i = 0; i < queryCount; i++) {
			ArrayList<String> seriesForQuery = new ArrayList<String>();
			results.add(seriesForQuery);
			for (SeriesValue sv : seriesSortedByValueLists) {
				if (sv.queryPos == i) {
					seriesForQuery.add(sv.label);
				}
			}
		}
		
		return results;
	}
	
	private List<String> getSeriesForQuery(int qPos, List<ClientHistoItem> histogram) {
		Map<Integer, TreeSet<String>> allSeries = new TreeMap<Integer, TreeSet<String>>();
		for (ClientHistoItem clientHistoItem : histogram) {
			List<SeriesValue> seriesNames = clientHistoItem.getSeriesNames(qPos);
			for (SeriesValue seriesValue : seriesNames) {
				if (!allSeries.containsKey(seriesValue.groupId)) allSeries.put(seriesValue.groupId, new TreeSet<String>());
				allSeries.get(seriesValue.groupId).add(seriesValue.label);
			}
		}
		ArrayList<String> results = new ArrayList<String>();
		
		for (Map.Entry<Integer, TreeSet<String>> entry : allSeries.entrySet()) {
			results.addAll(entry.getValue());
		}		
		return results;
	}
	private Map<String,Integer> getSeriesGroupMap(List<String> series, Set<String> tagsForQuery) {
		Map<String,Integer> result = new HashMap<String,Integer>();
		if (tagsForQuery == null) {
//			tagsForQuery = new HashSet<String>(series);
			return result;
		}
		String lastTag = "";
		int group = 0;
		for (String seriesName : series) {
			String thisTag = "";
			for (String tag : tagsForQuery) {
				if (seriesName.contains(tag) || seriesName.equals(tag)) {
					thisTag = tag;
				}
			}
			if (thisTag.equals(lastTag)){
				group++;
			}
			else group = 0;
			
			result.put(seriesName, group);
			lastTag = thisTag;
		}
		return result;
	}

	public DateTime calculateOtherTime(String source, DateTime fromTime, int requiredCount, boolean reverseIt) {
		DateTime calculateOtherTime = calculateOtherTime(loadBucketHitsOverTime(source), fromTime, requiredCount, reverseIt);
		if (calculateOtherTime.equals(fromTime)) {
			LOGGER.info("Using 60 mins window because the search expired");
			if (reverseIt) return fromTime.minusMinutes(60);
			else return fromTime.plusMinutes(60);
		}
		return calculateOtherTime;
	}
	
	@SuppressWarnings("unchecked")
	public DateTime calculateOtherTime(LinkedHashMap<DateTime, Integer> hitsOverTime, DateTime fromTime, int requiredCount,
			boolean reverseIt) {
		while (true) {
			try {
				ArrayList<DateTime> keys = new ArrayList<DateTime>(hitsOverTime.keySet());
				int gotCount = 0;
				if (reverseIt) {
					Collections.sort(keys, new Comparator<DateTime>() {

						public int compare(DateTime o1, DateTime o2) {
							return o2.compareTo(o1);
						}
					});
				} else {
					Collections.sort(keys);
				}

				int pos = -1;
				DateTime lastTime = fromTime;
				for (DateTime time : keys) {
					lastTime = time;
					pos++;
					if (reverseIt) {
						if (time.getMillis() > fromTime.getMillis())
							continue;
						gotCount += hitsOverTime.get(time);
						if (gotCount >= requiredCount) {
							DateTime nextBucketTime = keys.get(pos + 1);
							int factor = gotCount / requiredCount;
							if (factor > 2) factor /= 2;
							long delta = nextBucketTime.getMillis() - time.getMillis();
							delta = delta / factor;
							if (factor == 1) delta = 0;
							DateTime dateTime = new DateTime(time.getMillis() + delta);
							
							// still on the first bucket
							if (time.getMillis() == fromTime.getMillis()) {
								return fromTime.minus(delta);
							}
							// dont scan less than a minute period
							if (fromTime.getMillis() - dateTime.getMillis() < 60 * 1000) {
								dateTime = fromTime.minusMinutes(1);
							}
							return dateTime;
						}
					} else {
						if (time.getMillis() < fromTime.getMillis())
							continue;
						gotCount += hitsOverTime.get(time);
						if (gotCount >= requiredCount) {

							// try and counter a massive amount of data by
							// cutting the bucket in half
							if (gotCount > requiredCount * 5 && pos < keys.size() - 1) {
								DateTime nextBucketTime = keys.get(pos + 1);
								int factor = gotCount / requiredCount;
								// split it in half
								if (factor > 2) factor /= 2;
								long delta = nextBucketTime.getMillis() - time.getMillis();
								delta = delta / factor;
								if (factor == 1) delta = 0;

								DateTime dateTime = new DateTime(time.getMillis() - delta);
								// still on the first bucket
								if (time.getMillis() == fromTime.getMillis()) {
									return fromTime.plus(delta);
								}
								// dont scan less than a minute period
								if (dateTime.getMillis() - fromTime.getMillis() < 60 * 1000) {
									dateTime = fromTime.plusMinutes(1);
								}
								return dateTime;
							}
							// we want to return the next bucket time
							if (pos < keys.size() - 1) {
								return keys.get(pos + 1);
							}
							return time;
						}
					}
				}
				return lastTime;
			} catch (ConcurrentModificationException ex) {
			}
		}
	}

	/**
	 * Used by the client and also to collection host and source count
	 */
	public LinkedHashMap<DateTime, Integer> loadBucketHitsOverTime(String source) {
		LinkedHashMap<DateTime, Integer> results = new LinkedHashMap<DateTime, Integer>();
		List<Map<String, Bucket>> histosFromProviders = histogram;
		LogRequest logRequest = request;

		if (logRequest == null){
			LOGGER.info("Didnt find Request for source:" + source);
			return results;
		}
		if (histosFromProviders == null) {
			LOGGER.info("Didnt find Histogram for source:" + source);
			return results;
		}
		Map<String, Boolean> isReplaying = getReplayingQueryKeys(logRequest);
		
//		Collection<List<Map<String, Bucket>>> values = histosFromProviders.values();
		
		for (Map<String, Bucket> map2 : histosFromProviders) {
			synchronized (map2) {
//				for (Map<String, Bucket> map2 : list) {
					
					for (String key : map2.keySet()) {
						// if a replay is switched off then ignore the hitcount
						if (logRequest.isVerbose()) LOGGER.info(" Replay:"+ isReplaying.get(key) + " key:" + key);
						if (!isReplaying.get(key)) {
							continue;
						}
						Bucket bucket = map2.get(key);
						DateTime start = new DateTime(bucket.getStart());
						if (!results.containsKey(start)) {
							results.put(start, new Integer(0));
						}
						results.put(start, results.get(start).intValue() + bucket.hits());
					}
//				}
			}
		}
		if (logRequest.isVerbose()) {
			for (DateTime t : results.keySet()) {
				LOGGER.info(DateUtil.shortTimeFormat.print(t) + " hits:" + results.get(t));
			}
		}
		return results;
	}
	private Map<String, Boolean> getReplayingQueryKeys(LogRequest logRequest) {
		HashMap<String, Boolean> result = new HashMap<String, Boolean>();
		List<Query> queries = logRequest.queries();
		for (Query query : queries) {
			result.put(query.key(), query.isReplay());
		}
		
		return result;
	}

    public List<Map<String, Bucket>> getHisto() {
        return histogram;
    }

    public void stop() {
        if (this.scheduler != null) scheduler.shutdownNow();
    }
}
