package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.util.DateTimeExtractor;
import org.apache.log4j.Logger;

import java.util.*;

public class Elapsed extends FunctionBase implements Function, FunctionFactory {
    private static final String SUMMARY = "Summary";
	private String tag;
    private String field;
    private String start;
    private String end;
    private String labelGroup;
    private String displayUnit;
    private boolean lineChart;
    private int divisor = 1;
    private static final Logger LOGGER = Logger.getLogger(Elapsed.class);

    List<ElapsedInfo> elapsedTimes = new ArrayList<ElapsedInfo>();
    private int maxElapsed = LogProperties.getMaxElapsed();


    public Elapsed(String tag, String field, String start, String end, String displayUnit, String labelGroup, boolean lineChart) {
        super(Elapsed.class.getSimpleName());
        this.tag = tag;
        this.field = field;
        this.start = start;
        this.end = end;
        this.displayUnit = displayUnit;
		this.labelGroup = labelGroup;
        this.lineChart = lineChart;
        divisor(displayUnit);
    }

    private void divisor(String displayUnit) {
        if ("S".equalsIgnoreCase(displayUnit)) {
            this.divisor = 1000;
        } else if ("M".equalsIgnoreCase(displayUnit)) {
            this.divisor = 60000;
        } else if ("H".equalsIgnoreCase(displayUnit)) {
            this.divisor = 60 * 60000;
        } 
    }


    public String group() {
        return field;
    }
    
    transient DateTimeExtractor extractor;

    transient Map<String, ElapsedInfo> times = new HashMap<String, ElapsedInfo>();
    	
    synchronized public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
    	if (elapsedTimes().size() > maxElapsed) return false;
    	if (extractor == null) extractor = new DateTimeExtractor();
    	time = rawLineData != null ? extractor.getTimeWithFallback(rawLineData, time).getTime() : time;
        String fieldValue = fieldValue(fieldSet, fields, matchResult);
        
        if (fieldValue != null) {
        	
        	String labelValue = getLabelValue(labelGroup, matchResult, fields, fieldSet);
        	
            if (isStart(fieldValue)) {
            	// DETECT OVERLAP
            	getTimes().put(labelValue,  new ElapsedInfo(time, time, divisor, labelValue));
            	
            } else if (isEnd(fieldValue)) {
            	ElapsedInfo completeTask = getTimes().remove(labelValue);
            	if (completeTask != null) {
            		completeTask.setEndTime(time);
            		// only allow items with > 0 value
            		if (completeTask.getDuration() > 0.0) elapsedTimes().add(completeTask);
            	}
            }
            return true;
        } else {
        	return false;
        }
    }
    public void execute(String apply, String group) {
        // dunno!
    }

	final private Map<String, ElapsedInfo> getTimes() {
		if (times == null) times = new HashMap<String, ElapsedInfo>();
		return times;
	}


    private String getLabelValue(String labelGroup2, MatchResult matchResult, String[] fields, FieldSet fieldSet) {
    	if (labelGroup2 == null || labelGroup2.length() == 0) return "";
    	if (groupIsInt(labelGroup2)) {
    		try {
	    		if (matchResult != null && matchResult.isMatch()) {
	    			int groupId = Integer.parseInt(labelGroup2);
	    			return matchResult.getGroup(groupId);
	    		}
    		} catch (Throwable t) {
    			return "";
    		}
    	}
			return fieldSet.getFieldValue(labelGroup2, fields);
	}

	private String fieldValue(FieldSet fieldSet, String[] fields, MatchResult matchResult) {
        Integer group = getGroup();
        if (group != null) {
            return matchResult.getGroup(group);
        }
        return fieldSet.getFieldValue(field, fields);
    }

    private boolean isEnd(String fieldValue) {
        return fieldValue.contains(end);
    }

    private boolean isStart(String fieldValue) {
        return fieldValue.contains(start);
    }

    private List<ElapsedInfo> elapsedTimes() {
        if (elapsedTimes == null) {
            elapsedTimes = new ArrayList<ElapsedInfo>();
        }
        return elapsedTimes;
    }

    public Function create() {
        return new Elapsed(tag, field, start, end, displayUnit, labelGroup, lineChart);
    }

    public String getTag() {
        return tag;
    }

    public Map getResults() {
        Map results = new HashMap<String, List<ElapsedInfo>>();
        results.put("elapsed", elapsedTimes());
        return results;
    }

    public boolean isLine() {
        return lineChart;
    }

    public void updateResult(String key, Map<String, Object> map) {
        List<ElapsedInfo> results = (List<ElapsedInfo>) map.get("elapsed");
        if (results != elapsedTimes()) {
            if (results != null) {
                for (ElapsedInfo info : results) {
// kills performance - items should be unique anyways                	
                    if (!elapsedTimes().contains(info)) {
                        elapsedTimes().add(info);
                    }
                }
            }
        }
    }

    public synchronized void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> otherFunctionMap, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
        List<ElapsedInfo> tasks = (List<ElapsedInfo>) otherFunctionMap.get("elapsed");
//        if (tasks.size() > 100) {
//        	LOGGER.info("TaskSize:" + tasks.size());
//        	tasks = tasks.subList(0, 100);
//        }
        // we want consistent series results, sorted by label - if none - the do it by time
// sort is slow
        if (tasks.size() < 5000) {
	        Collections.sort(tasks, new Comparator<ElapsedInfo>(){
	
				@Override
				public int compare(ElapsedInfo o1, ElapsedInfo o2) {
					if (o1.getLabel() != null && o1.getLabel().length() > 0) {
						return o1.getLabel().compareTo(o2.getLabel());
					} 
					return Long.valueOf(o1.getStartTime()).compareTo(o2.getStartTime());
				}
	        });
        }

        int pos = 0;
        if (tag.contains(SUMMARY)) {
        	addTaskCountAtTimeValue(start, end, key, valueSetter, tasks);
        	addTaskElapsedCummulative(start, end, key, valueSetter, tasks);
        } else {
        	
	        for (ElapsedInfo task : tasks) {
	            if (lineChart) {
	                addPointsForLineChart(key, valueSetter, start, end, pos, task, sharedFunctionData);
	            } else {
	                addPointForElapsedOnly(key, valueSetter, start, end, pos, task, sharedFunctionData);
	            }
	            pos++;
	        }
        }
    }

    private void addTaskCountAtTimeValue(long start2, long end2, String key, ValueSetter valueSetter, List<ElapsedInfo> tasks) {
    	int taskCount = 0;
    	for (ElapsedInfo task : tasks) {
			if (task.isRunning(start2, end2)) taskCount++;
		}
    	valueSetter.set(tag + LogProperties.getFunctionSplit() + "Count", taskCount, false);		
	}
    private void addTaskElapsedCummulative(long start2, long end2, String key, ValueSetter valueSetter, List<ElapsedInfo> tasks) {
    	double taskElapsed = 0;
    	for (ElapsedInfo task : tasks) {
    		if (task.getEndTime() <= end2) taskElapsed += task.getDuration();
    	}
    	valueSetter.set(tag + LogProperties.getFunctionSplit() + "Total", taskElapsed, false);		
    }

	private void addPointForElapsedOnly(String key, ValueSetter valueSetter, long start, long end, int pos, ElapsedInfo result, Map<String, Object> sharedFunctionData) {
    	Set<ElapsedInfo> startResults = getStartResults(sharedFunctionData);
//    	LOGGER.info(format(this.hashCode() + " CHECK Elapsed ---- [%s] BStart %s, BEnd %s, RESULT %s  pos:%d", result.getLabel(), new DateTime(start).toString(), new DateTime(end).toString(), result.toString(), pos));

        if (isNonExtractedStartTime(start, end, result, startResults)) {
        	String tag = result.getLabel().length() > 0 ? key + LogProperties.getFunctionSplit() + result.getLabel() : key ;
            valueSetter.set(tag + LogProperties.getFunctionSplit() + pos, result.getDuration(), false);
//            LOGGER.debug(" CHECK Elapsed [" + tag + " STARTED:" + result.toString());
            startResults.add(result);
        }
    }
    
    private void addPointsForLineChart(String key, ValueSetter valueSetter, long start, long end, int pos, ElapsedInfo result, Map<String, Object> sharedFunctionData) {
    	String tag = key + LogProperties.getFunctionSplit() + result.getLabel() + LogProperties.getFunctionSplit() + pos;
    	Set<ElapsedInfo> startResults = getStartResults(sharedFunctionData);
    	Set<ElapsedInfo> endResults = getEndResults(sharedFunctionData);
    	
//    	LOGGER.info(format(" CHECK Elapsed ---- [%s] Bucket[%s %s][%s] %d %d", tag, new DateTime(start).toString(), new DateTime(end).toString(), result.toString(), pos, startResults.size()));
        if (isNonExtractedStartTime(start, end, result, startResults)) {
            valueSetter.set(tag, result.getDuration(), false);
//            LOGGER.info(" CHECK Elapsed START " + tag);
            startResults.add(result);
        } 
        	
        if (isNonExtractedEndTime(start, end, result, endResults)) {
            valueSetter.set(tag, result.getDuration(), false);
//            LOGGER.info(" CHECK Elapsed END  " + tag);
            endResults.add(result);
            
            // if we also put the start && END into this bucket - then we need to put into the next bucket so the line gets drawn
            if (result.getStartTime() >= start && result.getStartTime() < end) {
            	sharedFunctionData.put("PUT_NEXT" + tag, result);
            }
        }
        // where start and end times are in the same bucket - we also want to put in the next bucket so the line is drawn
        if (result.getEndTime() < start && sharedFunctionData.containsKey("PUT_NEXT" + tag)) {
        	sharedFunctionData.remove("PUT_NEXT" + tag);
        	valueSetter.set(tag, result.getDuration(), false);
//        	LOGGER.info(" CHECK Elapsed APPEND " + tag);
        }
    }


	private Set<ElapsedInfo> getStartResults(Map<String, Object> sharedFunctionData) {
		Set<ElapsedInfo> results = (Set<ElapsedInfo>) sharedFunctionData.get("elapsed-start");
    	if (results == null) {
    		results = new HashSet<ElapsedInfo>();
    		sharedFunctionData.put("elapsed-start", results);
    	}
		return results;
	}
	private Set<ElapsedInfo> getEndResults(Map<String, Object> sharedFunctionData) {
		Set<ElapsedInfo> results = (Set<ElapsedInfo>) sharedFunctionData.get("elapsed-end");
		if (results == null) {
			results = new HashSet<ElapsedInfo>();
			sharedFunctionData.put("elapsed-end", results);
		}
		return results;
	}


    public boolean isBucketLevel() {
        return false;
    }

    @Override
    public String toString() {
    	return toStringId();
    }

    public Integer getGroup() {
        try {
            return Integer.valueOf(field.trim());
        } catch (Exception e) {
            return null;
        }
    }


    private boolean isNonExtractedEndTime(long start, long end, ElapsedInfo result, Set<ElapsedInfo> endResults) {
        return result.getEndTime() >= start && result.getEndTime() < end && !endResults.contains(result);
    }

    private boolean isNonExtractedStartTime(long start, long end, ElapsedInfo result, Set<ElapsedInfo> startResults) {
        return result.getStartTime() >= start && result.getStartTime() < end && !startResults.contains(result);
    }

    public void updateResult(String key, Set<String> otherFunctionMap) {
    }

    public void updateResult(String key, Number value) {
    }

    public void extractResult(String key, Number object, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount, Map<String, Object> sourceDataForBucket, long start) {
    }

    public void extractResult(String key, Set<String> otherFunctionMap, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData) {
    }

    public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
    }

    public Elapsed() {
    }

	public String getLabelGroup() {
		return this.labelGroup;
	}
	public String getApplyToField() {
		return field;
	}
}
