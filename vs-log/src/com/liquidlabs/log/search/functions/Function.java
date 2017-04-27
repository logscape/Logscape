/**
 * 
 */
package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Function extends FunctionFactory, Serializable{

     static final String TAG_SEPARATOR = "!";
	

	/**
	 * used for all processing
	 * @return
	 */
	List<Filter> filters();
	
	/**
	 * Return Field or GroupId
	 * @return
	 */
	String getApplyToField();
	String groupByGroup();
	String name();
	
	/**
	 * used to call up the function heirarchy
	 * @return
	 */
	List<Filter> allFilters();
	
	String group();
	boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber);
    void execute(String apply, String group);
    void calculate(String fieldValue);
	String getTag();
	
	@SuppressWarnings("unchecked")
	Map getResults();
	
	/**
	 * Used to agg from other buckets with same data (i.e. transfer between histos
	 * @param key
	 * @param otherFunctionMap
	 */
	void updateResult(String key, Map<String, Object> otherFunctionMap);
	void updateResult(String key, Set<String> otherFunctionMap);
	void updateResult(String key, Number value);
	
	/**
	 * Used to extract from raw function results to a client histogram 
	 * @param fieldSet TODO
	 * @param currentGroupKey TODO
	 * @param requestStartTimeMs TODO
	 */
	void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> otherFunctionMap, ValueSetter valueSetter, long bucketStart, long bucketEnd, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request);
	void extractResult(String key, Number object, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount, Map<String, Object> sourceDataForBucket, long start);
	void extractResult(String key, Set<String> otherFunctionMap, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData);
	/**
	 * Client top level post processing - i.e. shared between buckets
	 * @param querySourcePos
	 * @param sharedFunctionData
	 */
	void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData);
	
	/**
	 * Is for SingleBucket or across AllBuckets
	 * @return
	 */
	boolean isBucketLevel();
	
	String toStringId();

	void reset();

	// bulk operation where fieldN
	void increment(String fieldName, String fieldValue, int numberOfEvents);
}