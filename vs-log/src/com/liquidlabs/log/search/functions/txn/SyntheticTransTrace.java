package com.liquidlabs.log.search.functions.txn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.liquidlabs.log.space.LogRequest;
import javolution.util.FastMap;

import org.apache.log4j.Logger;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.functions.FunctionBase;
import com.liquidlabs.log.search.functions.FunctionFactory;
import com.liquidlabs.log.search.functions.ValueSetter;
import com.liquidlabs.log.util.DateTimeExtractor;

/**
 * 
 * single function traces a single txn matching part of the tag
 * 1) - the tag needs a trace field, and a match rule for the UID,
 *    -  i.e. uid.txnTrace(thread,uid-value)
 * 
 */
public class SyntheticTransTrace extends FunctionBase implements Function, FunctionFactory  {

		private static final long serialVersionUID = 1L;
		private static final Logger LOGGER = Logger.getLogger(SyntheticTransTrace.class);
		
		// each txn-id is accumulated with the txn-line, and the other list if the other lines
		private transient Map<String, TxnTrace> trace;
		private transient Map<String, TxnTrace> liveTracers;
		
		private String tag = "trace";
		private String field;
		private int groupId = -1;
		
		// trace-filter.contains(), traceField - i.e. thread
		private String[] instruction;

		private String txnFilter;

		private String traceField;

		public SyntheticTransTrace(){
		}
		

		public SyntheticTransTrace(String field, String traceField, String txnFilter, String... instruction) {
			this.field = field;
			try {
				groupId = Integer.parseInt(field);
			} catch (Throwable t) {
			}
			
			this.instruction = instruction;
			this.traceField = traceField;
			this.txnFilter = txnFilter;
		}

		public Function create() {
			return new SyntheticTransTrace(field, traceField, txnFilter, instruction);
		}

		public boolean isBucketLevel() {
			return false;
		}
		
		public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
			time = getExtractor().getTimeWithFallback(rawLineData, time).getTime();
			String txnTag = null;
			
			// Grab the txn-signature
			if (groupId == -1) txnTag = fieldSet.getFieldValue(field, fields);
			else txnTag = matchResult.getGroup(groupId);
			
			// if no txn tag - then look for the trace field exists and matches
			String thisTraceFieldValue = fieldSet.getFieldValue(traceField, fields);
			String hostname = fieldSet.getFieldValue(FieldSet.DEF_FIELDS._host.name(), fields);
			
			return traceIt(rawLineData, txnTag, thisTraceFieldValue, hostname);
		}


		/**
		 * Collector Side
		 * @param rawLineData
		 * @param txnTag
		 * @param thisTraceFieldValue
		 * @param hostname
		 */
		boolean traceIt(String rawLineData, String txnTag, String thisTraceFieldValue, String hostname) {
			if (txnTag == null) {
				if (thisTraceFieldValue == null) {
					rawLineData = null;
					return false;
				}
				// add this line to the trace item.
				if (getLiveTracer().containsKey(thisTraceFieldValue)) {
					TxnTrace txnTrace = getLiveTracer().get(thisTraceFieldValue);
					txnTrace.addTrace(txnTrace.lastTxnLine(), rawLineData);
				}
			} else {
				getLiveTracer().remove(thisTraceFieldValue);
				if (!txnTag.contains(txnFilter)) {
					return false;
				}
			
				Map<String, TxnTrace> trace = getTrace();
				if (!trace.containsKey(txnTag)) trace.put(txnTag, new TxnTrace(txnTag));
				TxnTrace txnTrace = trace.get(txnTag);
				txnTrace.addTxn(rawLineData);
				getLiveTracer().put(thisTraceFieldValue, txnTrace);
			}
			return true;
		}

		private Map<String, TxnTrace> getLiveTracer() {
			if (liveTracers == null) liveTracers = new ConcurrentHashMap<String, TxnTrace>();
			return liveTracers;
		}


		private Map<String, TxnTrace> getTrace() {
			if (trace == null) {
				trace = new FastMap<String, TxnTrace>();
				((FastMap<String, TxnTrace>) trace).shared();
			}
			return trace;
		}

		public String getTag() {
			return tag;
		}

		
		@SuppressWarnings("unchecked")
		public Map getResults() {
			Map<String, TxnTrace> trace2 = getTrace();
			HashMap<String, TxnTrace> results = new HashMap<String, TxnTrace>();
			results.putAll(trace2);
			return results;
		}
		
		/**
		 * Aggregate/Rollup
		 */
		@SuppressWarnings("unchecked")
		Set<Object> handled;
		public void updateResult(String itemKey, Map<String, Object> otherTraceMap) {
			if (handled == null) handled = new HashSet<Object>();
			if (handled.contains(otherTraceMap)) return;
			handled.add(otherTraceMap);
			// need to reset this otherwise it will only fire once
			cPos = -1;
			
			synchronized (otherTraceMap) {
				trace = getTrace();
				// there should only be one of these
				Set<String> txnTags = this.trace.keySet();
				if (this.trace.isEmpty()) {
//					System.out.println("Populating txn trace..");
					Map oo = otherTraceMap;
					this.trace.putAll(oo);
				} else {
					for (String txnTag : txnTags) {
						TxnTrace txnTrace = this.trace.get(txnTag);
						TxnTrace otherTrace = (TxnTrace) otherTraceMap.get(txnTag);
						if (otherTrace == null) {
							LOGGER.warn("------- ooops trace");
							continue;
						} else {
							LOGGER.warn("Adding other trace:" + otherTrace);
						}
						txnTrace.addTrace(otherTrace);
					}
				}
			}
		}
		public void reset() {
			getTrace().clear();
		}

		public void updateResult(String groupBy, Number value) {
			new RuntimeException("NotImplemented: void updateResult(String groupBy, Number value)");
		}

		// used to repeated agg and update the valueSetter
		int cPos = -1;
		public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> otherFunctionMap, ValueSetter valueSetter, long bucketStart, long bucketEnd, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {
			
			if (currentBucketPos == 0) {
				sortGroups();
			}
			// only process each position once
			if (cPos == currentBucketPos) return;
			cPos = currentBucketPos;
			SynthTraceFunction func = null;
			// if in singleBucketmode generate something for the table..
			if (totalBucketCount == 1) {
				func = getSingleBucketTrace();
				func.apply(this.trace, valueSetter);				
			} else {
				// could show line chart with the txn details
			}

			
		}

		private SynthTraceFunction getSingleBucketTrace() {
			return new SynthTracer();
		}


		private void sortGroups() {
		}

		public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
		}
		
		long getElapsed(List<Long> list) {
			return list.get(list.size()-1) - list.get(0);
		}
		
		
		public String group() {
			return field;
		}
		
		public void extractResult(String key, Number value, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount, Map<String, Object> sourceDataForBucket, long start) {
			throw new RuntimeException("NotImplemented");
		}
		public List<Filter> filters() {
			return null;
		}
		public List<Filter> allFilters() {
			return null;
		}

		public void updateResult(String key, Set<String> otherFunctionMap) {
		}

		public void extractResult(String key, Set<String> otherFunctionMap, ValueSetter valueSetter, long start, long end, int currentBucketPos, int querySourcePos, int totalBucketCount,
			Map<String, Object> sharedFunctionData) {
			throw new RuntimeException("NotImplemented");
		}
		public String getApplyToField() {
			return field ;
		}
		public String groupByGroup() {
			return "";
		}
		
		transient DateTimeExtractor extractor;
		private DateTimeExtractor getExtractor() {
			if (extractor == null) extractor = new DateTimeExtractor();
			return extractor;
		}
		public String toStringId() {
			return String.format("%s %s %s", getClass().getSimpleName(), tag, field);
		}
}
