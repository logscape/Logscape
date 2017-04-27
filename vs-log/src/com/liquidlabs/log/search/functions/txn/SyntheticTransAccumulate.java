package com.liquidlabs.log.search.functions.txn;

import com.carrotsearch.hppc.BoundedProportionalArraySizingStrategy;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.functions.FunctionBase;
import com.liquidlabs.log.search.functions.FunctionFactory;
import com.liquidlabs.log.search.functions.ValueSetter;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.util.DateTimeExtractor;
import javolution.util.FastMap;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;

/**
 *
 * single function accumulates txns times
 * Note:
 * isBucketLevel == false - so the 1 instance of txn is run across all buckets, files etc - which means concurrency needs to be supported
 *
 * Ideas:
 * TxnsPerSecond
 * RunningTxns
 * AvgTxn with Parts
 * Min/max Txn overlaid on Avg
 * Min/max/avg number of steps
 * BaseLine hardcoded txn values
 * SLA max hardcoded - show msg when breached (can pick it up in alert)
 * SLA Breach - ??
 *
 * Efficiency:
 * - Use Int Values on Times - to elapsedMs from Search StartTime Ms, Max Int allows for X millis...gives up to 35791 mins - a long txn!
 * -  TODO: Use BucketLevel so more data can be sent - i.e. smaller chunks, better on copy-based serialization - but need to use final stage to grab value setters like AvgDelta does
 *
 */
public class SyntheticTransAccumulate extends FunctionBase implements Function, FunctionFactory, KryoSerializable  {

    private static final Logger LOGGER = Logger.getLogger(SyntheticTransAccumulate.class);
    private static final long serialVersionUID = 1L;
    public static final boolean isPrintingTotal = Boolean.getBoolean("txn.print.total");


    // turn ms to ms-1 decimal place... i.e. 1000
    static int DIVISOR = 10;

    // each txn is accumulated
    // the number is NOT MS from startTime or Seconds from startTime
    // BUT Ms/10 from startTime. So everytime that is returned
    // needs to use the DIVISOR
    private transient Map<String, IntArrayList> groups;


    private String tag;
    private String field;
    private int groupId = -1;
    // startTag, endTag
    private String[] instruction;

    transient int defArraySize = 3;
    transient boolean pruneDebug = Boolean.getBoolean("txn.prune.debug");



    private static int maxStepsSize = Integer.getInteger("txn.max.steps", 25);
    private static int maxTxnsToCollect = Integer.getInteger("txn.max.collect.count", 10000);
    private static int maxTxnsForPrune = Integer.getInteger("txn.max.prune.at", 500);
    private static double pruneTimeMultiplier = Double.parseDouble(System.getProperty("txn.prune.time", "0.5"));
    private static boolean autoPrune = Boolean.parseBoolean(System.getProperty("txn.auto.prune", "true"));

    public SyntheticTransAccumulate(){
    }


    public SyntheticTransAccumulate(String tag, String field, String... instruction) {
        this.tag = tag;
        this.field = field;
        try {
            groupId = Integer.parseInt(field);
        } catch (Throwable t) {
        }

        this.instruction = instruction;
    }

    public Function create() {
        return new SyntheticTransAccumulate(tag, field, instruction);
    }

    public boolean isBucketLevel() {
        return false;
    }

    transient volatile long slowestTime = 0;
    public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
        long timeCheck = getExtractor().getTimeWithFallback(rawLineData, time).getTime();
        // sanity check
        if (timeCheck >= time && timeCheck <= time + DateUtil.MINUTE * 1) {
            time = timeCheck;
        }
        String txnTag = null;

        // Grab the txn-signature
        if (groupId == -1) txnTag = fieldSet.getFieldValue(field, fields);
        else txnTag = matchResult.getGroup(groupId);

        if (txnTag == null) return false;

        int size = getGroups().size();

        // we hit the upper limit
        if (size > maxTxnsToCollect) {
            return false;
        }
        // Accumulate
        IntArrayList timesForTxn = getTimesForTxn(txnTag);
        synchronized (timesForTxn) {
            // if too many points drop the last item so it can be replaced -- OR -- if too many txns only grab the start-end
            if (timesForTxn.size() > maxStepsSize || (autoPrune && groups.size() > maxTxnsForPrune && timesForTxn.size() > 2)) {
                timesForTxn.remove(timesForTxn.size()-1);
            }
            int intValue = Long.valueOf( (time- requestStartTimeMs) / DIVISOR).intValue();
            if (!timesForTxn.contains(intValue)) {
                timesForTxn.add(intValue);
            }


            // always try to update with the slowest txn
            if (autoPrune && timesForTxn.size() >= 2) {
                synchronized (this) {
                    long elapsed = timesForTxn.get(timesForTxn.size()-1) - timesForTxn.get(0);
                    if (elapsed > slowestTime) {
                        slowestTime = elapsed;
                    }
                    // Only collect slowest
                    if (size > maxTxnsForPrune) {
                        // throw away anything that is too fast - i.e. less than slowestTime * .5
                        long timePrune = (long)(slowestTime * pruneTimeMultiplier);
                        if (elapsed < timePrune) {
                            if (pruneDebug) LOGGER.info("PruneTx:" + txnTag);
                            getGroups().remove(txnTag);
                        }
                    }
                }
            }
        }
        return true;
    }

    synchronized private IntArrayList getTimesForTxn(String groupBy) {
        IntArrayList list = getGroups().get(groupBy);
        if (list == null) {
            list = new IntArrayList(defArraySize, new BoundedProportionalArraySizingStrategy(defArraySize,defArraySize,1.5f));
            getGroups().put(groupBy, list);
        }
        return list;
    }

    public String getTag() {
        return tag;
    }


    @SuppressWarnings("unchecked")
    public Map getResults() {
        Map<String, IntArrayList> functionResults = new HashMap<String, IntArrayList>();
        for (Entry<String, IntArrayList> entry : getGroups().entrySet()) {
            IntArrayList value = entry.getValue();
            IntArrayList copy = new IntArrayList(value.size(), new BoundedProportionalArraySizingStrategy(defArraySize,defArraySize,1.5f));
            copy.addAll(value);
            if (functionResults.size() < maxTxnsToCollect) functionResults.put(entry.getKey(),copy);
        }
        if (pruneDebug) LOGGER.info("GetResults - TxnCount:" + functionResults.size());
        return functionResults;
    }

    /**
     * Occurs on AggSpace and on final client histo assembly
     */
    @SuppressWarnings("unchecked")
//		ObjectOpenHashSet<Object> handled;
            Object lastHObject = null;
    synchronized public void updateResult(String itemKey, Map<String, Object> otherFunctionMap) {
//			if (handled == null) handled = new ObjectOpenHashSet<Object>();
        synthFunc = null;
        if (lastHObject != null && lastHObject == otherFunctionMap) return;
        lastHObject = otherFunctionMap;
        // need to reset this otherwise it will only fire once
        cPos = -1;

        synchronized (otherFunctionMap) {
            long[] fastestSlowest = getFastestAndSlowest(getGroups());
            long elapsedThreshold = (long) (fastestSlowest[1] * pruneTimeMultiplier);
            for (String mapKey : otherFunctionMap.keySet()) {
                if (getGroups().size() >= maxTxnsToCollect) {
                    IntArrayList otherValue = (IntArrayList) otherFunctionMap.get(mapKey);
                    IntArrayList otherTxnParts = getGroups().get(mapKey);
                    if (otherTxnParts != null) {
                        // append the times
                        append(otherTxnParts, otherValue);
                    } else {
                        int txnSize = otherValue.size();
                        if (txnSize >= 3){
                            // if its slow enough and we have enough points then add it - otherwise ignore
                            int txElapsed = otherValue.get(txnSize-1) - otherValue.get(0);
                            if (txElapsed >= elapsedThreshold) {
                                getGroups().put(mapKey, otherValue);
                            }
                        } else {
                            // not enough points to make sense of it - so add it
                            getGroups().put(mapKey, otherValue);
                        }
                    }
                } else {
                    IntArrayList otherValue = (IntArrayList) otherFunctionMap.get(mapKey);
                    IntArrayList timesForTxn = getTimesForTxn(mapKey);
                    if (otherValue == timesForTxn) continue;

                    // For the fastest iteration, you can access the lists' data buffer directly.
                    final int [] buffer = otherValue.buffer;
                    // Make sure you take the list.size() and not the length of the data buffer.
                    final int size = otherValue.size();
                    for (int i = 0; i < size; i++) {
                        int val = buffer[i];
                        if (!timesForTxn.contains(val)) timesForTxn.add(val);
                    }
                }
            }
        }
        // post prune to keep the slowest txns
        while (getGroups().size() > maxTxnsToCollect) {
            removeFastest(getGroups(), otherFunctionMap);
        }
    }

    private void removeFastest(Map<String, IntArrayList> groups2, Map<String, Object> otherFunctionMap) {
        String fastedId = groups2.keySet().iterator().next();
        IntArrayList txnTimes = groups2.get(fastedId);
        int lastIndex = txnTimes.size()-1;
        int time = txnTimes.get(lastIndex) - txnTimes.get(0);
        for (String txnKey : groups2.keySet()) {
            if (txnTimes.size() >= 2) {
                txnTimes = groups2.get(txnKey);
                int txTime = txnTimes.get(lastIndex) - txnTimes.get(0);
                if (txTime > 0 && txTime < time) {
                    time = txTime;
                    fastedId = txnKey;
                }
            }
        }
        //System.out.println("Remove:" + fastedId + " time:" + time);
        groups2.remove(fastedId);
        otherFunctionMap.remove(fastedId);
    }


    private void append(IntArrayList mainTxn, IntArrayList otherValue) {
        int[] buffer = otherValue.buffer;
        int size = otherValue.size();
        mainTxn.ensureCapacity(mainTxn.size() + size);
        for (int i = 0; i < size; i++) {
            if (!mainTxn.contains(buffer[i])) mainTxn.add(buffer[i]);
        }
    }


    private long[] getFastestAndSlowest(Map<String, IntArrayList> groups) {
        long fastest = Long.MAX_VALUE;
        long slowest = 0;
        for (IntArrayList txnTimes : groups.values()) {
            long elapsed = getTime(txnTimes.buffer, txnTimes.size());
            if (elapsed > 0) {
                if (elapsed > slowest) slowest = elapsed;
                else if (elapsed < fastest) fastest = elapsed;
            }
        }
        return new long[] { fastest, slowest };
    }


    private long getTime(int[] buffer, int size) {
        if (size <= 1) return -1;
        return buffer[size-1] - buffer[0];
    }


    public void reset() {
//      bucket flush calls down to this while pumping results. if we clear it out it gets broken
//      so leave it in
//        groups = null;
//        getGroups();
    }

    public void updateResult(String groupBy, Number value) {
        new RuntimeException("NotImplemented: void updateResult(String groupBy, Number value)");
    }

    // used to repeated agg and update the valueSetter
    int cPos = -1;
    int lastPos = -1;
    synchronized public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> otherFunctionMap, ValueSetter valueSetter, long bucketStart, long bucketEnd, int currentBucketPos, int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {

        try {
            if (currentBucketPos == 0) {
                if (lastPos != 0) {
                    sortGroups();
                }
                lastPos = currentBucketPos;
            }
            // only process each position once
            if (cPos == currentBucketPos) return;


            // if this a first run through
            if (currentBucketPos == 0 || currentBucketPos < lastPos || lastPos == -1) {

                groups = splitLongTxns(groups);


            }
            cPos = currentBucketPos;

            if (pruneDebug) LOGGER.info("ExtractResult - TxnCount:" + this.hashCode() +" " + currentBucketPos + " txnSize:" + otherFunctionMap.size());
//				System.out.println(this.hashCode() + " ExtractResult - TxnCount: b" + currentBucketPos + " txnSize:" + " g:" + groups.size() + " o:" +  otherFunctionMap.size());

//				System.out.println(new DateTime() + " - " + Thread.currentThread().getName() + " SynthTxnAcc.extractResult() GOING TO EXTRACT:" + currentBucketPos);

            // get the synth function and apply it for this bucket....
            List<SynthFunction> funcs = getSynthFunction(bucketStart, bucketEnd, currentBucketPos, totalBucketCount);
            for (SynthFunction func : funcs) {
                Query query = request != null ? request.query(querySourcePos) : null;
                func.apply(currentBucketPos, groups, valueSetter, bucketStart, bucketEnd, requestStartTimeMs, query);
            }

            if (currentBucketPos == 0 && isPrintingTotal) {
                valueSetter.set("TxnCount-" + groups.size(), groups.size(), false);
            }
            lastPos = currentBucketPos;
        } catch (Throwable t) {
            LOGGER.warn("ExtractFailed:", t);
        }
    }

    private void sortGroups() {
        Set<String> keySet = this.groups.keySet();
        for (String key : keySet) {
            // puke - copyOnWAL doesnt allow a sort...
            IntArrayList list = groups.get(key);
            //Collections.sort(list);
            int[] indices = IndirectSort.mergesort(0, list.size(), new IndirectComparator.AscendingIntComparator(list.buffer));
            if (!isSorted(indices)) {
                IntArrayList sorted = new IntArrayList(indices.length, new BoundedProportionalArraySizingStrategy(defArraySize, defArraySize,1.5f));
                for (int i = 0; i < indices.length; i++) {
                    sorted.add(list.buffer[indices[i]]);
                }
                this.groups.put(key, sorted);
            }
            // cant do anything with 1 point
            if (list.size() <= 1) this.groups.remove(key);
        }
    }

    private boolean isSorted(int[] indices) {
        if (indices.length <= 1) return true;
        for (int i = 0; i < indices.length; i++) {
            if (i > 0 && indices[i] < indices[i-1]) return false;
        }
        return true;
    }

    transient List<SynthFunction> synthFunc = null;
    private List<SynthFunction> getSynthFunction(long bucketStart, long bucketEnd, int currentBucketPos, int totalBucketCount) {
        if (synthFunc != null) return synthFunc;
        else {
            if (tag.equals("perf")) {
                addPerfFuncs(totalBucketCount);
            } else if (tag.equals("elapsed")) {
                addElapsedFuncs(totalBucketCount);
            } else if (tag.equals("rate") || tag.equals("volume")) {
                addRateFuncs(totalBucketCount);
            } else if (tag.equals("rate-perf") || tag.equals("profile")) {
                addRatePerfFuncs(totalBucketCount);
            } else if (tag.equals("table")) {
                addTableFuncs(totalBucketCount);
            } else {
                addPerfFuncs(totalBucketCount);
            }
        }
        return synthFunc;
    }


    private void addTableFuncs(int totalBucketCount) {
        synthFunc = new ArrayList<SynthFunction>();
        synthFunc.add(new Table(totalBucketCount));
    }


    private void addRateFuncs(int totalBucketCount) {
        synthFunc = new ArrayList<SynthFunction>();
        synthFunc.add(new Rate(totalBucketCount));
    }
    private void addRatePerfFuncs(int totalBucketCount) {
        synthFunc = new ArrayList<SynthFunction>();
        synthFunc.add(new RatePerf(totalBucketCount));
    }



    private void addElapsedFuncs(int totalBucketCount) {
        synthFunc = new ArrayList<SynthFunction>();
        synthFunc.add(new Elapsed(totalBucketCount));
    }


    private void addPerfFuncs(int totalBucketCount) {
        synthFunc = new ArrayList<SynthFunction>();
        synthFunc.add(new Avg(totalBucketCount));
        synthFunc.add(new Slowest(totalBucketCount));
        synthFunc.add(new Fastest(totalBucketCount));
        // TODO - functions to show
        // most steps
        // least steps
        // total points
    }

    public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
    }

    long getElapsed(List<Long> list) {
        return list.get(list.size()-1) - list.get(0);
    }


    synchronized private Map<String, IntArrayList> getGroups() {
        if (this.groups == null) {
            this.groups = new FastMap<String, IntArrayList>();
            ((FastMap) this.groups).shared();
        }
        return groups;
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

    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, tag);
        kryo.writeObject(output, field);
        kryo.writeObject(output, groupId);
        kryo.writeObject(output, instruction);
    }
    public void read(Kryo kryo, Input input) {
        tag = kryo.readObject(input, String.class);
        field = kryo.readObject(input, String.class);
        groupId = kryo.readObject(input, Integer.class);
        instruction = kryo.readObject(input, String[].class);
    }

    int SPLIT_FACTOR = Integer.getInteger("txn.split.factor", 5);
    int GAP_FACTOR = Integer.getInteger("txn.gap.factor", 3);

    Map<String, IntArrayList> splitLongTxns(Map<String, IntArrayList> groups) {
        // get the avg
        long total = 0;
        Map<String, Long> times = new HashMap<String, Long>();
        TreeMap<Long, String> sortedTimes = new TreeMap<Long, String>();

        for (Entry<String, IntArrayList> txn : groups.entrySet()) {
            long min = Integer.MAX_VALUE;
            long max = 0;
            IntArrayList list = txn.getValue();
            Iterator<IntCursor> iterator = list.iterator();
            while (iterator.hasNext()) {
                int value = iterator.next().value;
                if (value < min) min = value;
                if (value > max) max = value;
            }
            total += max -  min;
            times.put(txn.getKey(), (long) (max  - min));
            sortedTimes.put((max  - min), txn.getKey());
        }
        long avg = total / groups.size();

        // get the list of txns to split

        Set<String> splitThese = new HashSet<String>();
        long splitAtTime = avg * SPLIT_FACTOR;

        for (Entry<Long, String> s : sortedTimes.entrySet()) {
            if (s.getKey() > splitAtTime) splitThese.add(s.getValue());
        }

        // split each txn into a set of parts
        Map<String, IntArrayList>  results = new HashMap<String, IntArrayList>(groups);
        for (String s : splitThese) {
            IntArrayList remove = results.remove(s);
            List<Integer> sorted = new ArrayList<Integer>();
            // now break it into mulitple TXNs
            for (IntCursor intCursor : remove) {
                sorted.add(intCursor.value);
            }
            Collections.sort(sorted);

            // now scan it an look for gaps
            IntArrayList txnSet = new IntArrayList();
            int found = 0;
            for (Integer time : sorted) {
                if (txnSet.size() == 0) {
                    txnSet.add(time);
                } else {
                    // is there a gap?
                    if (txnSet.size() > 1 && time - txnSet.get(txnSet.size()-1) > (avg * SPLIT_FACTOR)/GAP_FACTOR) {
                        // we have a gap so build the next one
                        results.put(s + "-" + found++, txnSet);
                        txnSet = new IntArrayList();
                    }
                    txnSet.add(time);
                }
            }
            if (txnSet.size() > 0) {
                results.put(s + "-" + found++, txnSet);
            }

        }
        return results;





    }

}
