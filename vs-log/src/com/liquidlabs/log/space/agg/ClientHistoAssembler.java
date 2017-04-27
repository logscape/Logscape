package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.fields.field.LiteralField;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.functions.FunctionBase;
import com.liquidlabs.log.search.functions.GlobalFunction;
import com.liquidlabs.log.search.functions.ValueSetter;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

public class ClientHistoAssembler {

    private static final String GC_OVERHEAD = "GC overhead";

    DateTimeFormatter formatter = DateUtil.shortDateTimeFormat3;
    private final static Logger LOGGER = Logger.getLogger(ClientHistoAssembler.class);
    HistoManager bucketHistoAssembler = new HistoManager();

    public List<ClientHistoItem> newClientHistogram(List<Map<String, Bucket>> aggdHisto) {
        ArrayList<ClientHistoItem> histogram = new ArrayList<ClientHistoItem>();

        int pos = 0;
        for (Map<String,Bucket> map : aggdHisto) {
            Bucket bucket = map.values().iterator().next();
            ClientHistoItem histogramItem = new ClientHistoItem();
            if (pos++ == 0) histogramItem.meta = new Meta();
//			DateTime dateTime = new DateTime(timeMs);
            // round everything to the nearest minute
            histogramItem.setTimeValue(bucket.getStart());
            histogram.add(histogramItem);
        }

        return histogram;
    }
    public List<ClientHistoItem> newClientHistogram(DateTime fromTime, DateTime toTime, int bucketCount) {
        ArrayList<ClientHistoItem> histogram = new ArrayList<ClientHistoItem>();

        DateTimeFormatter formatter = this.formatter;

        // if > 24 hours use date and time formatting
        if ((toTime.getMillis() - fromTime.getMillis()) >= 24 * 60 * 60 * 1000) {
            formatter = DateTimeFormat.shortDateTime();
        }
        long timeWidth = (toTime.getMillis() - fromTime.getMillis()) / bucketCount;
        for (long timeMs = fromTime.getMillis(); timeMs < toTime.getMillis(); timeMs += timeWidth) {
            ClientHistoItem histogramItem = new ClientHistoItem();
            DateTime dateTime = new DateTime(timeMs);
            // round everything to the nearest minute
            dateTime = new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(), dateTime.getHourOfDay(),
                    dateTime.getMinuteOfHour(), 0, 0);
            histogramItem.setTimeValue(dateTime.getMillis());
            histogram.add(histogramItem);
        }
        return histogram;
    }

    public ClientHistoItem convertToClientHistoItem(final ClientHistoItem clientHistoItem, Bucket bucket, int bucketNumber,  Boolean isGroupBy, LogRequest request, int currentBucket, Map<String, Object> sharedFunctionData, int totalBucketCount, Map<Integer,Set<String>> functionTags, List<FieldSet> fieldSets) {
        try {

            if (isGroupBy) {
                if (request.isVerbose())
                    LOGGER.info(String.format("LOGGER s:%d Group Source[%s] MatchIndex[%s] Pattern[%s] Hits[%d]",
                            currentBucket, bucket.getSourceURI(), bucket.getQueryPos(), bucket.getPattern(), bucket.hits()));
                Set<FieldSet> fieldSet = getFieldSet(bucket, fieldSets);
                pushIntoClientHistoItem(clientHistoItem, bucket, request.getBucketCount(), bucket.functions(), request.isVerbose(),
                        request.queries().get(bucket.getQueryPos()).getSourcePos(),
                        currentBucket, sharedFunctionData, totalBucketCount, request.queries().get(bucket.getQueryPos()).filters(), functionTags, request.subscriber(), fieldSet, request.getStartTimeMs(), request);
            } else {
                if (request.isVerbose())
                    LOGGER.info(String.format("LOGGER s:%d Basic Source[%s] MatchIndex[%s] Pattern[%s] Hits[%d]",
                            currentBucket, bucket.getSourceURI(), bucket.getQueryPos(), bucket.getPattern(), bucket.hits()));
                putIntoHistogram(clientHistoItem, bucket, new DateTime(bucket.getStart()), new DateTime(bucket.getEnd()), request.getBucketCount(), request.queries().get(bucket.getQueryPos()).filters());
            }
            return clientHistoItem;
        } catch (Throwable t) {
            t.printStackTrace();
            if (t != null && t.getMessage() != null && t.getMessage().contains(GC_OVERHEAD)) {
                throw new RuntimeException(GC_OVERHEAD);
            }
            t.printStackTrace();
            LOGGER.warn("Failed to handle ClientHisto:" + t.getMessage(), t);
        }
        return clientHistoItem;
    }
    private Set<FieldSet> getFieldSet(Bucket bucket, List<FieldSet> fieldSets) {
        Set<FieldSet> sets = new HashSet<FieldSet>();
        for (FieldSet fieldSet : fieldSets) {
            if (bucket.getFieldSetId().contains(fieldSet.getId())) sets.add(fieldSet);
        }
        return sets;
    }

    @SuppressWarnings("unchecked")
    public void pushIntoClientHistoItem(final ClientHistoItem clientHistoDest, final Bucket aggBucketSrc, int bucketCount,
                                        List<Function> functions, final boolean verbose, final int querySourcePos, int bucketPos,
                                        Map<String, Object> sharedFunctionData, int totalBucketCount, List<Filter> filters, Map<Integer, Set<String>> functionTags, String subscriber, final Set<FieldSet> fieldSet, long requestStartTimeMs, final LogRequest request) {

        int groupId = 0;

        functions = FunctionBase.sortPostAggFunctions(functions);

        // TODO Refactor out common chunks of code - puke!

        final List<GlobalFunction> globalFunctions = FunctionBase.getGlobalFunctions(functions);
        final Map<String, Object> globalValues = new HashMap<>();

        final Map<String,Map<String,Object>> transformGroupThenFieldValueMap = new HashMap<String,Map<String,Object>>();

        for (final Function function : functions) {
            final List<Filter> postAggFunctionFilters = getPostAggFiltersForFunctionTag(function.getTag(), filters);
            final List<Function> postAggFunctions = FunctionBase.getPostAggFunctions(function, functions);

            if (!functionTags.containsKey(querySourcePos)) functionTags.put(querySourcePos, new HashSet<String>());
            functionTags.get(querySourcePos).add(function.getTag());

            Map<String, Object> groups = aggBucketSrc.getAggregateResult(function.toStringId(), verbose);
            if (groups == null){
                // may have been collected in a post-agg function
                groups = function.getResults();
            }
            if (groups == null) {
                LOGGER.warn("LOGGER GOT NULL group for:" + function.getTag() + " functionResults:" + aggBucketSrc.getAggregateResults()
                        + " histoSrc:" + aggBucketSrc.getSourceURI() + " bucketFromTime:" + formatter.print(aggBucketSrc.getStart()) + " POS:" + aggBucketSrc.getQueryPos() + " PAT:" + aggBucketSrc.getPattern());
                continue;
            }
            final String fieldName = function.getApplyToField();
            // could be that the fieldSet is not the right one in which the getField() will return null...
            FieldI afield = getFieldForBucket(fieldSet, fieldName);
            final FieldSet foundFieldSet =getFieldSetForBucket(fieldSet, fieldName);
            final FieldI field = afield != null ? afield : new LiteralField(fieldName, 1, true, true, "",null);
            for (final String groupName : groups.keySet()) {
                if (!transformGroupThenFieldValueMap.containsKey(groupName)) transformGroupThenFieldValueMap.put(groupName, new HashMap<String,Object>());
                final Map<String, Object> transformFieldValuesForGroupMap = transformGroupThenFieldValueMap.get(groupName);

                final int aGroupId = groupId;
                final boolean isFieldTransformable = field.description() != null && field.description().contains("transform");

                final String key = groupName;
                Object object = groups.get(groupName);
                if (object instanceof Double) {
                    function.extractResult(key, (Double) object, new ValueSetter() {
                        public void set(String group, Number value, boolean useThisResultLiterally) {

                            if (!isPassed(postAggFunctionFilters, group, value)) return;
                            if (verbose) LOGGER.info(String.format("*** 1 Setting key %s, \n", key, aggBucketSrc.getPattern(), value));
                            if (isFieldTransformable){
                                transformFieldValuesForGroupMap.put(fieldName, value);
                                transformFieldValuesForGroupMap.put(group, value.floatValue());
                            }

                            Object val =  field.transform(value.doubleValue(), transformFieldValuesForGroupMap, foundFieldSet);

                            if (!postAggFunctions.isEmpty()) {
                                for (Function aggFunction : postAggFunctions) {
                                    aggFunction.execute(value.toString(),  aggFunction.getTag()+ "-" + fieldName);
                                }
                            } else {
                                if (globalFunctions.isEmpty()) {
                                    clientHistoDest.set(group, val, aggBucketSrc.getQueryPos(), aGroupId, querySourcePos);
                                    clientHistoDest.setField(group, fieldName, function.name(), field.description());
                                }
                            }

                            globalValues.put(group, val);

                            if (isFieldTransformable){
                                transformFieldValuesForGroupMap.put(fieldName, val);
                                transformFieldValuesForGroupMap.put(group, val);
                            }
                        }
                    }, sharedFunctionData, querySourcePos, bucketPos, totalBucketCount, (Map) groups, aggBucketSrc.getStart());

                } else if (object instanceof Map) {
                    function.extractResult(foundFieldSet, groupName, key, (Map) object, new ValueSetter() {
                        public void set(String group, Number value, boolean useThisResultLiterally) {
                            if (!isPassed(postAggFunctionFilters, group, value)) return;
                            String label = group;

                            if (isFieldTransformable) {
                                transformFieldValuesForGroupMap.put(fieldName, value);
                                transformFieldValuesForGroupMap.put(label, value.intValue());
                                value = (Double) field.transform(value.doubleValue(), transformFieldValuesForGroupMap, foundFieldSet);
                            }

                            if (!postAggFunctions.isEmpty()) {
                                for (Function aggFunction : postAggFunctions) {
                                    aggFunction.execute(value.toString(),  fieldName);
                                }
                            } else {
                                if (globalFunctions.isEmpty()) {
                                    clientHistoDest.set(label, value, aggBucketSrc.getQueryPos(), aGroupId, querySourcePos);
                                    clientHistoDest.setField(label, fieldName, function.name(), field.description());
                                }
                            }

                            globalValues.put(label, value);

                            if (isFieldTransformable) {
                                transformFieldValuesForGroupMap.put(fieldName, value);
                                transformFieldValuesForGroupMap.put(label, value);
                            }
                        }
                    }, aggBucketSrc.getStart(), aggBucketSrc.getEnd(), bucketPos, querySourcePos, totalBucketCount, sharedFunctionData, requestStartTimeMs, null);
                } else if (object instanceof Set) {
                    function.extractResult(key, (Set) object, new ValueSetter() {
                        public void set(String group, Number value, boolean useThisResultLiterally) {
                            if (!isPassed(postAggFunctionFilters, group, value)) return;
                            String label = key.length() > 0 ?  key + LogProperties.getFunctionSplit() + group : group;
                            if (key.contains(group)) label = key;

                            if (isFieldTransformable) {
                                transformFieldValuesForGroupMap.put(label, value);
                                transformFieldValuesForGroupMap.put(group, value);
                                transformFieldValuesForGroupMap.put(fieldName, value);
                                value = (Double) field.transform(value.doubleValue(), transformFieldValuesForGroupMap, foundFieldSet);
                            }

                            if (!postAggFunctions.isEmpty()) {
                                for (Function aggFunction : postAggFunctions) {
                                    aggFunction.execute(value.toString(), fieldName);
                                }
                            } else {
                                if (globalFunctions.isEmpty()) {
                                    clientHistoDest.set(label, value, aggBucketSrc.getQueryPos(), aGroupId, querySourcePos);
                                    clientHistoDest.setField(label, fieldName, function.name(), field.description());
                                }
                            }

                            globalValues.put(label, value);

                            if (isFieldTransformable) {
                                transformFieldValuesForGroupMap.put(label, value);
                                transformFieldValuesForGroupMap.put(fieldName, value);
                                transformFieldValuesForGroupMap.put(group, value);
                            }

                        }
                    }, aggBucketSrc.getStart(), aggBucketSrc.getEnd(), bucketPos, querySourcePos, totalBucketCount, sharedFunctionData);
                } else {
                    function.extractResult(foundFieldSet, groupName, function.getTag(),  (Map) groups, new ValueSetter() {
                        public void set(String group, Number value, boolean useThisResultLiterally) {

                            if (!isPassed(postAggFunctionFilters, group, value)) return;
                            String label = group;//key.length() > 0 ?  key + LogProperties.getFunctionSplit() + group : group;
                            //if (key.contains(group)) label = key;
                            if (useThisResultLiterally) label = group;


                            if (verbose) LOGGER.info(String.format("*** 3 Setting key %s, to %s\n", group, value.toString()));
                            if (isFieldTransformable){
                                transformFieldValuesForGroupMap.put(group, value);
                                transformFieldValuesForGroupMap.put(fieldName, value);
                                value = (Double) field.transform(value.doubleValue(), transformFieldValuesForGroupMap, foundFieldSet);
                            }
                            if (!postAggFunctions.isEmpty()) {
                                for (Function aggFunction : postAggFunctions) {
                                    aggFunction.execute(value.toString(),  group.replace(function.getTag(), fieldName) );
                                }
                            } else {
                                if (globalFunctions.isEmpty()) {
                                    clientHistoDest.set(label, value, aggBucketSrc.getQueryPos(), aGroupId, querySourcePos);
                                    clientHistoDest.setField(label, fieldName, function.name(), field.description());
                                }
                            }


                            globalValues.put(label, value);

                            if (isFieldTransformable){
                                transformFieldValuesForGroupMap.put(fieldName, value);
                                transformFieldValuesForGroupMap.put(group, value);
                            }
                        }
                    }, aggBucketSrc.getStart(), aggBucketSrc.getEnd(), bucketPos, querySourcePos, totalBucketCount, sharedFunctionData, requestStartTimeMs, request);
                }
            }
            groupId++;
        }
        for (GlobalFunction globalFunction : globalFunctions) {
            globalFunction.handle(globalValues, new ValueSetter() {
                @Override
                public void set(String group, Number value, boolean useThisResultLiterally) {
                    clientHistoDest.set(group, value, aggBucketSrc.getQueryPos(), 0, querySourcePos);
                }
            });
        }
    }
    protected boolean isPassed(List<Filter> thisFunctionFilters, String key, Number value) {
        if (thisFunctionFilters.size() == 0) return true;
        if (key.contains(Function.TAG_SEPARATOR)) {
            String[] split = key.split(Function.TAG_SEPARATOR);
            key = split[split.length-1];
        }
        for (Filter filter : thisFunctionFilters) {
            if (filter.isNumeric() && !filter.execute(value.toString())) return false;
            if (!filter.isNumeric() && !filter.execute(key.toString())) return false;
        }
        return true;
    }
    private FieldI getFieldForBucket(final Set<FieldSet> fieldSets, final String fieldName) {
        if (fieldSets == null) return null;
        for (FieldSet set : fieldSets) {
            FieldI field = set.getField(fieldName);
            if (field != null) return field;
        }
        return null;
    }
    private FieldSet getFieldSetForBucket(final Set<FieldSet> fieldSets, final String fieldName) {
        if (fieldSets == null) return null;
        for (FieldSet set : fieldSets) {
            FieldI field = set.getField(fieldName);
            if (field != null) return set;
        }
        return null;
    }


    private List<Filter> getPostAggFiltersForFunctionTag(String tag, List<Filter> filters) {
        ArrayList<Filter> results = new ArrayList<Filter>();
        if (filters != null) {
            for (Filter filter : filters) {
                if (filter.group().equals(tag)) results.add(filter);
            }
        }
        return results;
    }




    public void putIntoHistogram(ClientHistoItem histogramBucket, Bucket bucket, DateTime fromTime, DateTime toTime, int bucketCount, List<Filter> filters) {
        if (filters != null && filters.size() > 0) {
            boolean passed = true;
            for (Filter filter : filters) {
                if (filter.isAppledAtFinalAgg()) {
                    if (!filter.execute(Integer.valueOf(bucket.hits()).toString())) passed = false;
                }
            }
            if (passed) histogramBucket.increment(bucket.getPattern(), bucket.hits(), bucket.getQueryPos(), 0);

        } else {
            histogramBucket.increment(bucket.getPattern(), bucket.hits(), bucket.getQueryPos(), 0);
        }
    }


    public List<ClientHistoItem> getClientHistoFromRawHisto(LogRequest request, final List<Map<String, Bucket>> srcHisto, Map<Integer,Set<String>> functionTags, List<FieldSet> fieldSets) {


        if (srcHisto == null) return new ArrayList<ClientHistoItem>();

        if (request.isVerbose()) LOGGER.info("======================== getClientHistoXML\t=============");


        if (request.isVerbose()) LOGGER.info("======================== Convert\t=============");

        // build base template for results
        final List<ClientHistoItem> result = newClientHistogram(srcHisto);

        if (request.isVerbose()) LOGGER.info("======================== Final stage\t=============");


        // allow each call to use the shared memory space for processing subsequent data
        // String == KEY = UID for function processing - i.e. might be function.toStringID()+queryPos
        Map<String, Object> sharedFunctionData = new HashMap<String,Object>();

        int currentBucketNumber = 0;

        for (Map<String, Bucket> histoItem : srcHisto) {


            if (request.isVerbose()) LOGGER.info("\t\tProcessing Bucket:" + currentBucketNumber);
            for (Bucket aggBucket : histoItem.values()) {
                //int bucketNumber = getBucketNumber(getBucketHitTime(aggBucket), request.getStartTimeMs(), request.getEndTimeMs(), request.getBucketCount());

                // somehow we got a dodgy bucket
                //if (bucketNumber < 0) continue;

                convertToClientHistoItem(result.get(currentBucketNumber), aggBucket, currentBucketNumber, request.query(aggBucket.getQueryPos()).isGroupBy(), request, currentBucketNumber, sharedFunctionData, srcHisto.size(), functionTags, fieldSets);

                // allow it to short circuit if things get cancelled
                if (request.isCancelled() || request.isExpired()) return result;//if (isCancelled(request.subscriber())) return result;
            }
            currentBucketNumber++;
        }
        // now let the last change stuff to be handled
        if (srcHisto.isEmpty()) throw new RuntimeException("EmptyHistoProvided:" + request);
        Bucket firstBucket = srcHisto.get(0).values().iterator().next();
        List<Function> functions = firstBucket.functions();
        for (Function function : functions) {
            function.flushAggResultsForBucket(firstBucket.getQueryPos(), sharedFunctionData);
//			function.reset();
        }

        if (request.isVerbose()) LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<< Convert =============");

        return result;
    }

    /**
     * Push all source histograms into single histogram
     * @return
     */
    public List<Map<String, Bucket>> mergeHistosToAggdHisto(LogRequest request, Map<String, List<Map<String, Bucket>>> histosFromSources) {


        List<Map<String, Bucket>> aggdHisto = bucketHistoAssembler.newHistogram(new DateTime(request.getStartTimeMs()), new DateTime(request.getEndTimeMs()), request.getBucketCount(), request.copy().queries(), request.subscriber(), null, null);
        if (request.isVerbose()) LOGGER.info(">>>>>>>>>>>>>>>>>>>>>> assemble from histos =============");

        for (String aggSpaceId : histosFromSources.keySet()) {
            List<Map<String, Bucket>> list = histosFromSources.get(aggSpaceId);
            if (request.isVerbose()) LOGGER.info(String.format("LOGGER ********** P[%s] Tenors",aggSpaceId));
            int bucketNumber = 0;
            for (Map<String, Bucket> histoFromSource : list) {
                if (histoFromSource == null) continue;
                Set<String> patternsKeySet = histoFromSource.keySet();
                if (request.isVerbose()) LOGGER.info(String.format("LOGGER  ******** P[%s] Tenors[%s]",aggSpaceId, histoFromSource.keySet().toString()));

                for (String patternKey : patternsKeySet) {
                    Bucket sourceBucket = histoFromSource.get(patternKey);
                    if (sourceBucket.hits() == 0) {
//						LOGGER.info(String.format("    [%s] ********** P[%s] [%s=>%s] hits[%d] res:%s", bucket.getId(), bKey, formatter.print(bucket.getStart()), formatter.print(bucket.getEnd()), bucket.hits(), bucket.getAggregateResults()));
                        continue;
                    }

                    Bucket aggdBucket = bucketHistoAssembler.getTargetBucket(aggdHisto, sourceBucket);
                    bucketHistoAssembler.handle(aggdBucket, sourceBucket, bucketNumber++, request.isVerbose(), true);
                    if (request.isVerbose()) LOGGER.info(String.format(" LOGGER  [%s] ********** P[%s] [%s=>%s] hits[%d] res:%s", sourceBucket.subscriber(), patternKey, formatter.print(sourceBucket.getStart()), formatter.print(sourceBucket.getEnd()), sourceBucket.hits(), sourceBucket.getAggregateResults()));
                }
            }

        }

        // prime the results map
        for (Map<String, Bucket> histoItem : aggdHisto) {
            for (Bucket bucket : histoItem.values()) {
                bucket.convertFuncResults(request.isVerbose());
                //if (request.isVerbose()) LOGGER.info("bucket::" + bucket.getAggregateResults());
            }
        }
        return aggdHisto;
    }

}
