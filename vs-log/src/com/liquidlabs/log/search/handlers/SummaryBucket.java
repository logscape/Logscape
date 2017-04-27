package com.liquidlabs.log.search.handlers;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.functions.CountFaster;
import com.liquidlabs.log.search.functions.CountUniqueHyperLog;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.functions.Summary;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.transport.serialization.Convertor;
import com.logscape.disco.indexer.IndexFeed;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static com.liquidlabs.transport.serialization.Convertor.deserialize;

/**
 * Automatically counts items when increment is called. This bucket is shared between all searchers so needs to support concurrency
 * @author neil
 *
 */
public class SummaryBucket extends Bucket {
	
	private static final Logger LOGGER = Logger.getLogger(SummaryBucket.class);

	
	public static final String AUTO_TAG_CU = "_CU_";
    public static final String AUTO_TAG_SUM = "_SUM_";
	private static final String EMPTY_STRING = "";
	private static final long serialVersionUID = 1L;
	private transient Set<String> processedFieldSet = new HashSet<String>();
    private transient AggSpace aggSpace;
    public transient Map<String,Map<String,Function>> functionsMap = new HashMap<String,Map<String,Function>>();
    private boolean processSystemFields = false;

    public SummaryBucket() {
	}
	
	public SummaryBucket(String subscriber, AggSpace aggSpace) {
        this.aggSpace = aggSpace;
        this.subscriber = subscriber.intern();
		this.id = UID.getUUID();
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Created:" + subscriber);
	}

	transient Set<String> fieldsAdded = new HashSet<String>();

	private boolean multipleTypes = false;
    private boolean abort = false;
    Map<String, Set<String>> fieldsWithMTypesTrue = new HashMap<String, Set<String>>();
    Map<String, Set<String>> fieldsWithMTypesFalse = new HashMap<String, Set<String>>();

    public void setMultiTypes(boolean isMulti) {
        this.multipleTypes = isMulti;
    }

	public void increment(final FieldSet fieldSet, final String[] fieldValues, final String filenameOnly, final String filename, final long time, final long fileStartTime, final long fileEndTime,
                          final int lineNumber, final String lineData, final MatchResult matchResult, final boolean isSummaryRequired, long requestStartMs, long requestEndMs) {
        if (abort) return;

		// add FieldSet functions when we are collecting summary information
        if (!processedFieldSet.contains(fieldSet.id)) {
            processedFieldSet.add(fieldSet.id);
            addFieldSet(fieldSet);
            if (processedFieldSet.size() > 1) multipleTypes = true;
        }
        try {

            /**
             * Get the current fields only one time!
             */
            Set<String> currentFields = multipleTypes ? fieldsWithMTypesTrue.get(fieldSet.getId()) : fieldsWithMTypesFalse.get(fieldSet.getId());
            if (currentFields == null) {
                currentFields = new HashSet<String>(fieldSet.getFieldNames(true, false, false, multipleTypes, processSystemFields));
                if (multipleTypes) {
                    fieldsWithMTypesTrue.put(fieldSet.getId(), currentFields);
                } else {
                    fieldsWithMTypesFalse.put(fieldSet.getId(), currentFields);
                }
            }

            /**
             * Handle the current fields!
             */
            for (String fieldName : currentFields) {

                Map<String, Function> map = functionsMap.get(fieldName);
                if (map != null) {
                    String fieldValue = fieldSet.getFieldValue(fieldName, fieldValues);
                    if (fieldValue == null || fieldValue.length() == 0) continue;

                    Collection<Function> iter = map.values();
                    for (Function function : iter) {
                        function.calculate(fieldValue);
                    }
                }
            }

            /**
             * Handle the dynamic fieldfields!
             */
            Map<String, String> dynamicFields = buildDynamicFields(fieldSet.getDynamicFields());
            for (Map.Entry<String, String> field : dynamicFields.entrySet()) {
                String value = field.getValue();
                if (value == null || value.length() == 0) continue;
                Map<String, Function> map = functionsMap.get(field.getKey());
                if (map != null) {
                    Collection<Function> iter = map.values();
                    for (Function function : iter) {

                        function.calculate(value);
                    }
                }
            }

        } catch (Throwable t) {
            t.printStackTrace();;
            LOGGER.error(String.format("%s: sub:%s q:%s \n\tFailed to handle:%s ex:%s file:%s line:%d sub:%s ", id, subscriber, pattern, "", t.toString(), filename, lineNumber, subscriber), t);
            abort = true;
        }
	}

    private Map<String, String> buildDynamicFields(Map<String, String> dynamicFields) {
        String timestampField = FieldSet.DEF_FIELDS._timestamp.name();
        String dateTimeField = FieldSet.DEF_FIELDS._datetime.name();
        String lineField = IndexFeed.FIELDS._line.name();
        Set<String> keys = dynamicFields.keySet();
        for (String fieldName : keys) {
            if (fieldsAdded.size() > LogProperties.getMaxFields()) continue;
            // dont summarize _timestamp or _line fields
            if (fieldName.equals(timestampField)) continue;
            if (fieldName.equals(dateTimeField)) continue;
            if (fieldName.equals(lineField)) continue;

            if (fieldsAdded.contains(fieldName)) continue;
            fieldsAdded.add(fieldName);
            CountFaster count = new CountFaster("DYN_" +fieldName, fieldName);
            count.setTopLimit(LogProperties.getSummaryTopLimit());
            count.setMaxAggSize(256);
            addFunctionMapItem(fieldName, "DYN_" + fieldName, count);

            CountUniqueHyperLog cUnique = new CountUniqueHyperLog("DYN_" + fieldName + AUTO_TAG_CU, fieldName);
            addFunctionMapItem(fieldName, "DYN_" + fieldName + AUTO_TAG_CU, cUnique);
        }
        return dynamicFields;
    }

    private void addFieldSet(final FieldSet fieldSet) {
        if (processedFieldSet.size() > 1) multipleTypes = true;
		for (FieldI field : fieldSet.fields()) {
            String fieldName = field.name();
			// if it is a multi-type search and we have processed more than one type then only add system fields.
			if (multipleTypes && !FieldSet.isDefaultField(fieldName)) continue;

			if (!field.isSummary()) continue;
			if (fieldsAdded.contains(fieldName)) continue;
			fieldsAdded.add(fieldName);

            if (LOGGER.isDebugEnabled()) LOGGER.debug("Created FUNCTION:" + subscriber + "  " + fieldSet.id + "." + fieldName);

            if (fieldName.equals(FieldSet.DEF_FIELDS._size.name())) {
                Summary sum = new Summary(fieldName + AUTO_TAG_SUM, "", fieldName);
                addFunctionMapItem(fieldName, fieldName + AUTO_TAG_SUM, sum);


            } else {

                CountFaster count = new CountFaster(fieldName, fieldName);
                if (fieldName.equals(FieldSet.DEF_FIELDS._sourceUrl.name())) {
                    count.setTopLimit(25);
                } else if (FieldSet.isDefaultField(fieldName)) {
                    count.setTopLimit(11);
                } else {
                    count.setTopLimit(LogProperties.getSummaryTopLimit());
                }
                count.setMaxAggSize(1024);
                addFunctionMapItem(fieldName, fieldName, count);

                CountUniqueHyperLog cUnique = new CountUniqueHyperLog(fieldName + AUTO_TAG_CU, fieldName);
                addFunctionMapItem(fieldName, fieldName + AUTO_TAG_CU, cUnique);
            }
		}
	}

    private void addFunctionMapItem(String fieldName, String stag, Function function) {
        Map<String, Function> stringFunctionMap = this.functionsMap.get(fieldName);
        if (stringFunctionMap == null) {
            stringFunctionMap = new HashMap<>();
            this.functionsMap.put(fieldName, stringFunctionMap);
        }
        if (!stringFunctionMap.containsKey(stag)) {
            stringFunctionMap.put(stag, function);
            super.functions.put(function.toStringId(), function);
        } else {
            function = stringFunctionMap.get(stag);
            super.functions.put(function.toStringId(), function);
        }
        fieldsAdded.add(fieldName);
    }

    public Bucket copy()  {
        try {
            SummaryBucket acb = (SummaryBucket) deserialize(Convertor.serialize(this));
            acb.aggSpace = this.aggSpace;
            return acb;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to copy:" + e, e);
        }
        return null;
    }

    @Override
    public void resetResults() {
//        super.resetFunctions();
//        super.functions.clear();
        this.functionsMap = null;
        this.fieldsAdded = null;
        this.processedFieldSet = null;
        this.fieldsWithMTypesFalse = null;
        this.fieldsWithMTypesTrue = null;
        this.processedFieldSet = null;
    }

    public void incrementSystemFields(FieldSet fieldSet, LogFile logFile, int numberOfLines, long bucketTime) {
        Map<String, String> dynamicFields = fieldSet.getDynamicFields();
        fieldSet.setDiscoFields(new HashMap<String, String>());
        processSystemFields = true;
        increment(fieldSet, new String[0], logFile.getFileNameOnly(), logFile.getFileName(), bucketTime, logFile.getStartTime(), logFile.getEndTime(), 1, "", new MatchResult(), false, logFile.getStartTime(), logFile.getEndTime());
        processSystemFields = false;
        updateSystemField(fieldSet, numberOfLines, FieldSet.DEF_FIELDS._agent);
        updateSystemField(fieldSet, numberOfLines, FieldSet.DEF_FIELDS._filename);
        updateSystemField(fieldSet, numberOfLines, FieldSet.DEF_FIELDS._type);
        updateSystemField(fieldSet, numberOfLines, FieldSet.DEF_FIELDS._tag);
        updateSystemField(fieldSet, numberOfLines, FieldSet.DEF_FIELDS._host);
        fieldSet.setDiscoFields(dynamicFields);

        Set<Map.Entry<String, Map<String, Function>>> entries = this.functionsMap.entrySet();
        for (Map.Entry<String, Map<String, Function>> entry : entries) {
            Map<String, Function> value = entry.getValue();
            Set<Map.Entry<String, Function>> entries1 = value.entrySet();
            for (Map.Entry<String, Function> stringFunctionEntry : entries1) {
                super.functions.put(stringFunctionEntry.getValue().toString(), stringFunctionEntry.getValue());
            }
        }
    }

    private void updateSystemField(FieldSet fieldSet, int numberOfLines, FieldSet.DEF_FIELDS field) {
        String fieldName = field.name();
        String value = fieldSet.getDefaultFieldValue(field);


        Map<String, Function> stringFunctionMap = functionsMap.get(fieldName);
        Function countFaster = stringFunctionMap.get(fieldName);
        countFaster.increment(fieldName, value, numberOfLines);
        Function countDistinct = stringFunctionMap.get(fieldName + AUTO_TAG_CU);
        countDistinct.increment(fieldName, value, numberOfLines);

        fieldsWithMTypesFalse.clear();
        fieldsWithMTypesTrue.clear();
        increment(numberOfLines);

    }
}
