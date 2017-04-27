package com.liquidlabs.log.search.facetmap;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.reader.JSONExtractor;
import com.liquidlabs.log.search.functions.CountFaster;
import com.liquidlabs.log.search.functions.CountUniqueHyperLog;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.handlers.SummaryBucket;
import com.logscape.disco.indexer.Pair;
import org.joda.time.DateTime;
import org.prevayler.Prevayler;
import org.prevayler.PrevaylerFactory;
import org.prevayler.foundation.serialization.XStreamSerializer;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

/**
 * Stored LogFile summary information
 */
public class PersistingFacetMap {
    // NOTE: Only needs to be set for write operations
    private boolean isJson = false;

    public PersistingFacetMap(String env, String firstLine) {
        new File(env).mkdirs();
        this.env = env;
        this.isJson = JSONExtractor.isJson(firstLine);
    }

    private QueueHandler queueHandler;
    String env = ".";

    public void write(String logFile, int logId, List<Pair> standardFields, List<Pair> discovered, int lineNumber) {
        // file is being ingested again
        if (lineNumber == 1) {
            FileUtil.deleteDir(new File(env + "/" + logId + ".sum"));
        }
        List<Pair> myiscovered = discovered;
        if (isJson) {
            myiscovered = reduceJsonArrays(discovered);
        }
        getQueueHandler(logId).addAll(standardFields, myiscovered);

    }

    private ArrayList<Pair> reduceJsonArrays(List<Pair> discovered) {
        ArrayList<Pair> results = new ArrayList<>();
        JSONExtractor jsonExtractor = new JSONExtractor();
        for (Pair pair : discovered) {
            if (pair.key.contains("_")) {
                pair = new Pair(jsonExtractor.dedupe(pair.key), pair.value);
            }
            results.add(pair);
        }
        return results;
    }

    public void flush(int logId) {
        getQueueHandler(logId).flush();
    }

    private QueueHandler getQueueHandler(int logId) {
        if (queueHandler == null) queueHandler = new QueueHandler(env, logId);
        return queueHandler;
    }

    public Map<String, Map<String, Function>> read(int i) {
            return getQueueHandler(i).map();
    }

    static String getDir(String env, int logId) {
        int bucketing = 1000;
        int modd = logId / bucketing;
        return env + "/" + modd + "/" + logId + ".sum";
    }

    public static class QueueHandler {
        private final String prevalenceDirectory;
        Prevayler prevayler;
        PersistentMap<String,Map<String,Function>> functionsMap;
        private int logId;

        public QueueHandler(String env, int logId)  {
            PrevaylerFactory factory = new PrevaylerFactory();

            if (Boolean.getBoolean("facets.xstream")) {
                factory.configureSnapshotSerializer(new XStreamSerializer());
                factory.configureJournalSerializer(new XStreamSerializer());
            }
            factory.configurePrevalentSystem(new HashMap<String, Map<String, Function>>());
            prevalenceDirectory = getDir(env, logId);
            factory.configurePrevalenceDirectory(prevalenceDirectory);
            try {
                prevayler = factory.create();
            } catch (Exception e) {
                throw new RuntimeException(e.toString() + " id:" + logId, e);
            }
            functionsMap = new PersistentMap<>(prevayler);
            this.logId = logId;
        }

        public void flush() {
            try {
                prevayler.takeSnapshot();
                removeOldJournals();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void removeOldJournals() {
            File[] journals = new File(prevalenceDirectory).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.contains("journal") && dir.lastModified() > new DateTime().minusDays(1).getMillis();
                }
            });
            if (journals != null) {
                for (int i = 0; i < journals.length; i++) {
                    File journal = journals[i];
                    journal.delete();
                }
            }
        }

        public void addAll(List<Pair> fields, List<Pair> discoveredFields) {

            buildFunctionsMap(fields, "");
            processFields(fields, "");
            buildFunctionsMap(discoveredFields, "DYN_");
            processFields(discoveredFields, "DYN_");
        }

        private void processFields(List<Pair> fields, String prefix) {
            for (Pair field : fields) {
                if (field.value == null) continue;
                Map<String, Function> map = functionsMap.get(field.key);
                if (map != null) {
                    Collection<Function> iter = map.values();
                    for (Function function : iter) {
                        function.calculate(field.value);
                    }
                }
            }
        }

        private void buildFunctionsMap(List<Pair> dynamicFields, String prefix) {
            for (Pair field : dynamicFields) {
                if (this.functionsMap.containsKey(field.key)) continue;
                CountFaster count = new CountFaster(prefix +field.key, field.key);
                count.setTopLimit(LogProperties.getSummaryTopLimit());
                count.setMaxAggSize(256);
                addFunctionMapItem(field.key, prefix + field.key, count);

                CountUniqueHyperLog cUnique = new CountUniqueHyperLog(prefix + field.key + SummaryBucket.AUTO_TAG_CU, field.key);
                addFunctionMapItem(field.key, prefix + field.key + SummaryBucket.AUTO_TAG_CU, cUnique);
            }
        }
        private void addFunctionMapItem(String fieldName, String stag, Function function) {
            Map<String, Function> stringFunctionMap = this.functionsMap.get(fieldName);
            if (stringFunctionMap == null) {
                stringFunctionMap = new HashMap<String, Function>();
                this.functionsMap.put(fieldName, stringFunctionMap);
                // do this for prevayler
                stringFunctionMap = this.functionsMap.get(fieldName);
            }
            stringFunctionMap.put(stag, function);
        }

        public Map<String, Map<String, Function>> map() {
            return functionsMap;
        }
    }
}
