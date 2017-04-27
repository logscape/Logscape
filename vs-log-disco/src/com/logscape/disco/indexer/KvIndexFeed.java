package com.logscape.disco.indexer;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
//import com.liquidlabs.log.DiscoProperties;
//import com.liquidlabs.log.fields.FieldSet;
//import com.liquidlabs.log.fields.field.FieldI;
//import com.liquidlabs.log.fields.field.LiteralField;
//import com.liquidlabs.log.fields.kv.KVExtractor;
//import com.liquidlabs.log.fields.kv.RulesKeyValueExtractor;
//import com.liquidlabs.log.disco.grokit.GrokItPool;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.indexer.persistit.PersisitDbFactory;
import com.logscape.disco.kv.KVExtractor;
import com.logscape.disco.kv.RulesKeyValueExtractor;
import com.logscape.disco.grokit.GrokItPool;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 17/09/2013
 * Time: 15:40
 *  NOTE: Choose your DB impl!
 *  MapDB is 20% faster than persistit - but MapDB is flakey
 *  (System.getProperty("kvi.db.impl","persistit").equals("persistit")) {
 */
public class KvIndexFeed implements IndexFeed {

    private static final Logger LOGGER = Logger.getLogger(KvIndexFeed.class);

    private static final int itemsPerGB = Integer.getInteger("pi.idx.cache.items.per.gb",2) * 1000 * 1000;
    public static final int COMMIT_INTERVAL = Integer.getInteger("kvi.commit.interval", 30);
    public static final ArrayList<Pair> NO_DATA = new ArrayList<Pair>();
    public static final HashMap<String,String> NO_DATA_MAP = new HashMap<String, String>();


    static final public BlockingQueue<Runnable> commitQueue = new LinkedBlockingQueue<Runnable>(1000);


    public static boolean rebuildRequired = false;

    private KVExtractor kvExtractor;
    private final KvIndex indexDb;
    private LookUpTable lutDb;
    private Dictionary dictionary;
    GrokItPool grokIt;
    private Db<Integer, String[]> lutDB;
    private Db<TupleIntInt, int[]> kvIndexDB;

    public KvIndexFeed(RulesKeyValueExtractor kvExtractor, KvIndex indexDb, LookUpTable lutDb, Dictionary dictionary, GrokItPool grokIt, Db<Integer, String[]> lutDB, Db<TupleIntInt, int[]> kvIndexDB) {
        this.kvExtractor = kvExtractor;
        this.indexDb = indexDb;
        this.lutDb = lutDb;
        this.dictionary = dictionary;
        this.grokIt = grokIt;

        this.lutDB = lutDB;
        this.kvIndexDB = kvIndexDB;
    }

    @Override
    public void setKvExtractor(KVExtractor kve) {
        this.kvExtractor = kve;
    }

    @Override
    public KVExtractor kvExtractor() {
        return kvExtractor;
    }


    public List<Pair> index(int logId, String filename, int lineNo, long timeMs, String data, boolean isFieldDiscoveryEnabled, boolean grokDiscoveryEnabled, boolean isSystemFieldsEnbled, List<Pair> indexedFields) {

        List<Pair> fields = new ArrayList<Pair>();

        if (indexedFields != null) fields.addAll(indexedFields);

        if (isFieldDiscoveryEnabled) {
            fields.addAll(kvExtractor.getFields(data));
        }

        if (grokDiscoveryEnabled) {
            addFields(fields, grokIt.processLine(filename, data));
        }
        if (isSystemFieldsEnbled) {
            fields.add(new Pair("_timestamp",Integer.toString(DiscoProperties.fromMsToSec(timeMs))));
            fields.add(new Pair("_datetime", DateUtil.shortDateTimeFormat6.print(timeMs)));
        }

        return fields;
    }

    public void store(int logId, int lineNo, List<Pair> fields) {
        if (fields.size() > 0) {
            String[] normalizedData = lutDb.normalize(logId, fields);
            indexDb.index(logId, lineNo, normalizedData);
        }
    }

    private void addFields(List<Pair> fields, Map<String, String> grokFields) {
        boolean added = false;
        for (Map.Entry<String, String> entry : grokFields.entrySet()) {
            String value = entry.getValue();
            String name = entry.getKey();
            added = false;

            for (Pair field : fields) {
                if (field.key.equals(name)) {
                    field.value = value;
                    added = true;
                }
            }
            if (!added) {
                fields.add(new Pair(name, value));
            }
        }
    }

    public void setKeyValueExtractor(RulesKeyValueExtractor kvExtractor) {
        this.kvExtractor = kvExtractor;
    }

    public List<Pair> get(int logId, int lineNo) {
        int[] normalizedData = indexDb.get(logId, lineNo);
        if (normalizedData == null || normalizedData.length == 0) return NO_DATA;
        return lutDb.get(logId, normalizedData, 0);
    }

    public Map<String,String> getAsMap(int logId, int lineNo, long timeMs) {
//        if (true) return new HashMap<String, String>();

        // check if it is indexed
        String[] strings = lutDB.get(logId);
        if (strings == null) {
            return lutDb.getAsMap(logId, new int[0], timeMs);
        }

        int[] normalizedData = indexDb.get(logId, lineNo);
        // check is a blank line was put into the store
        if (normalizedData == null || normalizedData.length == 0) return NO_DATA_MAP;
        return lutDb.getAsMap(logId, normalizedData, timeMs);
    }

    public void remove(int logId) {
        LOGGER.info("Remove:" + logId);
        indexDb.remove(logId);
        lutDb.remove(logId);
        dictionary.remove(logId);
    }

    public void compact() {
        this.kvIndexDB.compact();
        this.lutDB.compact();
    }


    public void commit() {
        commitQueue.offer(new Runnable() {
            @Override
            public void run() {
                doCommit();
            }
        });
    }

    synchronized  private void doCommit() {
        indexDb.commit();
        lutDb.commit();
        dictionary.commit();

    }

    public void close() {
        LOGGER.info("Close");
        doCommit();
        dictionary.close();
        indexDb.close();
        lutDb.close();
    }

    public boolean isRebuildRequired() {
        return rebuildRequired;
    }

    public static final String REBUILD_DB = "rebuild-db.";

    /**
     * Called from vs-log.bundle
     */
    public static void rebuildIndex() {
        if (KvIndexFactory.readOnly) {
            return;
        }
        String env = KvIndexFactory.env;
        LOGGER.info("Forcing KV_INDEX Rebuild on:" + env + " Exists:" + new File(env).exists());
        if (new File(env).exists()) {

            int i = FileUtil.deleteDir(new File(env));
            if (i < 0) {
                LOGGER.warn("Failed to delete DIR:" + env);
                // if we cant delete it now - then leave a rebuild-db file so we can rebuild it on the bounce
                rebuildRequired = true;
                try {
                    new File(env, REBUILD_DB).createNewFile();
                } catch (IOException e) {
                }
            }  else {
                new File(env, REBUILD_DB).delete();
            }
            new File(env).mkdirs();

        } else {
            try {
                new File(env).mkdirs();
                new File(env, REBUILD_DB).getParentFile().mkdirs();
                new File(env, REBUILD_DB).createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // copy over the persistit config file
        PersisitDbFactory.copyConfigFile();



    }

    public void reset() {
        lutDB = null;
        kvIndexDB = null;
    }

    private static ScheduledExecutorService scheduleCommit() {
        final ScheduledExecutorService kvCommit = ExecutorService.newScheduledThreadPool("services");
        kvCommit.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if(!commitQueue.isEmpty()) {
                    try {
                        commitQueue.take().run();
                        commitQueue.clear();
                    } catch (Exception e) {
                    }
                }
            }
        }, 60, COMMIT_INTERVAL, TimeUnit.SECONDS);
        return kvCommit;
    }

    public static int getHeapGB() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long max = memoryMXBean.getHeapMemoryUsage().getMax();
        return (int) Math.max(max / 1023 / 1023 / 1000, 1);
    }


}
