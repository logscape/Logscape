package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.DirectMemoryUtils;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.vso.agent.MyOperatingSystemMXBean;
import com.logscape.disco.DiscoProperties;
import com.liquidlabs.vso.VSOProperties;
import com.logscape.disco.indexer.Db;
import com.logscape.disco.indexer.KvIndexFeed;
import com.logscape.disco.indexer.TupleIntInt;
import org.apache.log4j.Logger;
import org.mapdb.*;

import java.io.File;
import java.lang.management.ManagementFactory;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 22/11/13
 * Time: 21:59
 * To change this template use File | Settings | File Templates.
 */
public class MapDbFactory {

    private static final Logger LOGGER = Logger.getLogger(KvIndexFeed.class);

    static boolean readOnly = Boolean.getBoolean("log.db.readonly");
    static String env = System.getProperty("kv.env", DiscoProperties.getKVIndexDB());


    static Db<TupleIntInt, int[]> getIndexDb() {
        final String dbName = "kvIndex";
        final MapDb<TupleIntInt, int[]> dbOne = new MapDb<TupleIntInt, int[]>(getDbIndex(env, "1"), dbName);
        final MapDb<TupleIntInt, int[]> dbTwo = new MapDb<TupleIntInt, int[]>(getDbIndex(env, "2"), dbName);
        return new DbWrapper<TupleIntInt, int[]>(dbOne, dbTwo, dbName, new LoggingEventMonitor());
    }

    static public Db<Integer, String[]> getLutDb() {

        final String dbName = "lut";
        try {
            final MapDb<Integer, String> dbOne = new MapDb<Integer, String>(getDbLut(env, "1"), dbName);
            final MapDb<Integer, String> dbTwo = new MapDb<Integer, String>(getDbLut(env, "2"), dbName);
            return new DbWrapper<Integer, String[]>(dbOne, dbTwo, dbName, new LoggingEventMonitor());
        } catch (Throwable t) {
            t.printStackTrace();
            LOGGER.warn("Failed to open existing KV_INDEX:" + t, t);
            LOGGER.warn("Going to rebuild");

            KvIndexFeed.rebuildRequired = true;
            FileUtil.deleteDir(new File(env));
            final MapDb<Integer, String> dbOne = new MapDb<Integer, String>(getDbLut(env, "1"), dbName);
            final MapDb<Integer, String> dbTwo = new MapDb<Integer, String>(getDbLut(env, "2"), dbName);
            return new DbWrapper<Integer, String[]>(dbOne, dbTwo, dbName, new LoggingEventMonitor());
        }
    }

    private static DB getDbIndex(String environmentDir, String thing) {
        return getDbFOR(environmentDir, "KV_DB_" + thing, "kvIndex", BTreeKeySerializer.TUPLE2, new StringArraySerialiser());
    }
    private static DB getDbLut(String environmentDir, String thing) {
        return getDbFOR(environmentDir, "LUT_DB_" + thing, "lut", BTreeKeySerializer.ZERO_OR_POSITIVE_INT, new StringArraySerialiser());
    }
    private static DB getDbFOR(String environmentDir, String dbName, String treemapName, BTreeKeySerializer keySerializer, Serializer valueSerializer) {
        new File(environmentDir).mkdirs();
        DB db =  getDb(environmentDir, dbName)
                .transactionDisable()
                .commitFileSyncDisable()
                .cacheLRUEnable()
                .cacheWeakRefEnable().
                        closeOnJvmShutdown().
                        make();


        if(!db.exists(treemapName)) {
            DB.BTreeMapMaker bTreeMapMaker = db.createTreeMap(treemapName).nodeSize(32).keySerializer(keySerializer);
            if (valueSerializer != null) bTreeMapMaker = bTreeMapMaker.valueSerializer(valueSerializer);
            bTreeMapMaker.make();
        }
        return db;
    }

    private static DBMaker getDb(String environmentDir, String name) {
        if (Boolean.getBoolean("test.mode")) {
            return DBMaker.newHeapDB();
        }

        final DBMaker dbMaker = DBMaker.newFileDB(new File(environmentDir, name));

        if (readOnly) dbMaker.readOnly();

        if(isWindows()) {
            return dbMaker;//.mmapFileEnableIfSupported()
        }
        if (hasEnoughSystemMemory() && isIndexStore()) {
            // use system memory
            return dbMaker;
        } else {
            // use on-heap only - but map the index
            return dbMaker.mmapFileEnableIfSupported();
        }
    }

    private static boolean hasEnoughSystemMemory() {
        long defaultDirectMemorySize = (long) FileUtil.getMEGABYTES(DirectMemoryUtils.getDefaultDirectMemorySize());
        long directMemSize = DirectMemoryUtils.getDirectMemorySize();
        if (directMemSize == -1) return false;
        else return true;
//        long directMemorySize = (long) FileUtil.getMEGABYTES(directMemSize);
//        LOGGER.info("DirectMemory Setting:" + directMemorySize);
//        return defaultDirectMemorySize != directMemorySize;// && getPhysicalMemTotalMb() > 4096;
    }
    public static int getPhysicalMemTotalMb() {
        return (int) (new MyOperatingSystemMXBean().getTotalPhysicalMemorySize()/(1024 * 1024));
    }
    private static boolean isIndexStore() {
        String resourceType = VSOProperties.getResourceType();
        return resourceType.contains("Management") || resourceType.contains("Failover") || resourceType.contains("Server") || resourceType.contains("IndexStore") ;
    }
    private static boolean isWindows() {
        return System.getProperty("os.name").toUpperCase().contains("WINDOW");
    }

    private static DB dictDB = null;

    public static Db<Integer, String> getDictDb(String dictTree) {
        if (dictDB == null) {
            dictDB = getDbFOR(env, "Dict", dictTree, BTreeKeySerializer.ZERO_OR_POSITIVE_INT, Serializer.STRING);
        } else {
            makeDbTree(dictDB, dictTree, BTreeKeySerializer.ZERO_OR_POSITIVE_INT, Serializer.STRING);
        }
        return new MapDb<Integer, String>(dictDB, dictTree);
    }

    private static DB revDictDB = null;
    public static Db<String, Integer> getRevDictDb(String tree) {
        if (revDictDB == null) {
            revDictDB = getDbFOR(env, "DictRev", tree, BTreeKeySerializer.STRING, Serializer.INTEGER);
        } else {
            makeDbTree(revDictDB, tree, BTreeKeySerializer.STRING, Serializer.INTEGER);
        }
        return new MapDb<String, Integer>(revDictDB, tree);
    }

    private static void makeDbTree(DB dictDB, String treemapName, BTreeKeySerializer keySerializer, Serializer valueSerializer) {
        if(!dictDB.exists(treemapName)) {
            DB.BTreeMapMaker bTreeMapMaker = dictDB.createTreeMap(treemapName).nodeSize(32).keySerializer(keySerializer);
            if (valueSerializer != null) bTreeMapMaker = bTreeMapMaker.valueSerializer(valueSerializer);
            bTreeMapMaker.make();
        }
    }

    private static DB countDB = null;
    public static Db<Integer, Integer> getDictCountDb(String db) {
        if (countDB == null) {
            countDB = getDbFOR(env, "DictCount", "count", BTreeKeySerializer.ZERO_OR_POSITIVE_INT, Serializer.INTEGER);
        }
        return new MapDb<Integer, Integer>(countDB, "count");
    }
}
