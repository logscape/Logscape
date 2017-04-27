package com.logscape.disco.indexer;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.indexer.flatfile.BArrayCreator;
import com.logscape.disco.indexer.flatfile.DataStreamCreator;
import com.logscape.disco.indexer.flatfile.BICreator;
import com.logscape.disco.indexer.flatfile.FFCreator;
import com.logscape.disco.indexer.mapdb.MapDbCreator;
import com.logscape.disco.indexer.persistit.PersistitCreator;
import org.apache.log4j.Logger;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 06/01/2015
 * Time: 13:45
 * To change this template use File | Settings | File Templates.
 */
public class KvIndexFactory {
    private static final Logger LOGGER = Logger.getLogger(KvIndexFeed.class);

    public enum IMPLS { PERSISTIT, MAPDB, FF1, BI1, DATASTREAM, BARRAY};

    public static boolean isDisabled = System.getProperty("test.debug.disable.discovery","false").equals("true");
    static public boolean readOnly = Boolean.getBoolean("log.db.readonly");

    static public String env = System.getProperty("kv.env", DiscoProperties.getKVIndexDB());

    static String defaultStoreType = System.getProperty("kvi.db.impl", IMPLS.FF1.name());

    static Map<IMPLS, Creator> creators = new HashMap<IMPLS, Creator>();

    static {
        KvIndexFactory.registerImpl(PersistitCreator.INST, new PersistitCreator());
        KvIndexFactory.registerImpl(MapDbCreator.INST, new MapDbCreator());
        KvIndexFactory.registerImpl(FFCreator.INST, new FFCreator());
        KvIndexFactory.registerImpl(BICreator.INST, new BICreator());
        KvIndexFactory.registerImpl(BArrayCreator.INST, new BArrayCreator());
        KvIndexFactory.registerImpl(DataStreamCreator.INST, new DataStreamCreator());
        scheduleCompaction();
        new KvIndexAdmin();
    }

    public static interface Creator {
        IndexFeed get();
        void close();
    }

    static public void registerImpl(IMPLS key, Creator impl){
        creators.put(key, impl);
    }

    public static IndexFeed get() {
        return get(IMPLS.valueOf(defaultStoreType));
    }

    private static IndexFeed firstOne;

    public static IndexFeed get(IMPLS impl) {

        FileUtil.mkdir(env);

        // if a LogForwarder then return a stub class
        if (DiscoProperties.isForwarder || isDisabled) return new StubIndexFeed();
        Creator creator = creators.get(impl);
        if (creator != null) {
            IndexFeed result = creator.get();
            if (firstOne == null) {
                firstOne = result;
            }
            return result;
        }
        LOGGER.error("Didnt find:" + defaultStoreType);
        throw  new RuntimeException("Failed to find:" + defaultStoreType);
    }

    public static void close() {
        Creator creator = creators.get(defaultStoreType);
        creator.close();
    }

    static private void scheduleCompaction() {
        final DateMidnight dateMidnight = new DateMidnight().plusDays(1);
        final DateTime oneAm = new DateTime(dateMidnight).plusHours(1);
        final DateTime now = new DateTime();
        final long initial = new Duration(now, oneAm).getStandardHours() + 1;
        LOGGER.info("Initial compaction-schedule:" + initial);
        ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Compacting");
                    if (firstOne != null) firstOne.compact();
                } catch (Throwable t) {
                    LOGGER.warn("CompactFailed",t);
                }
            }
        }, initial, 24, TimeUnit.HOURS);
    }

}
