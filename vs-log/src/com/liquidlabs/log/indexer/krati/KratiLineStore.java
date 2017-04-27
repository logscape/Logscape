package com.liquidlabs.log.indexer.krati;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.LineStore;
import com.liquidlabs.log.indexer.txn.krati.*;
import krati.core.StoreConfig;
import krati.store.DataStore;
import krati.store.DynamicDataStore;
import krati.store.SafeDataStoreHandler;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 11:25
 */
public class KratiLineStore extends AbstractKratiStore implements LineStore {
    private static final Logger LOGGER = Logger.getLogger(KratiLineStore.class);
    public static final int MINUTES_PER_DAY = 1440;
    private DataStore<byte[], byte[]> store;
    private ScheduledExecutorService scheduler;
    int startingSizeHash = Integer.getInteger("lineStore.size", 2 /* days */ * 100 /* files */ * MINUTES_PER_DAY);

    public KratiLineStore(String environment, ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;

        try {
            // 24 * 60 = 1440 minute buckets per day per file
            // 1000 files per day
            // 30 days at least
            // total = 30 * 1000 * 1440
            // sum =  43,200,000
            //StoreConfig _config = new StoreConfig(new File(environment + "/LINE"), 30 * 1000 * 1440);
            StoreConfig _config = new StoreConfig(new File(environment + "/LINE"),startingSizeHash );

            //_config.setSegmentFactory(new MemorySegmentFactory());
            _config.setSegmentFactory(KratiIndexer.getChannelFactory());
            _config.setSegmentFileSizeMB(8);
            _config.setDataHandler(new SafeDataStoreHandler());

            store = new DynamicDataStore(_config);

        } catch (Exception e) {
            throw new RuntimeException("Failed to build store", e);
        }
    }

    /**
     * Make 1 minute Bucket Objects for each item - make them so that the Key uses the logFileId & timeId for the bucket
     * @param lines
     */

    public void add(List<Line> lines) {
        try {
            if (store != null) new AddToBuckets(store,lines).doWork();
        } catch (Exception e) {
        }
    }

    public List<Bucket> find(int id, long startTime, long endTime) {
        return new FindBuckets(store).find(id, startTime, endTime);
    }

    public List<Line> linesForNumbers(LogFile logFile, int fromLine, int toLine) {
        return new FindLinesForNumbers(store).get(logFile.getId(), logFile.getStartTime(), logFile.getEndTime(), fromLine, toLine);
    }

    public long filePositionForLine(LogFile logFile, long lineNo) {
        return new FindPosForLine(store).get(logFile, lineNo);
    }

    public List<Line> linesForTime(LogFile logFile, long from, int pageSize) {
        return new FindLinesForTime(store).get(logFile, from, pageSize);

    }

    public void remove(int id, long startTime, long endTime) {
        LOGGER.info("RemoveLines:" + id + ">>");
        if (store != null)  {
            RemoveLines task = new RemoveLines(store);
            task.go(id, startTime, endTime);
            LOGGER.info("RemoveLines:" + id + "<<" + " count:" + task.count());
        }
        sync();
    }




}
