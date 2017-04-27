package com.liquidlabs.log.indexer.persistit;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.ThreadUtil;
import com.liquidlabs.common.util.OSUtils;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.AbstractIndexer;
import com.liquidlabs.log.indexer.LineStore;
import com.liquidlabs.log.indexer.txn.pi.PIAddToBuckets;
import com.liquidlabs.log.indexer.txn.pi.PIFindLinesForNumbers;
import com.liquidlabs.log.indexer.txn.pi.PIFindLinesForTime;
import com.liquidlabs.log.indexer.txn.pi.PIFindPosForLine;
import com.logscape.disco.indexer.Db;
import com.logscape.disco.indexer.persistit.PersistitThreadedDb;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.Value;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//import com.liquidlabs.log.disco.kv.PrestoThreadedDb;
//import com.twitter.prestissimo.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 11:25
 */
public class PILineStore implements LineStore {

    private int PI_LINE_CACHE = Integer.getInteger("pi.line.idx.size", 24 * 60 * 20);
    private static final Logger LOGGER = Logger.getLogger(PILineStore.class);
    private Db<BucketKey, Bucket> store;
    boolean isLRUEnabled = Boolean.getBoolean("pi.line.store.lru");
    boolean enoughRamFORLRU = OSUtils.getHeapGB() >= 4;

    public PILineStore( Db<BucketKey, Bucket> piStore, ScheduledExecutorService scheduler) {

        try {
            // 24 * 60 = 1440 buckets per day per file
            // 10000 files per day per server
            // 10 servers
            // 365 days at least
            // total = 365 * 100 * 10000
            // sum = 365,000,000
//            this.store = AbstractIndexer.getStoreCKey(environment + "/LI", "LI", threadLocal);
           // Db<BucketKey, Bucket> piStore = AbstractIndexer.getStoreCKey(environment + "/LI", "LI", threadLocal);

            this.store = piStore;

        } catch (Exception e) {
            e.printStackTrace();
            String threads = ThreadUtil.threadDump(null, "");
            LOGGER.warn("DUMP");
            LOGGER.warn(threads);

            throw new RuntimeException("Failed to build store", e);
        }
    }
    public static final ThreadLocal threadLocal = new ThreadLocal();

    /**
     * Make 1 minute Bucket Objects for each item - make them so that the Key uses the logFileId & timeId for the bucket
     * @param lines
     */
    public void add(List<Line> lines) {
        new PIAddToBuckets(store, lines, this).doWork();
    }
    public long size() {

        final AtomicLong results = new AtomicLong();

        try {
            Exchange exchange = getExchange();
            exchange.clear();

            Set<Integer> keys = new HashSet<Integer>();
            exchange.getKey().to(Key.BEFORE);
            while (exchange.next()) {
                Key key = exchange.getKey();
                keys.add(key.decodeInt());
            }
            for (Integer logid : keys) {
                iterateBuckets(logid, 0, Long.MAX_VALUE, new Task() {
                    @Override
                    public boolean run(Bucket bucket) {
                        results.incrementAndGet();
                        return false;
                    }
                });
            }
        } catch (Exception ex) {
            ex.printStackTrace();;
        }
        return results.get();
    }

    public List<Bucket> find(int id, long startTime, long endTime) {
        startTime = DateUtil.floorMin(startTime);
        endTime  = DateUtil.floorMin(endTime - DateUtil.MINUTE);
        final List<Bucket> results = new ArrayList<Bucket>();

        iterateBuckets(id, startTime, endTime, new Task() {
            public boolean run(Bucket bucket) {
                if (results.size() > 0) {
                    Bucket last = results.get(results.size()-1);
                    // sometimes can get stuffed up index - this is a hack to try and fix it...
                    if (bucket.firstLine < last.lastLine) {
                        last.lastLine = bucket.firstLine+1;
                    }
                    // prevent over scanning
                    if (bucket.startPos < last.startPos) {
                        bucket.startPos = last.startPos + 1024;
                    }
                }
                results.add(bucket);
                return false;
            }
        });


        return results;
    }
    public void iterateBuckets(int id, long from, long to, Task task) {

        try {
            Exchange exchange = getExchange();
            exchange.clear();
            KeyFilter filter = new KeyFilter();
            filter = filter.append(KeyFilter.simpleTerm(id));
            filter = filter.append(KeyFilter.rangeTerm(LogProperties.fromMsToMin(from), LogProperties.fromMsToMin(to)));

            exchange.append(Key.BEFORE);
            boolean finished = false;
            while (exchange.next(filter) && !finished) {
                Key key = exchange.getKey();
                Object kkey = key.decode();
                int i = key.decodeInt();

                Value value = exchange.getValue();
                Bucket b = (Bucket) value.get();
                b.setKey(new BucketKey(id, i));
                finished = task.run(b);
            }
        } catch (Exception pex) {
            LOGGER.error("Failed to iterate;" + pex, pex);
        }

    }
    public static interface Task {
        // true = finished
        boolean run(Bucket bucket);

    }

    public List<Line> linesForNumbers(LogFile logFile, int fromLine, int toLine) {
        return new PIFindLinesForNumbers(store, this).get(logFile.getId(), logFile.getStartTime(), logFile.getEndTime(), fromLine, toLine);
    }

    public long filePositionForLine(LogFile logFile, long lineNo) {
        return new PIFindPosForLine(store, this).get(logFile, lineNo);
    }

    public List<Line> linesForTime(LogFile logFile, long time, int pageSize) {
        return new PIFindLinesForTime(store, this).get(logFile, time, pageSize);
    }

    public void close() {
        try {
            if (store != null) {
                this.store.commit();
                this.store.close();
                this.store = null;
            }
        } catch (Exception e) {
            LOGGER.warn("Failure whilst closing store", e);
        }
    }

    public void remove(final int id, long startTime, long endTime) {
        startTime = DateUtil.floorMin(startTime);
        endTime  = DateUtil.floorMin(endTime + DateUtil.MINUTE);
        final AtomicInteger buckets = new AtomicInteger();
        iterateBuckets(id, startTime, endTime, new Task(){
            @Override
            public boolean run(Bucket bucket) {
                try {
                    getExchange().remove();
                    buckets.incrementAndGet();
                } catch (Exception pex) {
                    LOGGER.error("Remove Error:" + id);
                }
                return false;
            }
        });
        if(LOGGER.isDebugEnabled()) LOGGER.debug("RemovedBuckets:" + buckets);


    }

    public void sync() {
        try {
            this.store.commit();
        } catch (Exception e) {
            LOGGER.warn("Failure whilst syncing store", e);
        }
    }
    public Exchange getExchange() {
        return ((PersistitThreadedDb) this.store).getExchange();
//        return ((PrestoThreadedDb) this.store).getExchange();
    }
    public static class RetBucket {
        public Bucket value;
    }
    public Bucket get(BucketKey key) {
        try {
            final RetBucket rrr = new RetBucket();

            final HashSet<Bucket> fff =  new HashSet<Bucket>();
            iterateBuckets(key.logId(), key.getTimeMs(), key.getTimeMs(), new Task() {
                @Override
                public boolean run(Bucket bucket) {
                    rrr.value = bucket;
                    return true;
                }
            });
            return rrr.value;
        } catch (Exception pex) {
            LOGGER.error("GetFailed:!" + key, pex);
        }
        return null;
    }
    public void put(BucketKey key, Bucket value) {
        try {
//            System.out.println("PUt:" + value);
//            PrestoThreadedDb ddb = (PrestoThreadedDb) this.store;
            PersistitThreadedDb ddb = (PersistitThreadedDb) this.store;
            Exchange exchange = ddb.getExchange();
            exchange.clear();
            // NOPE!
//            exchange.append(key);
//            exchange.to(key);

            // YES! (put/get)
//            Key ekey = exchange.getKey();
//            ekey.append(key.logId()).append(key.getTimeMs());

            // NO put/get  - yes iterate
            exchange.append(key.logId()).append( key.getTimeMin());

            exchange.getValue().put(value);

            exchange.store();
        } catch (Exception ex) {
            ex.printStackTrace();;
        }

    }
}
