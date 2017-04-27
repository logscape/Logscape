package com.liquidlabs.log.search.summaryindex;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.facetmap.PersistentMap;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.agg.HistoManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.prevayler.Prevayler;
import org.prevayler.PrevaylerFactory;
import org.prevayler.foundation.serialization.XStreamSerializer;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stored LogFile summary information
 */
public class PersistingSummaryIndex {
    private final static Logger LOGGER = Logger.getLogger(PersistingSummaryIndex.class);

    static Map<String, QueueHandler> handlers = new ConcurrentHashMap<>();

    static public int INCR = 10;
    private static String SUMMARY = "summary.index(";

    public static boolean isWrite(String s) {
        return Action.WRITE.is(s) || Action.AUTO.is(s);
    }

    public enum Action  { AUTO, WRITE, READ, DELETE;
        String command = SUMMARY + this.name().toLowerCase() + ")";
        public boolean is(LogRequest request) {
            return is (request.query(0).sourceQuery());
        }
        public boolean is(String string) {
            return string.contains(command);
        }
    };

    String env = ".";


    public PersistingSummaryIndex(String env) {
        new File(env).mkdirs();
        this.env = env;
    }

    public static boolean isSummaryIndexAction(LogRequest request) {
        return Action.AUTO.is(request) || Action.READ.is(request) || Action.DELETE.is(request);
    }

    public void write(String queryHashWQPos, Bucket bucket) {
        getQueueHandler(queryHashWQPos).add(bucket);
    }
    public void flush(String queryHashId) {
        getQueueHandler(queryHashId).flush();
    }
    public String delete(String queryHashIde, AggSpace aggSpace, String resourceId, LogRequest request) {
        QueueHandler queueHandler = null;
        try {
            if (aggSpace != null) aggSpace.status(request.subscriber(), resourceId, 0, 0);
            queueHandler = getQueueHandler(queryHashIde).delete();
            handlers.remove(queryHashIde);
            if (aggSpace != null) aggSpace.status(request.subscriber(), resourceId, 100, 100);
            if (aggSpace != null) aggSpace.status(request.subscriber(), resourceId, -1, -1);
        } catch (Exception e) {
            LOGGER.error("DeleteFailed:" + e.toString());
        }
        return queueHandler.prevalenceDirectory;
    }
    public long handle(LogRequest request, String hostname, AggSpace aggSpace) {
        Action act = getAction(request);
        switch (act) {
            case READ:
                return search(request, hostname, aggSpace);
            case DELETE:
                delete(request.cacheKey(), aggSpace, hostname, request);
                break;

        }
        return -1;
    }

    private Action getAction(LogRequest request) {
        Action[] values = Action.values();
        for (Action value : values) {
            if (value.is(request.query(0).sourceQuery())) return value;
        }
        throw new RuntimeException("Unknown action");
    }

    public long search(LogRequest request, String resourceId, AggSpace aggSpace) {
        long lastTime = -1;
        try {
            List<Bucket> read = read(request.cacheKey(), request.getStartTimeMs(), request.getEndTimeMs());
            if (read.size() > 0) {
                aggSpace.status(request.subscriber(), resourceId, 0, 0);
                int hits = 0;
                for (Bucket bucket : read) {
                    bucket.subscriber = request.subscriber();
                    hits += bucket.hits();
                    bucket.convertFuncResults(false);
                    if (bucket.getEnd() > lastTime) lastTime = bucket.getEnd();
                }

                aggSpace.status(request.subscriber(), resourceId, hits, 100);
                aggSpace.write(read);
                aggSpace.status(request.subscriber(), resourceId, -1, -1);
            }


        } catch (Exception e) {
            LOGGER.error("SearchFailed:" + e.toString(), e);
        }
        return lastTime != -1 ? lastTime : request.getStartTimeMs();

    }


    public List<Bucket> read(String id, long from, long to) {
        long fromT = DateUtil.nearestMin(from, PersistingSummaryIndex.INCR);
        long toT  = DateUtil.nearestMin(to, PersistingSummaryIndex.INCR);
        long bump = 60 * PersistingSummaryIndex.INCR * 1000;
        ArrayList<Bucket> buckets = new ArrayList<>();
        PersistentMap<Long, QueueHandler.BloomBucket> map = getQueueHandler(id).map;
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Reading:" + env + "-" + id+ " from:" + DateUtil.shortDateTimeFormat.print(from));

        for (long i = fromT; i < toT; i += bump) {

            QueueHandler.BloomBucket bucket = map.get(i);
            if (bucket != null) {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("ReadBucket:" + env + "- ADD_ "  + id + " " +  DateUtil.shortDateTimeFormat.print(i));
                buckets.add(bucket.bucket);
            } else {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("ReadBucket:" + env + "- MISS "  + id + " " +  DateUtil.shortDateTimeFormat.print(i));

            }
        }
        return buckets;
    }
    synchronized private QueueHandler getQueueHandler(String logId) {
        if (!handlers.containsKey(logId)) {
            handlers.put(logId, new QueueHandler(env, logId));
        }
        return handlers.get(logId);
    }


    public static class QueueHandler {
        private final String prevalenceDirectory;
        Prevayler prevayler;
        PersistentMap<Long,BloomBucket> map;
        private String id;

        public QueueHandler(String env, String id) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Created:" + env + "-" + id);
            id = id.replace("*","o").replace(" ","");
            PrevaylerFactory factory = new PrevaylerFactory();
            if (Boolean.getBoolean("sidx.xstream")) {
                factory.configureSnapshotSerializer(new XStreamSerializer());
                factory.configureJournalSerializer(new XStreamSerializer());
            }
            factory.configurePrevalentSystem(new ConcurrentHashMap());
            prevalenceDirectory = env + "/" + id + ".sidx";
            factory.configurePrevalenceDirectory(prevalenceDirectory);
            try {
                prevayler = factory.create();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            map = new PersistentMap<>(prevayler);
            this.id = id;
        }

        public void flush() {
            try {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("flush():" + "-" + id);
                prevayler.takeSnapshot();
                removeOldJournals(prevalenceDirectory);


            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        private void removeOldJournals(String prevalenceDirectory) {
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


        HistoManager histoManager = new HistoManager();


        // TODO - remove syncronised
        synchronized public void add(Bucket bucket) {

            try {
                // neeed to ensure that the bucket is old enough to be buckets
                if (DateUtil.nearestMin(bucket.getEnd(), INCR) == DateUtil.nearestMin(System.currentTimeMillis(), INCR)) return;

                // TODO: ignore buckets that are less than the window old
                long line = DateUtil.nearestMin(bucket.getStart(), INCR);
                BloomBucket bucket1 = map.get(line);
                if (bucket1 != null) {
                    // TODO: for some reason buckets in the same source search are written multiple times - so allow multiple updates writes ETC from the same source search
                    if (bucket1.bucket.subscriber().equals(bucket.subscriber()) || !bucket1.bloomFilter.mightContain(bucket.getFilePath()) ) {
                        histoManager.handle(bucket1.bucket, bucket, 1, false, false);
                        bucket1.bloomFilter.put(bucket.getFilePath());
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("update():" + "-" + id + " " + bucket.getFilePath() + " id:" + bucket.id() + " h:" + bucket.hits() + " " + bucket.toStringTime());
                    } else {
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("skip():" + "-" + id + " " + bucket.getFilePath() + " id:" + bucket.id() + " h:" + bucket.hits() + " " + bucket.toStringTime());
                    }
                } else {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("created():" + "-" + id + " " + bucket.getFilePath() + " id:" + bucket.id() + " h:" + bucket.hits() + " " + bucket.toStringTime());
                    bucket1 = new BloomBucket(bucket.copy());
                    bucket1.bloomFilter.put(bucket.getFilePath());
                    bucket1.bucket.resetResults();
                }
                map.put(line, bucket1);
            } catch (Throwable t) {
                LOGGER.warn("WRITE Failed:" + t.toString());
            }
        }

        public QueueHandler delete() {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("delete():" + "-" + id);
            try {
                prevayler.close();
            } catch (IOException e) {
                LOGGER.error(e.toString());
            }
            FileUtil.deleteDir(new File(prevalenceDirectory));
            return this;
        }
        public static class BloomBucket implements Serializable{
            BloomFilter<CharSequence> bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1000, Double.valueOf(System.getProperty("sidx.bloom.prob","0.001")));
            Bucket bucket;
            String timestamp;

            public BloomBucket(Bucket copy) {
                this.bucket = copy;
                this.bucket.subscriber = "";
                this.timestamp = DateUtil.shortDateTimeFormat1.print(copy.getStart());
            }
        }
    }

}
