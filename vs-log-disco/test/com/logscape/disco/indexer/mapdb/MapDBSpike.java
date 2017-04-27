package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.file.FileUtil;
import org.mapdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/08/2013
 * Time: 17:13
 * Issues to sort:
 * - compaction is slow - so use hashing on logIds
 */
public class MapDBSpike {
    // IndexStore
    static int hosts = 1;
    static int filesPerHost = 10;
    static int linesPerFile = 5 * 100 * 1000;


    static class Key implements  Comparable<Key>{
        private int logId;
        private int lineNo;

        public Key() {}

        public Key(int logId, int lineNo) {
            this.logId = logId;
            this.lineNo = lineNo;
        }

        public int compareTo(Key o) {
            int logIdOrder = logId - o.logId;
            if (logIdOrder != 0) {
                return logIdOrder;
            }
            return lineNo - o.lineNo;
        }

        public int hashCode() {
            return Integer.valueOf(logId + lineNo).hashCode();
        }

        public boolean equals(Object obj) {
            Key other = (Key) obj;
            return this.logId == other.logId && this.lineNo == other.lineNo;
        }
    }

    static class Db{

        private final DB db;

        public Db(String parentDir) {

            db = DBMaker.newFileDB(new java.io.File(parentDir, "DB_ID"))
                    .transactionDisable()
                    .cacheWeakRefEnable().make();

            BTreeMap<Long, String> lines = db.createTreeMap("lines").nodeSize(120).keySerializer(BTreeKeySerializer.TUPLE2).make();
        }

        public void put(Key key, String s) {
            Fun.Tuple2<Integer, Integer> kk = Fun.t2(key.logId, key.lineNo);
            db.getTreeMap("lines").put(kk, s);
        }

        public ConcurrentNavigableMap<Object, Object> forLogId(int id) {
            return db.getTreeMap("lines").subMap(Fun.t2(id,0), Fun.t2(id,Integer.MAX_VALUE));
        }

        public void remove(Key key) {
            db.getTreeMap("lines").remove(key);
        }

        public int size() {
            return db.getTreeMap("lines").size();
        }

        public void compact() {
            db.compact();
        }

        public void commit() {
            db.commit();
        }
    }
    static class LogWriter implements Runnable{

        private final int logId;
        private int lineNo;
        private Db db;

        public LogWriter(int logId,Db db){
            this.logId = logId;
            this.db = db;
        }


        @Override
        public void run() {
            try {
                System.out.println("Starting:" + logId);
                while(lineNo < linesPerFile) {
                    db.put(new Key(logId, lineNo++), "foo,blah,,jkdflkajsdfl,k-" + lineNo);
                    if (lineNo % (10 * 1000) == 0) {
                        db.commit();
                        System.out.println("Sync:" + lineNo);
                    }
                }
                System.out.println("Finished:" + logId);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String dir = "./vs-log/build/TEST_DB";
        FileUtil.deleteDir(new File(dir));
        new File(dir).mkdirs();

        System.out.println("TotalFiles:" +  hosts * filesPerHost);

        final Db db = new Db(dir);
        List<LogWriter> writers = new ArrayList<LogWriter>();
        for (int file = 0; file < hosts * filesPerHost; file++) {
            writers.add(new LogWriter(file, db));
        }

        ExecutorService executorService = com.liquidlabs.common.concurrent.ExecutorService.newFixedThreadPool(10, "Writer");
        for (LogWriter writer : writers) {
            executorService.submit(writer);
        }
        executorService.shutdown();
        try {
            System.out.println("Waiting");
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        System.out.println("TotalSize:" + db.size());
        executorService = com.liquidlabs.common.concurrent.ExecutorService.newFixedThreadPool(10, "Writer");

        // iterate 1 file
        testPerf("Iterate", executorService, new Runnable() {
            @Override
            public void run() {
                ConcurrentNavigableMap<Object, Object> valuesForFile = db.forLogId(3);
                int items = 0;
                long start = System.currentTimeMillis();
                for (Object o : valuesForFile.keySet()) {
                    valuesForFile.get(o);
                    items++;
                }
                long end = System.currentTimeMillis();
                double secs = (end-start)/1000.0;
                System.out.println("Iterate Items:" + items + " RatePerSec:" + ((double)items)/secs);
            }
        });
        System.out.println("0) DB_SIZE MB:" + FileUtil.getMEGABYTES(new File(dir, "DB_ID.p").length()));

        testPerf("Remove-1", executorService, new Runnable() {
            @Override
            public void run() {
                int itemsBefore = db.size();
                ConcurrentNavigableMap<Object, Object> valuesForFile = db.forLogId(1);
                valuesForFile.clear();
                System.out.println("Remove Before:" + itemsBefore + " After:" + db.size());
            }
        });
        System.out.println("1) DB_SIZE MB:" + FileUtil.getMEGABYTES(new File(dir, "DB_ID.p").length()));

        testPerf("Compact", executorService, new Runnable() {
            @Override
            public void run() {
                db.compact();
            }
        });

        System.out.println("1.1) DB_SIZE MB:" + FileUtil.getMEGABYTES(new File(dir, "DB_ID.p").length()));

        System.out.println("TotalSize Items:" + db.size());

        testPerf("Remove-2", executorService, new Runnable() {
            @Override
            public void run() {
                ConcurrentNavigableMap<Object, Object> valuesForFile = db.forLogId(2);
                valuesForFile.clear();
            }
        });

        testPerf("Compact-2", executorService, new Runnable() {
            @Override
            public void run() {
                db.compact();
            }
        });
        System.out.println("2) DB_SIZE MB :" + FileUtil.getMEGABYTES(new File(dir, "DB_ID.p").length()));
        System.out.println("TotalSize Items:" + db.size());

    }

    private static void testPerf(String task, ExecutorService executorService, Runnable iterateFile) {
        Future<?> submit = executorService.submit(iterateFile);
        try {
            long start = System.currentTimeMillis();
            Object o = submit.get(10, TimeUnit.MINUTES);
            long end = System.currentTimeMillis();
            System.out.println(task + " Elapsed:" + (end - start));
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ExecutionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (TimeoutException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
