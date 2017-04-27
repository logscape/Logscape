package com.liquidlabs.log;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.indexer.persistit.PIIndexer;
import com.logscape.disco.DiscoProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 25/07/2013
 * Time: 13:57
 * To change this template use File | Settings | File Templates.
 */
public class DBDump {

    static {
        System.setProperty("log.db.readonly", "true");
    }

    	        static AtomicInteger lines = new AtomicInteger();
    	        static AtomicLong bytes = new AtomicLong();
    	        static AtomicLong totBytes = new AtomicLong();
    	        static AtomicLong totFiles = new AtomicLong();

    public static void main(String[] args) {

        try {
            if (args.length == 0) {
                    args = new String[] { "/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape" };
//                  args = new String[] { "D:/work/LOGSCAPE/TRUNK/master/build/logscape"};
            }

            //System.out.println("1" +  getMB(new AtomicLong(1234)));

            if (args.length == 0) {
                    System.out.println("Using Default LS_HOME:'../'");
                    return;
            } else {
                    System.out.println("Reading DBDir LS_HOME:" + args[0]);
            }

            String dir = args[0]  + "/" + DiscoProperties.getEventsDB();
            String kvDir = args[0] + "/" + LogProperties.getKVIndexDB();
            System.out.println("Events:" +dir);
            System.out.println("KvIndex:" +kvDir);
            System.setProperty("kv.env",kvDir);

            System.setProperty("log.db.readonly", "true");
            final Indexer indexer = new PIIndexer(dir,null);


            indexer.indexedFiles(0, Long.MAX_VALUE, true, new LogFileOps.FilterCallback() {
                    public boolean accept(LogFile logFile) {
                       bytes.set(0);
                        lines.set(0);
                        bytes.addAndGet(logFile.getPos());
                        lines.addAndGet(logFile.getLineCount());

                        totBytes.addAndGet(bytes.get());
                        totFiles.incrementAndGet();

                        System.out.println(logFile.toString());
                        lines.set(0);
                        bytes.set(0);

                        return false;
                    }
            });

            System.out.println("TOTAL MB:\t" + getMB(totBytes)  );
            System.out.println("TOTAL Files:\t" + totFiles.get());

            if (args.length > 1) {
                    System.out.println("Going to show info on MatchingFiles:" + args[1]);
                final String[] finalArgs = args;
                final List<LogFile> indexedFiles = new ArrayList<LogFile>();
                indexer.indexedFiles(new LogFileOps.FilterCallback(){

                        @Override
                        public boolean accept(LogFile logFile) {
                            if (logFile.getFileName().contains(finalArgs[0])) indexedFiles.add(logFile);
                            return false;
                        }
                    });
                    for (LogFile logFile : indexedFiles) {
                            System.out.println(">FILE:" + logFile.toString());
                            List<Bucket> buckets = indexer.find(logFile.getFileName(), 0, System.currentTimeMillis());
                            for (Bucket bucket : buckets) {
                                    System.out.println(bucket);
                            }
                    }
            }
            indexer.close();

        } catch (Throwable t) {
                t.printStackTrace();
        }
    System.exit(1);
    }

	        private static String getMB(AtomicLong bytes) {
	                return String.format("%.3f", ((double) bytes.get())/ (1024.0 * 1024.0));
	        }
}
