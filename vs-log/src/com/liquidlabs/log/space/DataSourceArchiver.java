package com.liquidlabs.log.space;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 14/10/14
 * Time: 15:11
 * To change this template use File | Settings | File Templates.
 */
public class DataSourceArchiver {
    static int archiveCopyK = Integer.getInteger("datasource.achive.stream.bytes", 10);

    static final Logger LOGGER = Logger.getLogger(DataSourceArchiver.class);
    ScheduledExecutorService scheduler = com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool("services");
    private Map<String, WatchDirectory> watchDirSet;
    private Map<String, WatchDirectory> fwdrWatchDirSet;

    private Indexer indexer;
    Map<String, Task> tasks = new HashMap<String, Task>();

    public DataSourceArchiver(Map<String, WatchDirectory> watchDirSet, Map<String, WatchDirectory> fwdrWatchDirSet, Indexer indexer) {
        this.watchDirSet = watchDirSet;
        this.fwdrWatchDirSet = fwdrWatchDirSet;
        this.indexer = indexer;
        tasks.put(WatchDirectory.ARCHIVE_TASKS.Delete.name(), new DeleteTask());
        tasks.put(WatchDirectory.ARCHIVE_TASKS.Snappy_Compress.name(), new Snappy_Compress_Task());
        tasks.put(WatchDirectory.ARCHIVE_TASKS.Lz4_Compress.name(), new LZ_Compress_Task());
        tasks.put(WatchDirectory.ARCHIVE_TASKS.Gzip_Compress.name(), new GZip_Compress_Task());

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                processArchiveRules();
            }

        }, (24 - new DateTime().getHourOfDay())+1, 24, TimeUnit.HOURS);

    }

    public String run() {
        StringBuilder results = new StringBuilder();
        results.append("isForwarder:" + LogProperties.isForwarder);
        processArchiveRules();
        results.append("ActionsTriggered:" + this.actionsTriggered);
        return results.toString();
    }

    private int actionsTriggered;
    synchronized private void processArchiveRules() {
        actionsTriggered = 0;
        final boolean isForwarder = LogProperties.isForwarder;
        final List<WatchDirectory> allSources = new ArrayList<WatchDirectory>(watchDirSet.values());
        allSources.addAll(fwdrWatchDirSet.values());

        LOGGER.info("START Archive Actions, sources:" + allSources.size());
        long start = System.currentTimeMillis();

        this.indexer.indexedFiles(new LogFileOps.FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {

                if (logFile.getEndTime() == 0) return false;
                printProgress();
                for (WatchDirectory watchDir : allSources) {
                    if (watchDir.isWatching(logFile)) {
                        try {
                            String ruleString = watchDir.archivingRules();
                            if (watchDir.archivingRules().length() > 0 && watchDir.archivingRules().contains(",")) {
                                String[] rules = watchDir.archivingRules().split(",");

                                if (isForwarder) {
                                    int forwarderPeriod = Integer.parseInt(rules[0]);
                                    if (forwarderPeriod > 1 && logFile.getAgeInDays() > forwarderPeriod) {
                                        if (LOGGER.isDebugEnabled()) LOGGER.debug("File:" + logFile + " Age:" + logFile.getAgeInDays() + " End:" + logFile.getEndTime() + " Rule:" + watchDir.archivingRules);
                                        tasks.get(WatchDirectory.ARCHIVE_TASKS.Delete.name()).run(new File(logFile.getFileName()), indexer);
                                        actionsTriggered++;
                                    }
                                } else {
                                    int indexerPeriod = Integer.parseInt(rules[1]);
                                    if (indexerPeriod > 1 && logFile.getAgeInDays() > indexerPeriod) {
                                        Task task = tasks.get(rules[2]);
                                        if (LOGGER.isDebugEnabled()) LOGGER.debug("File:" + logFile + " Age:" + logFile.getAgeInDays() + " End:" + logFile.getEndTime() + " Rule:" + watchDir.archivingRules + " Task" + task);
                                        task.run(new File(logFile.getFileName()), indexer);
                                        actionsTriggered++;
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            if (!t.toString().contains("java.lang.NumberFormatException")) {
                                LOGGER.warn("ArchiveActionFailed:" + logFile.getFileName() + ";" + logFile.getTags() + " " + t.toString());
                            }
                        }
                        return false;
                    }
                }
                return false;
            }
        });
        LOGGER.info("COMPLETE Archive Actions, sources:" + allSources.size() + " Actions:" + actionsTriggered + " ElapsedMs:" + (System.currentTimeMillis() - start));
    }

    private interface Task {
        void run(File file, Indexer indexer) throws Exception;
    }
    private long lastPrinted = System.currentTimeMillis();
    public void printProgress() {
        if (System.currentTimeMillis() - lastPrinted > DateUtil.MINUTE) {
            LOGGER.info("Processed:" + actionsTriggered);
            lastPrinted = System.currentTimeMillis();
        }

    }
    static class  DeleteTask implements Task {
        @Override
        public void run(File file, Indexer indexer) throws Exception {
            file.delete();
            indexer.removeFromIndex(file.getAbsolutePath());
        }
    }
    static class  Snappy_Compress_Task implements Task {
        @Override
        public void run(File file, Indexer indexer) throws Exception {
            if (file.getName().endsWith(".snap")) return;
            if (!file.exists()) indexer.removeFromIndex(file.getAbsolutePath());
            if (!file.canWrite() && !file.getParentFile().canWrite()) return;

            File outFile = new File(file.getParent(), file.getName() + ".snap");
            if (outFile.exists()) return;

            FileInputStream fis = new FileInputStream(file);
            OutputStream fos = new SnappyFramedOutputStream(new FileOutputStream(outFile));
            byte[] array = new byte[archiveCopyK * 1024];

            while (fis.available() > 0) {
                int read = fis.read(array);
                fos.write(array,0,read);
            }
            fos.close();
            fis.close();
            indexer.rolled(file.getAbsolutePath(), outFile.getAbsolutePath());
            file.delete();
        }
    }
    static class  LZ_Compress_Task implements Task {
        @Override
        public void run(File file, Indexer indexer) throws Exception {
            if (file.getName().endsWith(".lz4")) return;
            if (!file.exists()) indexer.removeFromIndex(file.getAbsolutePath());
            if (!file.canWrite() && !file.getParentFile().canWrite()) return;

            File outFile = new File(file.getParent(), file.getName() + ".lz4");
            if (outFile.exists()) return;

            FileInputStream fis = new FileInputStream(file);
            final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            LZ4BlockOutputStream fos = new LZ4BlockOutputStream(new FileOutputStream(outFile), 64, lz4Factory.fastCompressor());


            byte[] array = new byte[archiveCopyK * 1024];

            while (fis.available() > 0) {
                int read = fis.read(array);
                fos.write(array,0,read);
            }
            fos.close();
            fis.close();
            indexer.rolled(file.getAbsolutePath(), outFile.getAbsolutePath());
            file.delete();
        }
    }
    static class  GZip_Compress_Task implements Task {
        @Override
        public void run(File file, Indexer indexer) throws Exception {
            if (file.getName().endsWith(".gz")) return;
            if (!file.exists()) indexer.removeFromIndex(file.getAbsolutePath());
            if (!file.canWrite() && !file.getParentFile().canWrite()) return;

            File outFile = new File(file.getParent(), file.getName() + ".gz");
            if (outFile.exists()) return;

            FileInputStream fis = new FileInputStream(file);
            OutputStream fos = new GZIPOutputStream(new FileOutputStream(outFile));
            byte[] array = new byte[archiveCopyK * 1024];

            while (fis.available() > 0) {
                int read = fis.read(array);
                fos.write(array,0,read);
            }
            fos.close();
            fis.close();
            indexer.rolled(file.getAbsolutePath(), outFile.getAbsolutePath());
            file.delete();
        }
    }
}