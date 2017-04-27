package com.liquidlabs.log.space;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.util.notifier.FSNotifierFactory;
import com.liquidlabs.log.util.notifier.PathNotifier;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks dir/mod state change for the WatchDir/DataSource
 */
public class WatchDirectoryTracker {
    private final static Logger LOGGER = Logger.getLogger(WatchDirectoryTracker.class);
    public static final Integer TRACKER_DELAY = Integer.getInteger("track.live.ms", 30 * 1000);
    private Map<String, PathNotifier> pps = new HashMap<>();
    static private int maxMinutesOld = LogProperties.getMaxModifiedMinutes();
    static private boolean doFileCount = LogProperties.isVisitorDoingFileCount();
    static private int fileCountMaxDays = LogProperties.visitorFileCountMaxDays();


    private WatchDirectory watchDirectory;

    public WatchDirectoryTracker(WatchDirectory watchDirectory) {
        this.watchDirectory = watchDirectory;
    }

    @Override
    public String toString() {
        if (lastModifiedDirs == null) lastModifiedDirs = new ConcurrentHashMap<String, Long>();
        return WatchDirectoryTracker.class.getSimpleName() + " " + this.lastModifiedDirs.keySet().toString().replace(",","\n<br>&nbsp;&nbsp;&nbsp;");
    }

    public List<File> scanDirForLastMod() throws InterruptedException {
        List<File> dirs = listDirs();
        for (File dir : dirs) {
            assembleLastModMap(dir, watchDirectory);
        }
        return dirs;
    }

    public transient Map<String, Long> lastModifiedDirs = new ConcurrentHashMap<String, Long>();
    transient Map<String, Integer> lastModifiedDirsCount = new ConcurrentHashMap<String, Integer>();


    synchronized final private void assembleLastModMap(File dir, final WatchDirectory wd) throws InterruptedException {
        if (isTopOfHour() || Thread.currentThread().isInterrupted()) {
            // dont do visiting at the very top of the hour because files may be rolling
            return;
        }

        String path = FileUtil.getPath(dir.getAbsolutePath());
        if (lastModifiedDirs.containsKey(path)) return;
        else {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug(this.watchDirectory.getTags() + " DIR ADDED:" + path);
            lastModifiedDirs.put(path, 0L);
        }
    }

    public boolean isDirectoryModified(String dir, File baseDir, long ourLastMod, long lastModified) {
        boolean isModified = ourLastMod != lastModified;
        if (isModified) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug(this.watchDirectory.getTags() + " DIR CHANGED:" + dir + " LastMod:" + new DateTime(lastModified) + " OurLastMod:" + new DateTime(ourLastMod));
            this.lastModifiedDirs.put(dir, lastModified);
        }

        // only run file count check on dirs less than a certain age - and we check in the first 15 mins of the hour
        if (!isModified && doFileCount && lastModified > System.currentTimeMillis() - DateUtil.DAY * fileCountMaxDays && new DateTime().getMinuteOfHour() < 15) {
            final AtomicInteger count = new AtomicInteger();
            final AtomicInteger fileCount = countFilesInDir(baseDir, count);
            Integer knownFileCount = this.lastModifiedDirsCount.get(dir);
            boolean isNowModified = knownFileCount == null || knownFileCount != fileCount.get();
            if (isNowModified) {
                if (knownFileCount != null && knownFileCount.intValue() > 0) {
                    LOGGER.debug("Detected DIR Count change:" + dir + " count:" + fileCount.get());
                    isModified = true;
                }
            }
            this.lastModifiedDirsCount.put(FileUtil.getPath(dir), fileCount.get());
        }
        // if it changed in the last hour the give it a scan
        // there was a broad assumption that we are tracking stuff when files are added or removed... however
        // the last mod of a dir doesnt reflect that a file was modified....
        if (!isModified) isModified = lastModified > System.currentTimeMillis() - TRACKER_DELAY;
        if (isModified && LOGGER.isDebugEnabled())
            LOGGER.debug(" Detected DIR CHANGED:" + dir + " TimeDelta:" + (lastModified - ourLastMod) + " Our:" + new DateTime(ourLastMod));
        return isModified;
    }

    private AtomicInteger countFilesInDir(File baseDir, final AtomicInteger count) {
        final AtomicInteger fileCount = new AtomicInteger();
        baseDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                fileCount.incrementAndGet();
                // this can spin the cpu when we get loads of files
                try {
                    if (count.incrementAndGet() > 10000) Thread.sleep(1);
                } catch (InterruptedException iex) {
                }
                return false;
            }
        });
        return fileCount;
    }

    private boolean isDirIgnored(String baseDirPath) {
        for (String dirString : this.ignoreDirs) {
            if (baseDirPath.contains(dirString)) return true;
        }
        return false;
    }

    public static Set<String> ignoreDirs = new HashSet<String>(com.liquidlabs.common.collection.Arrays.asList("logscape/work/DB", "logscape\\work\\DB", "logscape/work/jetty", "logscape\\work\\jetty"));

    /**
     * don't grab new files while on the top of the hour - 20 seconds either side - (time based rolling is most common)
     *
     * @return
     */
    private boolean isTopOfHour() {
        int minuteOfHour = new DateTime().getMinuteOfHour();
        int secOfMinute = new DateTime().getSecondOfMinute();
        return (minuteOfHour == 0 && secOfMinute < 50) || (minuteOfHour == 59 && secOfMinute > 50);
    }


    public List<File> listDirs() {
        return listDirsCollect();
    }

    private List<File> listDirsCollect() {
        List<File> results = new ArrayList<File>();
        watchDirectory.makeDirsAbsolute();
        String[] dirs = StringUtil.splitFast(watchDirectory.getDirName(), ',');
        for (String dir : dirs) {
            try {
                long started = System.currentTimeMillis();
                //add_SERVER_DIR(dir);
                if (!dir.startsWith("!")) {
                    // handle all files with *
                    if (dir.contains("*")) {
                        try {

                            // PathPattern will allow you to recurse
                            if (dir.startsWith(".")) dir = new File(".").getAbsolutePath() + dir.substring(1);
                            dir = FileUtil.cleanDirectory(dir);
                            if (!pps.containsKey(dir))
                                pps.put(dir, FSNotifierFactory.getNotifier(dir, watchDirectory.getMaxAge()));

                            Set<File> allFiles = pps.get(dir).getDirs();
                            int count = 0;
                            for (File file : allFiles) {
                                if (isAddable(file)) {
                                    results.add(file);
                                    if (LogProperties.isForwarder && count > 1000) Thread.sleep(10);
                                }
                                count++;
                            }
                            if (count > 100 * 1000) {
                                LOGGER.warn("Massive file count on :" + dir + " source:" + this);
                            }
                        } catch (Throwable t) {
                            LOGGER.warn("DataSource Path Error:" + this + " Ex:" + t.getMessage());

                        }

                    } else {
                        if (isAddable(new File(dir))) results.add(new File(dir));
                    }
                }
                long ended = System.currentTimeMillis();
                if (ended - started > 10000 && LOGGER.isDebugEnabled()) {
                    LOGGER.debug("DataSource ScanTook:" + (ended - started) + " DIR:" + dir + " DS:" + this);
                }
            } catch (Throwable t) {
                LOGGER.error("Failed to handle DataSource Tag:" + watchDirectory.getTags() + " DIR:" + dir, t);
            }

        }

        if (results.size() > 0 && LOGGER.isDebugEnabled()) LOGGER.debug(watchDirectory.getTags() + " Added:" + results.size() + " First:" + results.get(0) + " length:" + results.size());
        return results;

    }

    private boolean isAddable(File dir) {
        String baseDirPath = FileUtil.getPath(dir);
        if (isDirIgnored(baseDirPath)) return false;

        if (baseDirPath.endsWith(LogProperties.getServiceDIRName())) return false;

        String[] hostnameFromPath = LogFile.getHostnameFromPath(baseDirPath);
        if (hostnameFromPath == null) return true;
        return watchDirectory.isHostMatch(hostnameFromPath[0]);

    }

    public Map<String, Long> lastModifiedDirs() {
        if (this.lastModifiedDirs.size() == 0) {
            try {
                scanDirForLastMod();
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }


        return lastModifiedDirs;
    }
}
