package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.collection.CompactCharSequence;
import com.liquidlabs.common.collection.PriorityQueue;
import com.liquidlabs.common.collection.Queue;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.space.WatchDirectoryTracker;
import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileFilter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class WatchVisitor implements LifeCycle {
    static final Logger LOGGER = Logger.getLogger(WatchVisitor.class);

    private final LoggingEventMonitor eventMonitor = new LoggingEventMonitor();
    private static final String DOT = ".";

    public static Set<String> ignoreExtensionsSet = new HashSet<String>(Arrays.asList(".zip",".swp",".tar",".swf",".seg",".js",".gif",".png",".jpg",".tmp",".ser",".xstreamsnapshot",".xstreamjournal",".exe",".cab",".dll",".lib",".jar",".meta",".war",".so",".class",".jdb",".bin",".hprof", ".replay", ".class",".idx",".DS_Store"));

    private int updateIntervalMin = Integer.getInteger("visit.interval", 10);

    //	Set<String> stoppedIndexing = new CopyOnWriteArraySet<String>();
//	// need some kind of bad-files cache to reduce CPU overhead
    Map<Integer, Set<CompactCharSequence>> currentlyHandledFiles = new ConcurrentHashMap<Integer, Set<CompactCharSequence>>();
    Set<String> serverDirs = new ConcurrentHashSet<String>();

    // make sure we reuse the same pool
    ScheduledExecutorService visitorScheduler = ExecutorService.newScheduledThreadPool(1, new NamingThreadFactory("watch-visit"));

    Queue<WatchVisitor.Pair> queue = new PriorityQueue<WatchVisitor.Pair>(LogProperties.highPriorityTailerQueueSize(), LogProperties.lowPriorityTailerQueueSize());

    final Map<String, WatchDirectory> watchDirSet;
    final Map<String, WatchDirectory> fwdWatchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
    java.util.concurrent.ThreadPoolExecutor oneThread = (ThreadPoolExecutor) ExecutorService.newFixedThreadPool(1, "visitor-feed");

    private final Callback callback;
    private ScheduledFuture<?> tailerFuture;
    LifeCycle.State lifecycle = LifeCycle.State.STARTED;


    public List<CompactCharSequence> currentlyHandledFiles() {
        List<CompactCharSequence> all = new ArrayList<CompactCharSequence>();
        for(Set<CompactCharSequence> files : currentlyHandledFiles.values()) {
            all.addAll(files);
        }
        return all;
    }

    public void stoppedWatching(String fileName) {
        addFile(fileName);

    }

    public List<WatchDirectory> getFwdSources(String dsId) {
        List<WatchDirectory> results = new ArrayList<WatchDirectory>();
        for (String key : fwdWatchDirSet.keySet()) {
            if (key.contains(dsId)) {
                results.add(fwdWatchDirSet.get(key));
            }
        }
        return results;
    }
    public Map<String, WatchDirectory> getFwdWatchDirs() {
        return this.fwdWatchDirSet;
    }

    public Map<String, Long> lastModifiedDirs(String filter) {
        HashMap<String, Long> results = new HashMap<String, Long>();
        Collection<WatchDirectory> fullSetOfWatchDirs = getFullSetOfWatchDirs();
        for (WatchDirectory dir : fullSetOfWatchDirs) {
            Map<String, Long> modd = dir.getTracker().lastModifiedDirs();
            for (String s : modd.keySet()) {
                if (dir.getTags().contains(filter) || s.contains(filter)) results.put(dir.id() + "/" + dir.getTags() + "/" + s, modd.get(s));
            }
        }
        return results;
    }


    // used by the client to be called when a file is being watched
    public interface Callback {
        void startTailing(WatchDirectory watch, String file);
        boolean isTailingFile(String fullFilePath, File file);
        boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException;
    }

    public WatchVisitor(final Callback callback, Map<String, WatchDirectory> watchDirSet) {
        this.callback = callback;
        this.watchDirSet = watchDirSet;

        scheduleVisitor();


        this.visitorScheduler.scheduleAtFixedRate(new Runnable() {

            public void run() {
                reset();
            }

        }, 15, updateIntervalMin, TimeUnit.MINUTES);

        this.visitorScheduler.scheduleAtFixedRate(new Runnable() {

            public void run() {
                checkForServerDirChange();
            }

        }, 30, Integer.getInteger("visitor.server.dir.mod.interval.sec",30), TimeUnit.SECONDS);


        startConsumerThread();
        /**
         * Do this so we get data in quicker at first boot. i.e. there might be forwarder directories being created
         */
        for (int i = 2; i < 10; i+=2) {
            this.visitorScheduler.schedule(new Runnable() {
                public void run() {
                    LOGGER.info("Building LastModMap:");
                    try {
                        buildLastModMap();
                    } catch (InterruptedException e) {
                    }
                }
            }, i, TimeUnit.MINUTES);
        }


    }

    void checkForServerDirChange() {
        try {
            Collection<WatchDirectory> values = this.watchDirSet.values();
            for (WatchDirectory value : values) {
                String dirName = value.getDirName();
                if (dirName.contains(LogProperties.getLogServerDIR())) {
                    if (dirName.contains(",")) dirName = dirName.substring(0, dirName.indexOf(","));
                    if (!serverDirs.contains(dirName) || new File(dirName).lastModified() >  new DateTime().minusMinutes(2).getMillis()) {
                        serverDirs.add(dirName);
                        fwdWatchDirSet.clear();
                    }
                }
            }

            String defaultServerDir = new File("work", LogProperties.getLogServerDIR()).getPath();
            if (!serverDirs.contains(defaultServerDir) || new File(defaultServerDir).lastModified() > new DateTime().minusMinutes(2).getMillis()  ){
                serverDirs.add(defaultServerDir);
                fwdWatchDirSet.clear();

            }
            if (fwdWatchDirSet.isEmpty()) {
                addAllForwarderDirs();
            }
        } catch (Throwable t) {
            LOGGER.warn(t);
        }

    }

    private void scheduleVisitor() {
        tailerFuture = this.visitorScheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                searchForMatchingFiles();
            }}, LogProperties.getAddWatchFileDelay(), LogProperties.getAddWatchFileDelay(), TimeUnit.SECONDS);
    }

    private void startConsumerThread() {
        oneThread.submit(createRunnable());

    }

    private Runnable createRunnable() {
        return new Runnable() {
            @Override
            public void run() {
                if(tailerFuture == null || lifecycle == State.STOPPED) return;

                try {
                    Pair newFileToWatch = queue.take(LogProperties.getStartTailingQueueWaitMs());
                    if (newFileToWatch != null) {
                        try {
                            callback.startTailing(newFileToWatch.watch, newFileToWatch.file.toString());

                        } catch (Throwable t) {
                            LOGGER.warn("Q Failed:", t);
                        }
                        newFileToWatch.clear();
                    }
                } catch (Throwable t) {
                    LOGGER.error("Take From Q Failed:", t);
                }

                oneThread.submit(createRunnable());
            }};
    }

    private static class Pair {


        public Pair(File file2, WatchDirectory watchDirectory) {
            file = new CompactCharSequence(FileUtil.getPath(file2));
            watch = watchDirectory;
            this.lastModified = file2.lastModified();
        }
        WatchDirectory watch;
        CompactCharSequence file;
        public long lastModified;

        public void clear() {
            watch = null;
            file = null;
        }
    }

    boolean clearLastMod = true;


    public void searchForMatchingFiles() {
        try {

            if (LOGGER.isDebugEnabled()) LOGGER.debug(" searchForMatchingFiles >>");
            if (clearLastMod) {
                clearLastMods();

                // interrupted
                if (buildLastModMap()) return;
                clearLastMod = false;
            }

            if (LOGGER.isDebugEnabled()) LOGGER.debug(" >> WATCH Scan");
            long start = System.currentTimeMillis();

            scanDirs();

            if (LOGGER.isDebugEnabled()) LOGGER.debug(" << WATCH Scan elapsed:" + (System.currentTimeMillis() - start));
            if (LOGGER.isDebugEnabled()) LOGGER.debug(" searchForMatchingFiles <<");

            // sort newest to oldest (i.e. higher numbers first0

        } catch (Throwable t){
            LOGGER.error("Failed schedule", t);
        }
    }

    private void clearLastMods() {
        Collection<WatchDirectory> values = this.watchDirSet.values();
        for (WatchDirectory value : values) {
            value.getTracker().lastModifiedDirs.clear();
        }


    }

    private void scanDirs() throws InterruptedException {

        Collection<WatchDirectory> watchDirs = getFullSetOfWatchDirs();
        for (WatchDirectory watchDirectory : watchDirs) {
            scanWatchDir(watchDirectory);
        }
        addAllForwarderDirs();
    }

    List<File> scanWatchDir(WatchDirectory watchDirectory) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("scanDirs:" + watchDirectory.getTags() + "/" + watchDirectory.getDirName());

        List<File> allFiles = new ArrayList<>();
        WatchDirectoryTracker tracker = watchDirectory.getTracker();
        for (Map.Entry<String, Long> dir : tracker.lastModifiedDirs().entrySet()) {
            try {
                File baseDir = new File(dir.getKey());
                long ourLastMod = dir.getValue();
                long lastModified = baseDir.lastModified();
                boolean isModified = tracker.isDirectoryModified(dir.getKey(), baseDir, ourLastMod, lastModified);
                if (LOGGER.isDebugEnabled()) LOGGER.debug(" -- checkdir:" + dir + " mod:" + isModified);

                if (isModified) {
                    tracker.lastModifiedDirs.put(dir.getKey(), lastModified);
                    String dirKey = makeDirKey(watchDirectory, baseDir);
                    File[] files = findMatchingFiles(baseDir, watchDirectory, dirKey, ourLastMod);
                    if (LOGGER.isDebugEnabled()) {
                        if (files.length > 0) LOGGER.debug("DS-Tag:" + watchDirectory.getTags() + " Dir:" + baseDir.getAbsolutePath() + " Adding FILES:" + files.length);
                        else LOGGER.debug("DS-Tag:" + watchDirectory.getTags() + " Adding FILES: 0");
                    }

                    for (File file : files) {
                        queue(watchDirectory, file);
                        allFiles.add(file);
                    }
                    if (Thread.currentThread().isInterrupted())	throw new InterruptedException();

                }

            } catch (InterruptedException t) {
                throw t;
            } catch (Throwable t) {
                LOGGER.error("Failed to scan dir:" + dir, t);
            }
        }
        if (allFiles.size() > 0) {
            eventMonitor.raise(new Event("ImportStarted").with("Tag", watchDirectory.getTags()).with("FileCount", allFiles.size()));
        }

        tracker.scanDirForLastMod();
        return allFiles;
    }


    synchronized boolean buildLastModMap() throws InterruptedException {

        if (LOGGER.isDebugEnabled()) LOGGER.debug(" >> WATCH AssembleLastMod");

        for (WatchDirectory watchDirectory : getFullSetOfWatchDirs()){
            watchDirectory.getTracker().scanDirForLastMod();
        }

        if (LOGGER.isDebugEnabled()) LOGGER.debug(" << WATCH AssembleLastMod");
        return false;
    }


    private void addAllForwarderDirs() {
        for (String serverDir : this.serverDirs) {
            addForwarderDirectories2(serverDir);
        }
    }

    private void removeCurrentlyHandled(File dir) {
        final long now = System.currentTimeMillis() - DateUtil.HOUR;
        dir.listFiles(new FileFilter() {
            public boolean accept(File file) {
                if (file.lastModified() > now) currentlyHandledFiles.remove(file.getAbsolutePath().hashCode());
                return true;
            }
        });
    }

    public Collection<WatchDirectory> getFullSetOfWatchDirs() {
        addAllForwarderDirs();
        ArrayList<WatchDirectory> allWatchDirs = new ArrayList<WatchDirectory>(watchDirSet.values());
        allWatchDirs.addAll(fwdWatchDirSet.values());
        return allWatchDirs;
    }




    private void addForwarderDirectories2(String serverDir) {
        try {
            Set<String> existingKeys = this.watchDirSet.keySet();
            for (String existingKey : existingKeys) {
                WatchDirectory sourceDS = this.watchDirSet.get(existingKey);

                if (sourceDS.getDirName().contains(LogProperties.getServiceDIRName())) continue;


                String fwdKey =  sourceDS.getFwdId() + serverDir;

                if (fwdWatchDirSet.containsKey(fwdKey)) continue;

                WatchDirectory fwdWatcher = sourceDS.copy();
                StringBuilder newPath = new StringBuilder();
                String[] srcPath = sourceDS.getDirName().split(",");
                for (String src : srcPath) {
                    if (src.length() == 0 || src.equals(".") || src.contains(WatchDirectory.ABS_GENERATED_)) continue;
                    newPath.append(serverDir);

                    if (src.startsWith(".")) {
                        // relative
                        newPath.append("/*/**/logscape/");
                        src = src.substring(2);
                        // support explicit host reference
                    } if (sourceDS.getHosts() != null && sourceDS.getHosts().length() > 0 && !sourceDS.getHosts().contains(",")) {
                        newPath.append("/*" + sourceDS.getHosts() + "*/");
                        // absolute path - so place underneath a 'server dir (*)
                    } else if (src.contains(":") || src.startsWith("/")) {
                        newPath.append("/*/");
                    } else {
                        // let the user do what they want - i.e. might be ** etc
                        newPath.append("/");
                    }


                    newPath.append(src);
                    newPath.append(",");

                    if (src.contains("@")) {
                        newPath.append(serverDir + "/" +src);
                        newPath.append(",");

                    }

                }

//                System.out.println("FWD:" + fwdKey + ">>>PATH:" + newPath);
                String path = newPath.toString();

                fwdWatcher.setId(fwdKey);
                String path1 = FileUtil.cleanupPathAndMakeNative(path);
//                System.out.println("FWD2:" + path1);
                fwdWatcher.setPath(path1);
                fwdWatcher.makeDirsAbsolute();

                fwdWatchDirSet.put(fwdKey, fwdWatcher);
            }

        } catch (Throwable t) {
            LOGGER.error("FWD Add Server Watch Error", t);
        }
    }

    private File[] findMatchingFiles(File baseDir, final WatchDirectory wd, String dirKey, final long ourLastMod) throws Exception {

        if (LOGGER.isDebugEnabled()) LOGGER.debug("findMatching:" + dirKey + " - " + baseDir.getPath());
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        final AtomicBoolean bailing = new AtomicBoolean(false);

        FileFilter filter = new FileFilter() {
            public boolean accept(File file) {
                try {
                    if (interrupted.get() || bailing.get() || file.isDirectory()) return false;
                    return isFileAccepted(wd, file, ourLastMod);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                } catch (Exception e) {
                    LOGGER.error(String.format("findMatchingFiles File:%s %s ex:%s", file.getName(), wd.getFilePattern(), e.toString()), e);
                    bailing.set(true);
                }
                return false;
            }

        };
        if (interrupted.get()) throw new InterruptedException();
        File[] files = baseDir.listFiles(filter);

        if (interrupted.get()) throw new InterruptedException("Interrupted: DataSource:" + wd);
        if (bailing.get()) throw new Exception("Bailing: DataSource:" + wd);

        return files == null ? new File[0] : files;
    }


    private String makeDirKey(WatchDirectory wd, File baseDir) {
        return wd.getTags() + baseDir.getPath() + ":"+ wd.id();
    }

    boolean isIgnoringThisExtension(String fullFilePath) {
        return fullFilePath.contains(DOT) && ignoreExtensionsSet.contains(fullFilePath.substring(fullFilePath.lastIndexOf(DOT)));
    }
    public static boolean isIgnoring(String file) {
        if (!file.contains(".")) return false;
        return ignoreExtensionsSet.contains(file.substring(file.lastIndexOf(DOT)));
    }
    public void stoppedIndexing(String fullFilePath) {
    }
    public boolean isFileAccepted(final WatchDirectory wd, File file, long ourLastMod) throws Exception  {
        String filePath = file.getPath();

        // fast
        if (isIgnoringThisExtension(filePath)) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Ignoring Ext:" + file.getName());
            return false;
        }
        if (isCurrentlyHandled(filePath)) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Currently Handled:" + file.getName());
            return false;
        }

        // dont track old files - just bin them - there might be millions of old files which we cannot afford to hold in memory
        long lastModified = file.lastModified();

        if (wd.isTooOld(lastModified)) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Too Old:" + file.getName());
            return false;
        }

        if (!wd.matchesHost(LogProperties.getHostFromPath(filePath))) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Not Host:" + file.getName());
            return false;
        }


        if (FileUtil.isDirectory(file) || file.length() == 0 || !file.canRead()) return false;

        String fileCanon =  file.getAbsolutePath();

        // if full filePath doesnt match, check that it might be an explicit match only on the filename...
        // also dont cut this file out because it might be watched with an overlapping WatchDir
        if (!wd.isWatching(fileCanon, "")) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Not Watching:" + file.getName());
            return false;
        }


        if (callback.isTailingFile(fileCanon, file)) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Is Tailing:" + file.getName());
            addFile(filePath);
            return false;
        }

//        File fileO = new File(filename);
//        long lastMod = fileO.lastModified();
//        long lastWatchWindow = new DateTime().minusHours(LogProperties.getMaxTailDequeueHours()).getMillis();
//        return lastMod > lastWatchWindow;



        //only bother checking rolling candidates on files which are new - they should roll-detect within a minute
        if (isLessThanXHours(lastModified) && callback.isRollCandidateForTailer(fileCanon, new HashSet<String>(), wd.getTags())) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Is Roll Candidate:" + file.getName());
            addFile(filePath);
            return false;
        }


        // it might be a compressed empty file
        if ((fileCanon.endsWith(".gz") || fileCanon.endsWith(".bz2")) && file.length() < 1024) return false;

        if (LOGGER.isDebugEnabled()) LOGGER.debug("Running:isFileAccepted:" + wd.getTags() + " TRUE file:" + fileCanon);

        return true;
    }

    private boolean isLessThanXHours(long lastModified) {
        return (System.currentTimeMillis() - lastModified) <  LogProperties.LIVE_ROLL_HOURS * DateUtil.HOUR;
    }

    private void addFile(String filePath) {
        if (!currentlyHandledFiles.containsKey(filePath.hashCode())) currentlyHandledFiles.put(filePath.hashCode(), new HashSet<CompactCharSequence>());
        currentlyHandledFiles.get(filePath.hashCode()).add(new CompactCharSequence(filePath));
    }

    private boolean isCurrentlyHandled(String filePath) {
        final Set<CompactCharSequence> strings = currentlyHandledFiles.get(filePath.hashCode());
        if(strings == null) return false;

        return strings.contains(filePath);
    }

    public void start() {
    }
    public void stop() {
        lifecycle = State.STOPPED;
        if (tailerFuture != null) tailerFuture.cancel(false);
        queue.clear();
        visitorScheduler.shutdownNow();
    }
    void queue(final WatchDirectory watch, final File file) {
        queue.put(new Pair(file, watch), file.lastModified());
    }


    public boolean isSuspended() {
        return tailerFuture == null;
    }

    /**
     * Stops all tailing activity
     */
    public synchronized void suspend(boolean suspended)  {
        if (suspended) {
            try {
                if (tailerFuture != null) {
                    if(LOGGER.isDebugEnabled()) LOGGER.debug("Interrupting ******************* :" + tailerFuture);
                    tailerFuture.cancel(false);
                    tailerFuture = null;
                }
            } catch (Throwable t) {
                LOGGER.info("Interrupt Visitor Thread:" + t.toString());
            }
            queue.clear();

            reset();
        } else {
            currentlyHandledFiles.clear();
            if (tailerFuture == null) {
                scheduleVisitor();
                clearLastMod = true;
                LOGGER.info("Starting ******************* :" + tailerFuture + " Q:" + queue.size());
                oneThread.submit(createRunnable());
            }
        }
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }
    }
    public void reset() {
        try {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Visitor Reset Collection");
            queue.clear();
            currentlyHandledFiles.clear();
            clearLastMods();
            clearLastMod = true;
            fwdWatchDirSet.clear();
            serverDirs.clear();
            fwdWatchDirSet.clear();

        } catch (Throwable t) {
            LOGGER.warn("Reset() error", t);
        }
    }

}
