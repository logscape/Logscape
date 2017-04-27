package com.liquidlabs.log;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.space.WatchDirectory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WatchManager {
    static final Logger LOGGER = Logger.getLogger(WatchManager.class);
    private final EventMonitor monitor = new LoggingEventMonitor();

    private final Callback callback;

    private Map<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();

    private final Indexer indexer;
    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");

    private WatchVisitor watchVisitor;
    public static boolean PREVENT_SEARCH = false;

    public static class Callback {

        public void statsRecorderScheduleUpdate(int i) {
        }

        public void statsRecorderScheduleUpdateForNextMinutes(int i) {
        }

        public List<Tailer> tailers() {
            return null;
        }

        public void deleteLogFile(List<LogFile> logFile, boolean forceRemove) {
        }

        public void failedToAddWatch(WatchDirectory newWatchDirectory, Throwable reason) {

        }
    }

    public WatchManager(Callback callback, final Indexer indexer, final int dailyQuota) {
        this.callback = callback;
        this.indexer = indexer;
        LOGGER.info("QUOTA_LIMIT_GB:" + dailyQuota);
        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                double today = FileUtil.getGIGABYTES(indexer.getLastStats().indexedToday());
                if (today > dailyQuota) {
                    LOGGER.info("QUOTA_Hit_GB:" + dailyQuota + " QUOTE_Collected_Today:" + today);
                    System.out.println("QUOTA_Hit_GB:" + dailyQuota + " QUOTE_Collected_Today:" + today);
                    if (!watchVisitor.isSuspended()) watchVisitor.suspend(true);
                    PREVENT_SEARCH = true;
                } else {
                    PREVENT_SEARCH = false;
                    if (watchVisitor.isSuspended()) {
                        LOGGER.info("QUOTA_Block_Released_GB:" + today);
                        watchVisitor.suspend(false);
                    }
                }

            }
        }, 1, 1, TimeUnit.MINUTES);
    }


    public int removeWatch(WatchDirectory watchItem) {
        int count = 0;

        LOGGER.info(String.format("LS_EVENT:RemoveWatch Id:" + watchItem.id()));
        try {
            monitor.raise(new Event("Remove").with("dataSource", watchItem.getTags()));

            watchVisitor.suspend(true);
            watchDirSet.remove(watchItem.id());

            watchItem.makeDirsAbsolute();

            count += deleteFilesForWatch(watchItem, true);

            final Event removeWatch = new Event("RemoveWatch");
            watchItem.fillIn(removeWatch);
            monitor.raise(removeWatch.with("tailers", callback.tailers().size()).with("removed", count));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            watchVisitor.reset();
            watchVisitor.suspend(false);
        }

        return count;
    }

    private static class WatchFilePair {
        public WatchFilePair(WatchDirectory watch, LogFile logFile) {
            this.watch = watch;
            file = logFile;
        }

        WatchDirectory watch;
        LogFile file;
    }

    private int deleteFilesForWatch(final WatchDirectory watchItem, boolean forceRemove) {

        final List<WatchFilePair> changeOwnerShip = new ArrayList<WatchFilePair>();
        final List<LogFile> removeFiles = new ArrayList<LogFile>();
        final String myHost = NetworkUtils.getHostname();
        indexer.indexedFiles(new LogFileOps.FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {

                if (isWatching(watchItem, null, logFile.getFileName(), logFile.getTags())) {
                    removeFiles.add(logFile);
                }
                return false;
            }
        });

        callback.deleteLogFile(removeFiles, forceRemove);
        updateOwnership(changeOwnerShip);

        final Event deleteFilesForWatch = new Event("DeleteFilesForWatch");
        watchItem.fillIn(deleteFilesForWatch);
        monitor.raise(deleteFilesForWatch.with("removed", removeFiles.size()));
        return removeFiles.size();
    }

    private void updateOwnership(final List<WatchFilePair> changeOwnerShip) {
        for (WatchFilePair pair : changeOwnerShip) {
            pair.file.setTags(pair.watch.getTags());
            indexer.updateLogFile(pair.file);
        }
    }


    public void updateWatch(final WatchDirectory newWatchDirectoryS, boolean suspendIt) {

        LOGGER.info("LS_EVENT:UpdateWatch:" + newWatchDirectoryS.id() + " MyHostName:" + NetworkUtils.getHostname() + " DS_HOSTNAME:" + newWatchDirectoryS.getHosts());

        try {


            if (suspendIt) watchVisitor.suspend(true);
            // remove it so we can automatically grab the new version in the scan
            //removeWatch(newWatchDirectoryS);
            final WatchDirectory newWatchDirectory = newWatchDirectoryS.copy();
            final WatchDirectory existing = watchDirSet.get(newWatchDirectory.id());

            // check if we need to remove and then bail////
            if (LogProperties.isForwarder && !newWatchDirectory.isHostMatch(NetworkUtils.getHostname()) && existing != null) {
                LOGGER.info("LS_EVENT: TAG:" + newWatchDirectory.getTags() + " UPDATED, HOST MISMATCH, REMOVING");
                removeWatch(existing);
                return;
            }

            final List<WatchDirectory> fwdDs = watchVisitor.getFwdSources(newWatchDirectory.id());


            List<Tailer> tailers = callback.tailers();
            for (Tailer tailer : tailers) {
                if (isWatching(newWatchDirectory,fwdDs, tailer.filename(), tailer.fileTag()))  {
                    tailer.setWatch(newWatchDirectory);
                }
            }



            if (existing != null && newWatchDirectoryS.equals(existing)) {
                LOGGER.debug("LS_EVENT:UpdateWatch Evaluated to SAME, - Nothing to do(noop)");
                return;
            }
            if (existing == null) {
                addWatch(newWatchDirectory);
                return;
            }

            // now see if the timeformat or the linebreak rule have changed - if they have then we need to remove everything and re-import
            if (!existing.getBreakRule().equals(newWatchDirectory.getBreakRule()) || !existing.getTimeFormat().equals(newWatchDirectory.getTimeFormat())) {
                LOGGER.warn(String.format("LS_EVENT:Add/Remove Watch - due to BreakRule[%s/%s] OR TimeFormat[%s/%s] change!", existing.getBreakRule(), newWatchDirectory.getBreakRule(), existing.getTimeFormat(), newWatchDirectory.getTimeFormat()));
                // delete against the existing watch
                removeWatch(existing);
                Thread.sleep(2000);
                // add the new watch
                addWatch(newWatchDirectory);
                return;
            }
            watchVisitor.reset();
            watchDirSet.put(newWatchDirectory.id(), newWatchDirectory);

            monitor.raise(new Event("WatchUpdated").with("tags", newWatchDirectory.getTags()).with("tags", newWatchDirectory.getTags()));


            // Check discovery flags  have changed..... if so - then reindex
            if (existing != null) {
                if (!newWatchDirectory.isFieldDiscoveryEqual(existing)) {
                    monitor.raise(new Event("WatchUpdated").with("tags", newWatchDirectory.getTags()).with("discoveryFlagsChange", "true"));
                    removeWatch(existing);
                    addWatch(newWatchDirectory);
                    return;
                }
            }

            /**
             * file path or mask has changed - so reindex
             */
            // need to change path to be local-format before doing any comparisons
            newWatchDirectory.makeDirsAbsolute();
            if (existing != null && !existing.getDirName().equals(newWatchDirectory.getDirName()) || !existing.getFilePatterns().equals(newWatchDirectory.getFilePatterns()) ) {
                monitor.raise(new Event("WatchUpdated").with("tags", newWatchDirectory.getTags()).with("PathChanged NewPath", newWatchDirectory.getDirName()).with("OldPath", existing.getDirName()));
                // need to remove stuff which is no longer being watch by either....
                indexer.indexedFiles(new LogFileOps.FilterCallback() {
                    public boolean accept(LogFile logFile) {
                        // was watching but not any more
                        if (isWatching(existing, fwdDs, logFile.getFileName(), logFile.getTags()) &&
                                // TODO; this is problematic.... need fwdd version
                                !isWatching(newWatchDirectory, null, logFile.getFileName(), logFile.getTags())) {
                            callback.deleteLogFile(Arrays.asList(logFile), true);
                        }
                        return false;
                    }
                });

            }

            // see if the tags have changed.
            if (existing != null && !existing.getTags().equals(newWatchDirectory.getTags())) {
                LOGGER.info("WatchUpdated - Renaming TAGS: for:" + newWatchDirectory + " Tags:" + newWatchDirectory.getTags());
                // we need to rename all of the tags files.
                final AtomicInteger count = new AtomicInteger();
                indexer.indexedFiles(new LogFileOps.FilterCallback() {
                    public boolean accept(LogFile logFile) {
                        if (isWatching(existing, fwdDs, logFile.getFileName(), logFile.getTags())) {
                            logFile.setTags(newWatchDirectory.getTags());
                            indexer.updateLogFile(logFile);
                            count.incrementAndGet();
                        }
                        return false;
                    }
                });
                LOGGER.debug("Going to rename tags for files:" + count);

                LOGGER.info("WatchUpdated - DONE Renaming TAGS: for:" + newWatchDirectory.id() + " Count:" + count);
                return;
            }

//            LOGGER.debug("LS_EVENT:UpdateWatch - Adding files");

            try {

//	            watchVisitor.scanDirForLastMod(newWatchDirectory);
// This operation is TOO slow and doesnt scale for the amount of dirs
//                newWatchDirectory.getTracker().scanDirForLastMod();
//
//                List<File> filesToAdd = watchVisitor.scanWatchDir(newWatchDirectory);
//                if (filesToAdd.size() > 0) LOGGER.info("LS_EVENT:StartMonitoringFiles :" + newWatchDirectory.getDirName() + " count:" + filesToAdd.size());
//                if (filesToAdd.size() > LogProperties.highPriorityTailerQueueSize()) {
//                    filesToAdd = filesToAdd.subList(0, LogProperties.highPriorityTailerQueueSize() -5);
//                }
//                for (File file : filesToAdd) {
//                    try {
//                        watchVisitor.queue(newWatchDirectory, file);
//                    } catch (Throwable t) {
//                        LOGGER.warn("addWatch op-warn:" + file.getAbsolutePath(), t);
//                    }
//                }
            } catch (Throwable t) {
                LOGGER.warn("Failed to add Dir:" + newWatchDirectory.getDirName(), t);
            }
            LOGGER.info("removeFilesViaWatchManager");
            removeFilesNoLongerBeingWatched();
        } catch (InterruptedException iex) {
        } finally {
            watchVisitor.reset();
            if (suspendIt) watchVisitor.suspend(false);
        }

    }

    private boolean isWatching(WatchDirectory ds, List<WatchDirectory> fwdDs, String filename, String tags) {
        if (ds.isWatching(filename, tags)) {
            String fileHost = getFileHost(filename);
            if (ds.matchesHost(fileHost))  return true;
        } else {
            if (fwdDs == null) fwdDs = watchVisitor.getFwdSources(ds.id());
            for (WatchDirectory fwdD : fwdDs) {
                if (fwdD.isWatching(filename, tags)) {
                    String fileHost = getFileHost(filename);
                    if (fwdD.matchesHost(fileHost))  return true;
                }
            }
        }
        return false;
    }
    private String getFileHost(String filename) {
        String[] hostnameFromPath = LogFile.getHostnameFromPath(filename);
        if (hostnameFromPath != null) return hostnameFromPath[0];
        return NetworkUtils.getHostname();
    }

    public void addWatch(WatchDirectory watchItem) {
        callback.statsRecorderScheduleUpdate(20);
        callback.statsRecorderScheduleUpdateForNextMinutes(10);
        addWatchItem(watchItem.copy());
    }

    public void addWatches(List<WatchDirectory> watching) {
        watchVisitor.suspend(true);
        LOGGER.info("LS_EVENT:Bulk addWatches");
        for(WatchDirectory directory : watching) {
            if (LogProperties.isForwarder && !directory.isHostMatch(NetworkUtils.getHostname())) {
                if(LOGGER.isDebugEnabled()) LOGGER.debug(String.format("LS_EVENT:FwdrIgnoreWatch WrongHost TAG:" + directory.getTags() +
                        " GOT_HOST:" + NetworkUtils.getHostname().toLowerCase() + " EXPECTED_HOST:" + directory.getHosts().toLowerCase()));
            } else {
                LOGGER.info(String.format("LS_EVENT:AddWatch Tags:" + directory.getTags() + " Id:" + directory.id()));
                this.watchDirSet.put(directory.id(), directory);
            }

        }
        watchVisitor.suspend(false);

    }

    void addWatchItem(WatchDirectory newWatchDirectory) {

        if (LogProperties.isForwarder && !newWatchDirectory.isHostMatch(NetworkUtils.getHostname())) {
            LOGGER.info(String.format("LS_EVENT:FwdrIgnoreWatch WrongHost TAG:" + newWatchDirectory.getTags() +
                                        " GOT_HOST:" + NetworkUtils.getHostname().toLowerCase() +  " EXPECTED_HOST:" + newWatchDirectory.getHosts().toLowerCase()));
            return;
        }
        LOGGER.info(String.format("LS_EVENT:AddWatchItem Tags:" + newWatchDirectory.getTags() + " Id:" + newWatchDirectory.id()));

        final WatchDirectory existing = watchDirSet.get(newWatchDirectory.id());
        if (existing != null && newWatchDirectory.equals(existing)) {
            LOGGER.info("Ignore existing Watch Id:" + existing.id());
            return;
        }

        monitor.raise(newWatchDirectory.fillIn(new Event("AddWatch")));

        if (this.watchDirSet.containsKey(newWatchDirectory.id())) {
            LOGGER.info("LS_EVENT:Add_to_Update_Watch id:" + newWatchDirectory.id());
                    updateWatch(newWatchDirectory, false);
            return;

        }

        watchVisitor.reset();

        try {
            watchVisitor.suspend(true);
            this.watchDirSet.put(newWatchDirectory.id(), newWatchDirectory);
            newWatchDirectory.getTracker().scanDirForLastMod();
            watchVisitor.suspend(false);
            List<File> filesToAdd = watchVisitor.scanWatchDir(newWatchDirectory);
            LOGGER.info("LS_EVENT:AddWatch AddingFiles:" + filesToAdd.size());

            if (LOGGER.isDebugEnabled()) LOGGER.debug("LS_EVENT:AddWatch DONE :" + newWatchDirectory);
        } catch (Throwable t) {
            callback.failedToAddWatch(newWatchDirectory, t);
            LOGGER.warn("Failed to add Dir:" + newWatchDirectory, t);
        }  finally {
            watchVisitor.suspend(false);
        }
    }

    protected void expireFileData() {
        try {

            LOGGER.info("expireFileData()");

            final List<LogFile> deleteFiles = new ArrayList<LogFile>();
            indexer.indexedFiles(new LogFileOps.FilterCallback() {
                public boolean accept(LogFile logFile) {
                    if(shouldDeleteLogFile(logFile, WatchManager.this.watchDirSet)) deleteFiles.add(logFile);
                    return false;
                }
            });
            if (deleteFiles.size() > 0) {
                monitor.raise(new Event("RemoveData").with("reason", "expired").with("count", deleteFiles.size()));
                callback.deleteLogFile(deleteFiles, true);
            }

        } catch (Throwable t) {
            LOGGER.warn("Check expired files failed", t);
        }
    }

    protected boolean shouldDeleteLogFile(LogFile logFile, Map<String, WatchDirectory> watchDirSet){
        if(LOGGER.isDebugEnabled())LOGGER.debug("Should delete " + logFile.getFileName() + "  ?");
        int watchCount = 0;
        int deleteCount = 0;
        String isWatchingTags = "";
        String canDeleteTags = "";
        for(WatchDirectory wd : watchDirSet.values()){
            boolean isWatching = wd.isWatching(logFile);
            boolean isTooOld = wd.isTooOld(logFile.getEndTime());
            if(isWatching){
                watchCount++;
                isWatchingTags = wd.getTags() + ":" + isWatchingTags;
                if(isTooOld) {
                    canDeleteTags = wd.getTags() + ":" + canDeleteTags;
                    deleteCount++;
                }
            }
        }
        if((watchCount == 1 && deleteCount == 1)){
            if(LOGGER.isDebugEnabled()) LOGGER.debug("shouldDeleteLogFile:True watchCount:" + watchCount + " deleteCount:" + deleteCount + " tags calling for deletion: " +canDeleteTags);
            return true;
        }
        if(watchCount == 1 && deleteCount == 0) return false;
        if(watchCount > 0 && (watchCount == deleteCount)){
            LOGGER.warn("Deleting " + logFile.getFileNameOnly() + " however encountered overlapping datasources. Watching:" + watchCount + " WatchTags:" + isWatchingTags + " deleteTags:" +canDeleteTags);
            return true;
        }
        if(watchCount > deleteCount){
            LOGGER.error("Encountered overlapping datasources deleting file:" + logFile.getFileNameOnly() + " File was not deleted. Watching:" + watchCount + " Deleting:" + deleteCount + " tags:" + isWatchingTags);
            return false;
        }
        return false;
    }

    public void removeFilesNoLongerBeingWatched() {

        if (watchDirSet.size() == 0) {
            LOGGER.error("WatchDir/DataSources size == 0");
            return;
        }
        int maxItems = 5 * 1024;
        int removedCount = 1;
        int count = 0;
        LOGGER.info("removeFilesNoLongerBeingWatched()");
        monitor.raise(new Event("removeFilesNoLongerBeingWatched").with("DataSources",watchVisitor.getFullSetOfWatchDirs().size()));
        while (removedCount > 0 && count++ < 5) {
            if (watchVisitor.isSuspended()) return;

            try {
                final List<LogFile> notWatched = new ArrayList<LogFile>();
                indexer.indexedFiles(new LogFileOps.FilterCallback() {

                    public boolean accept(LogFile logFile) {
                        if (watchVisitor.isSuspended()) return false;
                        boolean watched = false;
                        for (WatchDirectory wd : watchVisitor.getFullSetOfWatchDirs()) {
                            if (isWatching(wd, null, logFile.getFileName(), logFile.getTags())) {
                                if (!wd.isTooOld(logFile.getEndTime())) {
                                    watched = true;
                                }
                            }
                        }
                        if (!watched) {
                            monitor.raise(new Event("Remove").with("file", logFile.getFileNameOnly()).with("Reason", "NotWatched"));
                            LOGGER.info("Removing:" + logFile.getFileName() + " tag:" + logFile.getTags() + " lastMod:" + new DateTime(logFile.getEndTime()));
                            tryAndSetLastModToCorrectTime(logFile);

                            watchVisitor.stoppedWatching(logFile.getFileName());
                            notWatched.add(logFile);
                        }
                        return false;
                    }
                });

                if (notWatched.size() > 0) {
                    monitor.raise(new Event("RemoveData").with("reason", "'not watched'").with("count", notWatched.size()));
                    callback.deleteLogFile(notWatched, true);
                }
                removedCount = notWatched.size();
                Thread.sleep(1000);
            } catch (Throwable t) {
                LOGGER.warn("Remove Files Error", t);
                removedCount = 1;
                pause();
            }
        }
    }

    /**
     * Ideally we can write the file and change the last mod and be done with it
     * Otherwise it is going to keep spinning
     * @param logFile
     */
    private void tryAndSetLastModToCorrectTime(LogFile logFile) {
        File file = new File(logFile.getFileName());
        DateTime detectedLastMod = new DateTime(logFile.getEndTime());
        if (file.canWrite() && new DateTime(file.lastModified()).getDayOfYear() != detectedLastMod.getDayOfYear()){
            try {
                file.setLastModified(detectedLastMod.getMillis());
            } catch (Throwable t) {
            }
        }
    }

    private void pause() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }

    public Map<String, WatchDirectory> watchDirSet() {
        return this.watchDirSet;
    }

    public void setVisitor(WatchVisitor watchVisitor) {
        this.watchVisitor = watchVisitor;
    }

    public boolean shouldWatch(String file) {

        for (WatchDirectory watch : getAllWatchDirs()) {
            if (watch.isWatching(file, "")) return true;
        }
        return false;
    }

    private List<WatchDirectory> getAllWatchDirs() {
        ArrayList<WatchDirectory> allWatches = new ArrayList<WatchDirectory>(this.watchDirSet.values());
        allWatches.addAll(watchVisitor.fwdWatchDirSet.values());
        return allWatches;
    }
}
