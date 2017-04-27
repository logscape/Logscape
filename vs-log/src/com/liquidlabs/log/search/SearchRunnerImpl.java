/**
 *
 */
package com.liquidlabs.log.search;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.search.handlers.Handlers;
import com.liquidlabs.log.search.handlers.LogScanningHandler;
import com.liquidlabs.log.search.tailer.TailerAggSpace;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;

public class SearchRunnerImpl implements SearchRunnerI {
    private static final Logger LOGGER = Logger.getLogger(SearchRunnerI.class);
    private Indexer indexer;
    private TailerAggSpace aggSpace;
    private final EventMonitor eventMonitor;
    DateTimeFormatter formatter = DateTimeFormat.mediumDateTime();
    DecimalFormat decimalFormatter = new DecimalFormat("#.##");

    private final ThreadPoolExecutor executorPool;
    private final ThreadPoolExecutor slowPool = (ThreadPoolExecutor) com.liquidlabs.common.concurrent.ExecutorService.newFixedPriorityThreadPool(LogProperties.getSlowSearchPoolSize(), LogProperties.getSlowPoolQueueSize(),"tailer-search-slow", Thread.NORM_PRIORITY-1);


    public SearchRunnerImpl(Indexer indexer, TailerAggSpace aggSpace, EventMonitor eventMonitor, ThreadPoolExecutor executorPool) {
        this.indexer = indexer;
        this.aggSpace = aggSpace;
        this.eventMonitor = eventMonitor;
        this.executorPool = executorPool;
    }

    public int search(final LogRequest request) {
        try {

            final Set<String> fileTypes = new HashSet<String>();
            long before = System.currentTimeMillis();
            final boolean verbose = false;
            indexer.stallIndexingForSearch();
            List<LogFile> filteredFiles = indexer.indexedFiles(request.getStartTimeMs(), request.getEndTimeMs(), true, new LogFileOps.FilterCallback() {
                public boolean accept(LogFile logFile) {
                    boolean searchable = request.isSearchable(logFile, NetworkUtils.getHostname());
                    if (verbose && !searchable){
                        LOGGER.info("Excluding:" + logFile.getFileName());
                    }
                    if (searchable) fileTypes.add(logFile.getFieldSetId());
                    return searchable;
                }
            });


            long after = System.currentTimeMillis();
            if (fileTypes.size() > 1) request.setMultiTypeSearchFlag();


            // Remove the system filters _host etc because the were used to filter the files in the query above
            // cant do this if there are multiple queries.....
            request.removeSystemFieldFilters();

            if (request.isVerbose()) {
                eventMonitor.raise(new Event("Scan").with("fileCount", filteredFiles.size())
                        .with("startTime", "'" + formatter.print(request.getStartTimeMs()) + "'")
                        .with("queries", request.queries()).with("endTime", "'" + formatter.print(request.getEndTimeMs()) + "'")
                        .took(after - before));
            }

            indexer.stallIndexingForSearch();

            boolean runSlow = request.isNowBGWork() && executorPool.getActiveCount() > 0;

            int count = 0;
            List<ScannerTask> tasks = new CopyOnWriteArrayList<ScannerTask>();
            for (LogFile logFile : filteredFiles) {
                LogScanningHandler handler = Handlers.getSearchHandlers(logFile, request, aggSpace);
                ScannerTask task = new ScannerTask(logFile, request, indexer, handler, LogProperties.getLogHttpServerURI(), aggSpace, ++count);
                if (runSlow) task.future = slowPool.submit(task);
                else task.future = executorPool.submit(task);

                tasks.add(task);
            }

            runLatchUntilComplete(tasks, request);

            return tasks.size();
        } catch (OutOfMemoryError oe) {
            LOGGER.fatal("Search OutOfMemory", oe);
            request.cancel();
            List<Thread> searches1 = getSearches();
            for (Thread thread : searches1) {
                thread.interrupt();
            }
        } catch (Throwable t) {
            LOGGER.error("Search Failed", t);
        }
        return 0;
    }
    public List<Thread> getSearches() {
        return searches;
    }
    List<Thread> searches = new CopyOnWriteArrayList<Thread>();

    private void runLatchUntilComplete(final List<ScannerTask> tasks, final LogRequest request) {
        final long start = System.currentTimeMillis();
        LOGGER.info("START:" + request.subscriber());

        final Thread finalThread = new Thread("Searching:" + request.subscriber()) {
            @Override
            public void run() {

                String subscriber = request.subscriber();
                long flushStart = System.currentTimeMillis();
                try {

                    int completeCount = 0;
                    try {
                        int interval = LogProperties.searchFlushIntervalMs();
                        int elapsed = 0;
                        int secondsBetweenUpdate = 5;
                        while (!request.isCancelled() && !request.isExpired() &&  !ScannerTask.isComplete(tasks)) {
                            Thread.sleep(interval);

                            elapsed += interval;
                            if (isSendingStatus(elapsed, secondsBetweenUpdate)) {
                                int[] eventsAndPercent = ScannerTask.getEventsAndPercentComplete(tasks);
                                aggSpace.status(request.subscriber(), "", eventsAndPercent[0], eventsAndPercent[1]);
                                //aggSpace.flush(request.subscriber(), false);
                            }

                            if ((elapsed % 1000) == 0)
                               // aggSpace.flush(subscriber, false);
                                indexer.stallIndexingForSearch();
                        }
                        aggSpace.status(request.subscriber(), "", ScannerTask.getScannedCount(tasks), 100);
                    } catch (InterruptedException ie) {
                        LOGGER.error("SearchInterrupted:" + subscriber,ie);
                        request.cancel();
                    } catch (Throwable e) {
                        LOGGER.error("SearchFailed:" + subscriber,e);
                        request.cancel();
                    }

                    LOGGER.info("FINISH:" + request.subscriber());
                    flushStart = System.currentTimeMillis();
                    aggSpace.flush(request.subscriber(), true);
                    LOGGER.info("FINISH FLUSH_COMPLETE:" + request.subscriber());

                    completeCount = ScannerTask.getCompleteCount(tasks);
                    if (completeCount != tasks.size() ||  request.isVerbose()) {
                        if (!ScannerTask.isComplete(tasks)) LOGGER.info("Didnt finish all Tasks, Cancelled:" + request.isCancelled() + " Complete:" + completeCount + " Tasks:" + tasks.size() + " Expired:" + request.isExpired());
                        request.cancel();
                        for (ScannerTask scannerTask : tasks) {
                            if (request.isVerbose()) LOGGER.info(scannerTask.toString());
                            if (scannerTask.future != null && !scannerTask.future.isDone()) {
                                if (!request.isCancelled()) LOGGER.info("Failed to finish:" + scannerTask);
                                else {
                                    if (LOGGER.isDebugEnabled()) LOGGER.debug("Cancelling:" + scannerTask);
                                    // must be FALSE as it will stuff up BDB
                                    boolean success = scannerTask.future.cancel(false);
                                    if (!success) LOGGER.warn("Failed to cancel task:" + scannerTask);

                                }
                            }
                        }
                    }

                } catch (Throwable t) {
                    LOGGER.warn("TasksFailed ex:" + t.toString(), t);
                } finally {
                    long end = DateTimeUtils.currentTimeMillis();
                    // now wait for the tasks to complete and then flush it down
                    long elapsed = end - start;
                    double ratePerSec = (double) ScannerTask.getScannedCount(tasks)/((double)elapsed / 1000);
                    eventMonitor.raise(new Event("SearchComplete").took(elapsed).with("linesPerSec", decimalFormatter.format(ratePerSec)).with("subscriber", subscriber));
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("FLUSH_COMPLETE:" + request.subscriber() + " FlushElapsed:" + (System.currentTimeMillis() - flushStart));
                }
            }
        };
        searches.add(finalThread);
        finalThread.start();
        removeCompleteTasks();
    }



    public void removeCompleteTasks() {

        // remove complete tasks
        for (Iterator iterator = searches.iterator(); iterator.hasNext();) {
            Thread task = (Thread) iterator.next();
            if (!task.isAlive()) searches.remove(task);
        }
    }

    boolean isSendingStatus(int elapsed, int secondsBetweenUpdate) {
        return elapsed > 0 && (elapsed % (secondsBetweenUpdate * 1000) == 0 || elapsed == 1000);
    }

    public void setAggSpace(TailerAggSpace aggSpace) {
        this.aggSpace = aggSpace;
    }
}