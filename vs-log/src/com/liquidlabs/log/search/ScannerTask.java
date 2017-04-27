package com.liquidlabs.log.search;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.collection.CompactCharSequence;
import com.liquidlabs.common.concurrent.PriorityThreadPoolExecutor;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.handlers.LogScanningHandler;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ScannerTask implements  PriorityThreadPoolExecutor.PrioritisedRunnable {

    String filename;

    private LogRequest request;
    private Indexer indexer;
    private LogScanningHandler handler;
    private String sourceUri;
    private AggSpace aggSpace;
    private int priority;

    public ScannerTask(LogFile filename, LogRequest request, Indexer indexer, LogScanningHandler handler, String sourceUri, AggSpace aggSpace, int priority) {
        this.file = filename;
        this.priority = priority;
        this.filename = filename.getFileName();
        this.request = request;
        this.indexer = indexer;
        this.handler = handler;
        this.sourceUri = sourceUri;
        this.aggSpace = aggSpace;
    }

    LogFile file;
    private Scanner scanner;
    long eventsComplete = 0;
    boolean done = false;
    String status = null;

    Future<?> future;

    public ScannerTask() {
        //To change body of created methods use File | Settings | File Templates.
    }

    public String toString() {
        if (status == null) {
            return String.format("ScannerTask: File:%s Incomplete", file.getFileName());
        } else {
            return String.format("ScannerTask: Scanner:%s", status);
        }
    }

    public boolean isComplete() {
        // has finished or hasnt run yet...
        if (done) return true;
        // currently running
        if (scanner != null){
            done = scanner.isComplete();
            if (done) {
                eventsComplete = scanner.eventsComplete();
                status = scanner.toString();
                scanner = null;
            }
            return done;
        }
        return false;
    }

    public long getCompleteEvents() {
        if (scanner != null) {
            eventsComplete = scanner.eventsComplete();
        }
        return eventsComplete;
    }

    static public int[] getEventsAndPercentComplete(List<ScannerTask> tasks) {
        if (isComplete(tasks)) return new int[] { 100, 100 };
        long total = 0;
        double percent = 0;
        for (ScannerTask task : tasks) {
            total += task.getCompleteEvents();
            percent += task.getPercentComplete();
        }

        return new int[] {
                (int) total,  (int) (percent / tasks.size()) };
    }

    static public int getPercentComplete(List<ScannerTask> tasks) {
        if (isComplete(tasks)) return 100;
        long total = 0;
        double percent = 0;
        for (ScannerTask task : tasks) {
            total += task.getCompleteEvents();
            percent += task.getPercentComplete();
        }

        return (int) (percent / tasks.size());
    }

    private double getPercentComplete() {
        if (done) return 100.0;
        return scanner == null ? 0 : scanner.getPercentComplete();
    }

    static public long getScannedCount(List<ScannerTask> tasks) {
        long complete = 0;
        for (ScannerTask task : tasks) {
            complete += task.getCompleteEvents();
        }
        return complete;
    }

    public static boolean isComplete(List<ScannerTask> tasks) {
        int completed = 0;
        for (ScannerTask task : tasks) {
            if (task.isComplete()) {
                completed++;
            }
        }
        return completed == tasks.size();
    }

    public static int getCompleteCount(List<ScannerTask> tasks) {
        int complete = 0;
        for (ScannerTask task : tasks) {
            if (task.isComplete()) {
                complete++;
            }
        }
        return complete;
    }
    private  Scanner getScanner() {
//        if (MTFileScanner.isMtTaskable(filename, request, logFile)){
//            return new MTFileScanner(indexer, filename, handler, mtExecutorPool,  logFile.getTags(), getSlowPool(), sourceUri, LogProperties.getMTScanTaskThreshold(), aggSpace);
//        } else {
            return new FileScanner(indexer, file.getFileName(), handler,  file.getTags(), sourceUri, aggSpace);
//        }
    }


    @Override
    public void run() {
        if (done) return;
        String hostname = NetworkUtils.getHostname();
        this.scanner = getScanner();
        List<HistoEvent> histo = new ArrayList<HistoEvent>();
        for (Query query : request.queries()) {
            histo.add(new HistoEvent(request, file.getId(), request.getStartTimeMs(), request.getEndTimeMs(), request.getBucketCount(), hostname, sourceUri, request.subscriber(), query, aggSpace, false));
        }
        this.scanner.search(request.copy(), histo, new AtomicInteger());
    }

    @Override
    public int getPriority() {
        return priority;
    }
    public void cancel() {
        if (future !=null && !future.isCancelled() && !future.isDone()) future.cancel(false);
        future = null;
        done = true;
    }
}
