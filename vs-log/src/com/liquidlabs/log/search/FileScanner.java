package com.liquidlabs.log.search;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.ReplayAbortedException;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.handlers.LogScanningHandler;
import com.liquidlabs.log.search.handlers.SummaryBucket;
import com.liquidlabs.log.search.handlers.SummaryBucketCachedHandler;
import com.liquidlabs.log.search.handlers.SummaryBucketHandler;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.logscape.disco.indexer.IndexFeed;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FileScanner extends BaseScanner  {

    private static final Logger LOGGER = Logger.getLogger(FileScanner.class);

    public FileScanner(Indexer indexer, String filename, LogScanningHandler handler, String fileTags, String sourceUri, AggSpace aggSpace) {
        super(indexer, filename, handler, fileTags, sourceUri);
    }

    @Override
    public double getPercentComplete() {
        if (totalEvents == 0) return 0;
        return Math.min(((double) scannedEvents.get()) / ((double) totalEvents), 1.0);
    }


    public int search(final LogRequest request, List<HistoEvent> histos, AtomicInteger sent) {

        RAF raf = null;
        IndexFeed indexStore = null;

        try {
            if (request.isCancelled()) return 0;
            this.sent = sent;

            int hits = 0;


            if (!new File(path).exists()) return 0;

            long searchTime = System.currentTimeMillis();

            indexer.stallIndexingForSearch();

            List<Bucket> buckets = indexer.find(path, request.getStartTimeMs(),	  request.getEndTimeMs());
            logFile = indexer.openLogFile(path);
            this.totalEvents = 0;
            for (Bucket bucket : buckets) {
                this.totalEvents += bucket.numberOfLines();
            }

            FieldSet fieldSet = super.getFieldSet(logFile.fieldSetId.toString(), logFile);
            if (fieldSet == null) {
                LOGGER.error("SEARCH Cancelled, failed to find FieldSet:" + logFile.fieldSetId + " for File:" + path);
                return 0;
            }
            DateTimeFormatter formatter = DateTimeFormat.shortDateTime();
            if (!buckets.isEmpty()) {
                long startTime = buckets.get(0).time();
                long endTime = buckets.get(buckets.size()-1).time();
                raf = RafFactory.getRaf(path, logFile.getNewLineRule());
                if (request.isVerbose()) {
                    String msg = String.format("LOGGER [%s] Found %d buckets to search for time period [%s => %s]", next.toString(), buckets.size(), formatter.print(startTime), formatter.print(endTime));
                    LOGGER.info(msg);
                    if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
                }

                indexStore = fieldSet.getKVStore();
                hits += searchBuckets(request.subscriber(), raf, buckets, request.queries(), histos, searchTime, request, startTime, endTime, fieldSet)[1];
            } else {
                if (request.isVerbose()) {
                    String frange = DateUtil.shortDateTimeFormat7.print(logFile.getStartTime()) + " - " + DateUtil.shortDateTimeFormat7.print(logFile.getEndTime());
                    String msg = String.format("LOGGER [%s] Found [%s]Buckets[%d]Range[%s] buckets to search for time period [%s => %s]", next.toString(), path, buckets.size(), frange, formatter.print(request.getStartTimeMs()), formatter.print(request.getEndTimeMs()));
                    LOGGER.info(msg);
                    if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
                }
            }
            this.hits += hits;


        } catch (ReplayAbortedException t) {
            LOGGER.info(t.getMessage());
            request.cancel();
        } catch (ClosedByInterruptException cbie) {
            LOGGER.info(cbie.getMessage() + " File:" + path);
            request.cancel();
        } catch (Throwable t) {
            LOGGER.error("Scan Failed:" + path + " Tags:" + logFile.getTags(), t);
            if (!t.toString().contains("Aborted")) {
                if (!request.isCancelled() && !request.isExpired()) {
                    LOGGER.warn("Request:" + t.getMessage(), t);
                }
            } else {
                request.cancel();
            }
        } finally {
            try {
                next.flush();
                if (indexStore != null) indexStore.commit();
                finished = true;
                if (raf != null) raf.close();
            } catch (IOException e) {
                LOGGER.warn(e);
            }
        }
        if (!request.isCancelled()) {
            // convert function results
            if (histos != null) {
                for (HistoEvent histo : histos) {
                    histo.writeBucket(request.isVerbose());
                }
            }
        } else {
            LOGGER.info(String.format("LOGGER - Search for subscriber %s was cancelled. Throwing away results, hits:" + hits, request.subscriber()));
        }
        return hits;
    }


    protected LogFile logFile;

    int[] searchBuckets(String subscriber, RAF raf, List<Bucket> buckets, List<Query> queryList, List<HistoEvent> histos, long searchTime, LogRequest request, long fileStartTime, long fileEndTime, FieldSet fieldSet) throws IOException, ReplayAbortedException {
        File file = new File(path);

        boolean verbose = request.isVerbose();
        if (buckets.size() == 0) return new int[] { 0, 0 };
        long scanSTime = DateTimeUtils.currentTimeMillis();
        int totalHits = 0;

        if (request.isVerbose()) {
            String msg = String.format(">> LOGGER Sub[%s] file[%s] Buckets[%d] lines[%d-%d] %s",
                    subscriber, path, buckets.size(), buckets.get(0).firstLine(), buckets.get(buckets.size()-1).lastLine(), queryList);
            LOGGER.info(msg);
            if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
        }

        // NOTE **** locate the RAF before we do a scan. If the index was corrupted then this will
        // skip over any holes in the data and still to a complete scan.
        raf.seek(buckets.get(0).startPos);

        if (fieldSet == null) return new int[] { 0, 0 };

        int scanned = 0;

        boolean isCompressedFile = FileUtil.isCompressedFile(file.getName());
        for (int i = 0; i < buckets.size() && !isFinished(request, buckets.get(i).time(), histos); i++) {
            Bucket bucket = buckets.get(i);

            // dont overscan the end time
            if (bucket.time() > request.getEndTimeMs()){
                continue;
            }

            long position = bucket.startingPosition();
            if (!isCompressedFile && position > raf.length()) {
                String msg = String.format("LOGGER %s sub[%s] pos[%s]  time[%s] position is too great[%d] aborting scan", path, subscriber, position, DateUtil.shortDateTimeFormat2.print(bucket.time()), raf.length());
                throw new RuntimeException(msg);
            }


            if (verbose) {
                String msg = "Scanning:" + bucket;
                LOGGER.info(msg);
            }



            totalHits += scanDataBucket(logFile, raf, queryList, histos, bucket, fileStartTime, fileEndTime, request, fieldSet);
            scanned += bucket.numberOfLines();
        }
        if (verbose) {
            long scanETime = DateTimeUtils.currentTimeMillis();
            String msg = String.format("LOGGER Sub[%s] file[%s] TotalElapsed[%d] Lines[%d] Hits[%d]", subscriber, path, (scanETime - scanSTime), totalEvents, totalHits);
            LOGGER.info(msg);
            LogProperties.logTest(getClass(), msg);
        }
        return new int[] { totalHits, scanned } ;
    }

    private long lastPos = -1;
    Bucket prevBucket = null;
    final private int scanDataBucket(final LogFile logFile, RAF raf, List<Query> queries, List<HistoEvent> histos, Bucket bucket, long fileStartTime, long fileEndTime, LogRequest request, FieldSet fieldSet) throws IOException {

        int hits = 0;


        long position = bucket.startingPosition();



        // We have to seek to each bucket position - otherwise out-of-time-sequence data will have incorrect line numbers
        if (position != lastPos && position != 0 && prevBucket != null) {

// There is (was!) a bug in the FileIngestion
            if (request.isVerbose()) {
                LOGGER.info("FileScanner Seek: PosDelta:" + (position - lastPos) + " ThisStartLine: " + bucket.firstLine() + " PrevEndLine:" + prevBucket.lastLine() + " " + logFile.getFileName()) ;
                LOGGER.info("\tPRE_Bucket:" + prevBucket.toString());
                LOGGER.info("\tNOW_Bucket:" + bucket.toString());
            }
            // - keep the file pos and fudge the line numbers.... FFS
            bucket.firstLine = prevBucket.lastLine+1;
            //raf.seek(position);
        }

        int qPos = 0;
        int misses = 0;
        int linesChecked = 0;
        String nextLine = null;
        try {

            short minLineLength = logFile.getMinLineLength();
            int bucketLine = 0;
            int lastLine = bucket.lastLine();

            for (int lineNo = bucket.firstLine(); lineNo <= lastLine; lineNo += raf.linesRead()) {

                nextLine = raf.readLine(minLineLength);

                if (nextLine == null) return hits;

                int lineLength = nextLine.length();


                // NOTE: can happen on some binary files - so leave this in so we can forge ahead

                if (lineLength == 0) {
                    if (raf.linesRead() == 0) lineNo++;
                    scannedBytes.addAndGet(1);
                    continue;
                }
                scannedBytes.addAndGet(lineLength +1);

                qPos = 0;

                for (Query query : queries) {
                    HistoEvent event = (histos == null || histos.size() == 0) ? null : histos.get(qPos);
                    if (event != null) event.incrementScanned(1);
                    linesChecked++;


                    if (scan(request, fieldSet, nextLine, lineNo, query, bucket.time() + bucketLine, event, fileStartTime, fileEndTime)){
                        if (bucketLine < DateUtil.MINUTE-1) bucketLine++;
                        hits++;
                        this.sent.incrementAndGet();
                    } else {
                        misses++;
                    }
                    qPos++;
                }
            }
            scannedEvents.getAndAdd(bucket.numberOfLines());
            if (hits > 0) {
                SummaryBucket summaryBucket = getSummaryBucket();
                if (summaryBucket != null) {
                    summaryBucket.incrementSystemFields(fieldSet, logFile, hits, bucket.time());
                }
            }
            prevBucket = bucket;
            lastPos = raf.getFilePointer();
        } finally {
            if (request.isVerbose()) {
                String warnMsg = hits == misses && hits == 0 ? " Note:[0 hits & 0 misses] indicate DataType mismatch" : "";
                int hitRate = (int) ((  ( (double) hits ) / bucket.numberOfLines()) * 100.0);
                String msg = String.format("ScanResults FSet:%s Checked:%d Total:%d Range[%d-%d] [Hits:%d Misses:%d Rate:%d]%s",fieldSet.getId(), linesChecked, totalEvents,
                        bucket.firstLine(),bucket.lastLine(), hits, misses, hitRate, warnMsg);
                LOGGER.info(msg);
//				LogProperties.logTest(getClass(), msg);
                if (linesChecked == 0) {
                    LOGGER.info("BAD LINE[" + nextLine + "]");
                }
            }


        }
        return hits;
    }

    public String toString() {
        return  getClass().getSimpleName() + " " + super.toString();
    }

    @Override
    public boolean isComplete() {
        return this.finished;
    }

    public SummaryBucket getSummaryBucket() {
        LogScanningHandler next = this.next;
        while (next != null) {
            if (next instanceof SummaryBucketHandler) return ((SummaryBucketHandler) next).getSummaryBucket();
            if (next instanceof SummaryBucketCachedHandler) return ((SummaryBucketCachedHandler) next).getSummaryBucket();
            next = next.next();
        }
        return null;
    }
}
