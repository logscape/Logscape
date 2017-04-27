package com.liquidlabs.log.reader;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.TimeZoneDiffer;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.BreakRuleUtil;
import com.liquidlabs.common.file.raf.ChunkingRAFReader;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.WriterResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.fields.field.GroupField;
import com.liquidlabs.log.fields.field.LiteralField;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.facetmap.PersistingFacetMap;
import com.logscape.disco.indexer.*;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.streaming.LiveHandler;
import com.liquidlabs.log.util.DateTimeExtractor;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Parses out a chunks of log-lines and tracks them into the data-store-disco (Handler) -
 * */
public class GenericIngester implements LogReader {
    private final static Logger LOGGER = Logger.getLogger(GenericIngester.class);
    private final String hostname;
    private LogFile logFile;
    private final Indexer indexer;
    private Handler client;
    private final String sourceURI;

    private final WatchDirectory watch;
    private long defaultFileTime = -1;
    private String timeFormat;
    private DateTimeExtractor dateTimeExtractor = null;
    private int tzOffsetHours;
    private boolean interrupted;
    private Map<String, LiveHandler> liveHandlers = new ConcurrentHashMap<String, LiveHandler>();
    private long lastWriterTime;
    private long lastPos;
    FieldSet fieldSet;
    private long lastTimeExtracted;
    private int lineFutureSpam;
    IndexFeed kvStore = KvIndexFactory.get();
    static private final EventMonitor eventMonitor = new LoggingEventMonitor();
    private int lineNumber;
    private LineExtractor extractor = new JSONExtractor();


    public GenericIngester(LogFile logFile, Indexer indexer, Handler client, String sourceURI, String hostname, WatchDirectory watch) {

        this.logFile = logFile;
        this.indexer = indexer;
        this.client = client;
        this.sourceURI = sourceURI;
        this.hostname = hostname;
        this.watch = watch;
        this.dateTimeExtractor = new DateTimeExtractor(watch.getTimeFormat());
    }


    @Override
    public WriterResult readNext(long startPos, int startingLine) throws IOException {
        PersistingFacetMap facetSummaryStore = null;

        if (logFile == null) return new WriterResult(new StringBuilder(), startingLine, 0, startPos);

        File file = new File(logFile.getFileName());
        if (defaultFileTime == -1) {
            if (timeFormat == null) {
                if (logFile.getTimeFormat() != null && logFile.getTimeFormat().length() > 0) {
                    timeFormat = logFile.getTimeFormat();
                } else {
                    timeFormat =  dateTimeExtractor.getFormat(file, 100, logFile.getNewLineRule());
                }
                dateTimeExtractor.setExtractorFormat(timeFormat);

                defaultFileTime = dateTimeExtractor.getFileStartTime(file, 100);
            }
        } else {
            defaultFileTime = file.lastModified();
        }
        if (defaultFileTime == 0 || defaultFileTime < new DateTime(file.lastModified()).minusYears(2).getMillis()) {
            defaultFileTime = file.lastModified();
        }

        tzOffsetHours = getTzOffset(logFile.getTags(), logFile);

        // in case the file rolled
        long linePos = startPos;
        lineNumber = startingLine;
        int linesRead = 0;

        client.startChunk();
        ChunkingRAFReader raf = new ChunkingRAFReader(logFile.getFileName(), logFile.getNewLineRule());

        try {
            long fileLength = file.length();
            raf.setStartLine(startingLine);
            List<String> chunk = null;
            List<String> summaryFieldNames = null;
            while ( (chunk = raf.readLines(linePos)) != null) {
                long fileStartTime = logFile.getStartTime();
                for (int lineItem = 0; lineItem < chunk.size(); lineItem++) {
                    String nextLine = chunk.get(lineItem);
                    if (interrupted) return null;
                    long linePosition = raf.getLinePosition(lineItem);
                    lastPos = linePosition;
                    int lineLength = nextLine.length();
                    int linesReadForEvent = raf.getLinesRead(lineItem);
                    linesRead++;


                    long lineTime = fudgeTime(getTime(logFile.getFileName(), lineNumber, nextLine, fileStartTime, defaultFileTime, linePosition, fileLength), lineNumber);

                    setupFieldSet(nextLine);
                    if (summaryFieldNames == null) summaryFieldNames = fieldSet.getStandardFieldNames(true, false);

                    String[] fields = fieldSet.getNormalFields(nextLine);

                    // ===== TODO: the client might not want discovery! - so check the client Forwarder handler actually needs it

                    List<Pair> discovered = new ArrayList<Pair>();
                    if (client.isDiscoveryEnabled()) {
                        discovered = watch.isDiscoveryEnabled() ? kvStore.kvExtractor().getFields(nextLine) : new ArrayList<Pair>();
                        fieldSet.setDiscovered(discovered);
                        List<Pair> indexedFields = fieldSet.getIndexedFields(fields);
                        if (extractor.applies(nextLine)) {
                            discovered.addAll(extractor.extract(nextLine));
                        }
                        discovered.addAll(kvStore.index(logFile.getId(), logFile.getFileName(), lineNumber, lineTime, nextLine, false, watch.isGrokDiscoveryEnabled(), watch.isSystemFieldsEnabled(), indexedFields));
                    }

                    Map<String, String> groupFields = getGroupFields(fieldSet, fieldSet.getFields(), fields);

                    Map<String, String> fieldsAsMap = fieldSet.getFieldsAsMap(-1, nextLine, lineNumber, lineTime, discovered);



                    if (client.isDiscoveryEnabled()) {

                        List<Pair> summaryFields = new ArrayList<>(summaryFieldNames.size());
                        for (String fieldName : summaryFieldNames) {
                            summaryFields.add(new Pair(fieldName, fieldSet.getFieldValue(fieldName, fields)));
                        }
                        if (facetSummaryStore == null) {
                            facetSummaryStore = new PersistingFacetMap(LogProperties.getDBRootForFacets(), logFile.firstLine());
                        }

                        facetSummaryStore.write(logFile.getFileName(), logFile.getId(), summaryFields, discovered, lineNumber);
                    }

                    client.handle(logFile, nextLine, lineLength, lineNumber, lineTime, linePosition, linesReadForEvent, fieldsAsMap, discovered, groupFields);

                    handleLiveQueryEvents(nextLine, lineNumber, lineTime, fields);

                    lineNumber += linesReadForEvent;
                }

                // release the memory before reading next chunk
                chunk.clear();
                linePos = raf.getFilePointer();
            }

            if (linesRead > 0) {
                if (facetSummaryStore != null) facetSummaryStore.flush(logFile.getId());
                client.flush();
                kvStore.commit();
                updateLogFilePosToPreventReTailingOldFile(raf.getMinLineLength());
            }

            checkLiveHandlerExpirey();

            // clear out any cached data to free memory
            if (fieldSet != null) fieldSet.reset();

            return new WriterResult(null,lineNumber, linesRead, linePos);
        } finally {
            raf.close();
        }

    }

    private Map<String, String> getGroupFields(FieldSet fieldSet, List<FieldI> fields, String[] fields1) {
        HashMap<String, String> results = new HashMap<String, String>();
        for (FieldI field : fields) {
            if (field instanceof GroupField) {
                String fieldValue = fieldSet.getFieldValue(field.name(), fields1);
                results.put(field.name(), fieldValue);

            }
        }
        return results;
    }

    private Map<String, String> makeMap(List<FieldI> index) {
        HashMap<String, String> results = new HashMap<String, String>();
        for (FieldI fieldI : index) {
            results.put(fieldI.name(), ((LiteralField)  fieldI).getValue());
        }

        return results;
    }

    private void handleLiveQueryEvents(final String nextLine, int lineNumber, long lineTime, String[] fieldValues) {
        if (setupFieldSet(nextLine)) return;


        for (Map.Entry<String, LiveHandler> handlerEntry :  this.liveHandlers.entrySet()) {
            try {
                LiveHandler handler = handlerEntry.getValue();
                if (handler.isExpired()) this.liveHandlers.remove(handlerEntry.getKey());
                else {
                    handler.handle(this.logFile, logFile.getFileName(), lineTime, lineNumber, nextLine, logFile.getTags(), fieldSet, fieldValues);
                }
            } catch (Throwable t) {
                LOGGER.warn("Live Event Failed:" + t.toString() + " Subscriber:" + handlerEntry.getKey());
            }
        }
    }


    private long fudgeTime(long parsedTime, int lineNumber) {
        long thisLineTime = parsedTime -  (tzOffsetHours * DateUtil.HOUR);
        if (!client.allowClockRewind() && thisLineTime < this.lastWriterTime) {

            if (lineFutureSpam++ % 1000 == 0 && lineNumber < 1000) {
                eventMonitor.raiseWarning(new Event("OutOfSequenceDataDetected").with(logFile.getFileName(), lineNumber),null);
                LOGGER.warn("OutOfSequenceDataDetected:" + logFile.getFileName() + ":" +  lineNumber + " EventCount:" + lineFutureSpam + "\n" +
                        " PrevTime:"  + new DateTime(this.lastWriterTime) + "\n" +
                        " LineTime:"  + new DateTime(thisLineTime)
                );
            }
            thisLineTime = this.lastWriterTime+10;
        }

        if (!client.allowClockRewind() && thisLineTime < this.lastWriterTime - DateUtil.HOUR) {
            eventMonitor.raiseWarning(new Event("OutOfSequenceDataDetected").with(logFile.getFileName(), lineNumber),null);
            LOGGER.warn("OutOfSequenceDataDetected:" + logFile.getFileName() + ":" +  lineNumber + " EventCount:" + lineFutureSpam + "\n" +
                    " PrevTime:"  + new DateTime(this.lastWriterTime) + "\n" +
                    " LineTime:"  + new DateTime(thisLineTime)
            );
            thisLineTime = this.lastWriterTime+10;
        }
        this.lastWriterTime = thisLineTime;
//        if (enableClockDriftAdjustment) this.lastWriterTime = ResourceAgentImpl.getTime(this.lastWriterTime);
        if (lastWriterTime > System.currentTimeMillis() + DateUtil.MINUTE * 15) {
            if (lineFutureSpam++ % 100 == 0 && lineNumber < 1000) {
                eventMonitor.raiseWarning(new Event("FutureOutOfSequenceDataDetected").with(logFile.getFileName(), lineNumber),null);
            }
            lastWriterTime = System.currentTimeMillis();
        }
        return this.lastWriterTime;
    }

    private int formatterAttemptCount;
    private int formatterAttemptLimit = 10 * 1000;
    public long getTime(final String filename, int lineNumber, String lineData, long fileStartTime, long fileLastMod, long filePos, long fileLength) {

        boolean printTimeUsed = false;
        // recheck the timeformat every 1000 lines OR when we dont have a timeformat!
        if (lineNumber % 1000 == 0 ||
                (lineData != null && this.timeFormat == null && (this.formatterAttemptCount <= formatterAttemptLimit))
                )	{
            this.formatterAttemptCount++;
            String formatMaybe = dateTimeExtractor.getFormat(lineData, fileLastMod);
            if (formatMaybe != null) {
                //LOGGER.debug("Using formatter:" + formatMaybe + " for file:" + filename + " attempt:" + this.formatterAttemptCount);
                if (lineNumber < 1000) printTimeUsed = true;
                dateTimeExtractor.setExtractorFormat(formatMaybe);
                this.timeFormat = formatMaybe;
            } else {
                // still not got a formatter so give up....
                dateTimeExtractor.clearGrabbers();
            }
        }
        try {
            if (this.timeFormat == null || this.timeFormat.length() == 0) {

                return handleUnableToParseLineTime(filename, fileStartTime, fileLastMod, filePos, fileLength, lineNumber);
            } else {
                long timeToDefault = lastTimeExtracted > 0 ? lastTimeExtracted : fileLastMod;

                // if we are tailing it then default to now when we cant parse it
                if (fileLastMod < System.currentTimeMillis() - DateUtil.MINUTE * 5 && fileLength - filePos < 1024) timeToDefault = fileLastMod;


                Date date = dateTimeExtractor.getTimeUsingFormatter(lineData, timeToDefault);

                if (date == null) {
                    return handleUnableToParseLineTime(filename, fileStartTime, fileLastMod, filePos, fileLength, lineNumber);
                }

                // clock rewind when no format
                if (date.getTime() < lastTimeExtracted || timeFormat.length() == 0) {
                    return lastTimeExtracted;
                }

                lastTimeExtracted = date.getTime();// ResourceAgentImpl.getTime(date.getTime()); - clock drifting??
            }
        } catch (Throwable t) {
            return handleUnableToParseLineTime(filename, fileStartTime, fileLastMod, filePos, fileLength, lineNumber);
        }
        if (printTimeUsed && lineNumber < 5000) {
            LOGGER.debug("Extracting firstTime:" + DateTimeFormat.mediumDateTime().print(lastTimeExtracted) + " file:" + filename + ":" + lineNumber);
        }
        return lastTimeExtracted;
    }

    private long handleUnableToParseLineTime(String filename, long fileStartTime, long fileEndTime, long fileCurPos, long fileLength, int lineNumber) {
        // if lastTime was found - increment lastTime for each missed line
        if (fileStartTime == 0) fileStartTime = new DateTime(fileEndTime).minusDays(1).getMillis();
        long result = fileEndTime;
        // if the file is live and we are processing a recent line (i.e. < 2 mins old) - use the current lastMod
        if (fileCurPos > fileLength * 0.8 && fileEndTime > new DateTime().minusHours(1).getMillis()) {
            result = fileEndTime;
        } else {
            int fileSecs = getFileSecs(fileStartTime, fileEndTime, fileCurPos, fileLength);
            // if this is the first line to be extracted - then reverse engineer time from lastMod
            result = new DateTime(fileStartTime).plusSeconds(fileSecs).getMillis();
        }
        // try and fall back on bad time extraction
        if (result < lastTimeExtracted || result == 0) {
            // need to remove tz offset because we are adding it again below
            result = lastTimeExtracted - tzOffsetHours * DateUtil.HOUR;
        }
        if (result == 0) result = fileEndTime;
        if (result == 0) result = new File(filename).lastModified();
        if (result == 0) result = System.currentTimeMillis();

        if (result > new DateTime().plusDays(1).getMillis()) result = DateTimeUtils.currentTimeMillis();

        printTimeWarning(filename, lineNumber, fileEndTime, result, fileCurPos, fileLength);

        result = result + tzOffsetHours * DateUtil.HOUR;
        lastTimeExtracted = result;
        return result;
    }
    private  int warnCount;
    private void printTimeWarning(String filename, int lineNumber, long lastMod, long result, long fileCurPos, long fileLength) {
        // if we are near the end then dont print antything
        if (fileCurPos > fileLength * 0.99) return;
        DateTimeFormatter formatter = DateTimeFormat.mediumDateTime();
        if (lineNumber < 20 || warnCount % LogProperties.getTimeDetectWarnMod() == 0) {
            LOGGER.warn(String.format("LOGGER TAILER - [suppressed %s msgs], failedTimeParse[%s:%d] timeFormat[%s] lastExtracted:%s  lastMod:%s using:%s",
                    warnCount, filename, lineNumber, this.timeFormat, formatter.print(this.lastTimeExtracted), formatter.print(lastMod), formatter.print(result)));
        }
        this.lastTimeExtracted = result;
        warnCount++;
    }


    public int getFileSecs(long fileStartTime, long fileEndTime, long fileCurPos, long fileLength) {
        long totalTimeSeconds = (fileEndTime - fileStartTime)/1000;
        double percent = (double) fileCurPos/ (double)fileLength;
        int fileSecs = (int) (totalTimeSeconds * percent);
        return fileSecs;
    }

    private void updateLogFilePosToPreventReTailingOldFile(int minLineLength) {
        // now update the logFile with the fileLength so we dont retail the old file
        LogFile logFile1 = indexer.openLogFile(this.logFile.getFileName());
        logFile1.setPos(new File(this.logFile.getFileName()).length());
        logFile1.setMinLineLength(minLineLength);
        indexer.updateLogFile(logFile1);

    }

    static public short getTzOffset(String filetag, LogFile logFile) {


        String fileName = logFile.getFileName();
        if (fileName.contains("_SERVER_")) {
            return getTzOffSetFromServerDir(fileName);
        }
        String[] tags = filetag.split(",");

        Properties properties = new Properties();
        File file = new File("downloads/datasource.properties");
        if (!file.exists()) file = new File("build/datasource.properties");
        if (!file.exists()) return 0;
        try {
            properties.load(new StringReader(FileUtil.readAsString(file.getAbsolutePath())));
            for (String tag : tags) {
                String tzToUse = (String) properties.get(tag.trim() + ".timezone");
                if (tzToUse != null) {
                    return (short) ((int) (TimeZoneDiffer.getHoursDiff(tzToUse)) * -1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    static short getTzOffSetFromServerDir(String fileName)  {
        short tzOffset = 0;
        String serverName = LogFile.getHostnameFromPath(fileName)[0];
        String path = fileName.substring(0, fileName.indexOf(serverName) + serverName.length());
        if (new File(path,"datasource.properties").exists()) {
            Properties properties = new Properties();
            try {
                properties.load(new StringReader(FileUtil.readAsString(new File(path,"datasource.properties").getAbsolutePath())));
                String tzToUse = (String) properties.get("source.timezone");
                if (tzToUse != null) {
                    tzOffset =  (short) ((int) (TimeZoneDiffer.getHoursDiff(tzToUse)) * -1);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return tzOffset;
    }

    private boolean setupFieldSet(String nextLine) {
        if (fieldSet == null) {
            fieldSet = indexer.getFieldSet(logFile.getFieldSetId());
            if (fieldSet == null) {
                LOGGER.warn("Didnt find fieldSet:" + logFile.getFieldSetId() + " File:" + logFile.getFileName());
                fieldSet = FieldSets.getBasicFieldSet();
            }
            fieldSet.example = null;
            fieldSet.addDefaultFields(fieldSet.getId(), logFile.getFileHost(NetworkUtils.getHostname()), FileUtil.getFileNameOnly(logFile.getFileName()), logFile.getFileName(), logFile.getTags(), VSOProperties.getResourceType(), sourceURI + logFile.getFileName(), nextLine.length(), true);
            fieldSet.enhance();
        }
        return false;
    }


    @Override
    public Indexer getIndexer() {
        return indexer;
    }
    private void checkLiveHandlerExpirey() {
        Set<Map.Entry<String, LiveHandler>> keySet = this.liveHandlers.entrySet();
        for (Map.Entry<String, LiveHandler> entry : keySet) {
            LiveHandler liveHandler = entry.getValue();
            if (liveHandler != null && liveHandler.isExpired()) {
                this.liveHandlers.remove(entry.getKey());
            }
        }
    }

    /**
     * Only live-event on recent files where we are near the head
     * @param linePosition
     * @param fileLength
     * @param lastTime
     * @return
     */
    private boolean isLiveEventing(long linePosition, long fileLength, long lastTime) {
        if (lastTime > System.currentTimeMillis() - DateUtil.HOUR){
            double percent = (double) linePosition/ (double)fileLength;
            return percent > 0.9 || linePosition > fileLength - 5024;
        }
        return  false;
    }


    @Override
    public void setFilters(String[] includes, String[] excludes) {
    }

    @Override
    public void roll(String from, String to) {
        client.roll(from, to);
    }

    public void roll(String rolledFrom, String rolledTo, long currentPos, int line) {
        client.roll(rolledFrom, rolledTo, currentPos, line, sourceURI, hostname, watch);
    }
    public long getLastTimeExtracted() {
        return lastWriterTime;
    }

    @Override
    public void deleted(String filename) {
        this.client.deleted(filename);
        //this.client.flush();
    }

    @Override
    public void addLiveHandler(LiveHandler liveHandler) {
        this.liveHandlers.put(liveHandler.subscriber(), liveHandler);
    }

    @Override
    public void setLogId(LogFile logFile) {
        this.logFile = logFile;
        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("Set LogId[%s][%d]", logFile.getFileName(), logFile.getId()));

    }

    @Override
    public void stopTailing() {
        this.client.flush();
        this.interrupt();
    }

    @Override
    public void interrupt() {
        this.client.flush();
        this.interrupted = true;
    }

    @Override
    public void setLogFiletype(String fieldSetId) {
    }

    public String toString() {
        return super.toString() + " LastTimeParsed:" + new DateTime(lastTimeExtracted) + " lastPos:" + this.lastPos + " Live:" + this.liveHandlers.values();
    }

    @Override
    public int currentLine() {
        return lineNumber;
    }
}
