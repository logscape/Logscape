package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.BreakRuleUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSetAssember;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.roll.ContentBasedSorter;
import com.liquidlabs.log.roll.RollDetector;
import com.liquidlabs.log.roll.RolledFileSorter;
import com.liquidlabs.log.roll.Roller;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.streaming.LiveHandler;
import com.liquidlabs.log.util.DateTimeExtractor;
import com.liquidlabs.transport.serialization.Convertor;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class TailerImpl implements Tailer {

    private static final Logger LOGGER = Logger.getLogger(TailerImpl.class);

    private static final int TRUNCATE_FIRST_LINE_SIZE = LogProperties.getMaxFirstLineSize();


    private String fieldSetId;
    private int logId = 0;
    private File logFile;
    private long currentPos;
    int line = 1;
    private static AtomicInteger backlog = new AtomicInteger();
    private String logCanonical;

    private final LogReader writer;
    private int failureCount = 0;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private WatchDirectory watch;

    private int scheduleSeconds = 1;
    private int noDataSeconds;
    private long lastRan = 0;
    private Future<TailResult> future;

    private AgentLogService agentLogService;
    private boolean importingFirstPass = true;
    private boolean cancelled = false;
    private final Indexer indexer;
    private Set<String> rolledFilenames = new HashSet<String>();

    RollDetector rollDetector  = new RollDetector();

    String firstFileLineForRollDetection = "";

    static private int initialScanCount = LogProperties.getInitialScanSize();
    static private int fileScanMinCheckSeconds = LogProperties.getFileScanMinCheck();
    static private int fileScanMaxCheckSeconds = LogProperties.getFileScanMaxCheck();
    static private int fileScanFallIntervalMins = LogProperties.getFileScanInterval0();
    static private int fileScanFallIntervalMax = LogProperties.getFileScanInterval1();
    static private final EventMonitor eventMonitor = new LoggingEventMonitor();


    public TailerImpl(File logfile, long givenFilePos, int givenLine, LogReader writer, WatchDirectory watch, Indexer indexer) throws IOException {
        this.logFile = logfile;
        this.writer = writer;
        this.watch = watch;
        this.indexer = indexer;
        this.logCanonical = FileUtil.getPath(logfile);
        backlog.incrementAndGet();

        if (LOGGER.isDebugEnabled()) {
            String msg = String.format(" >> Starting Tail file:%s line:%d pos:%d size:%d Tag:%s Backlog:%d", logFile.getName(), givenLine, givenFilePos, logFile.length(), watch.getTags(), backlog.get());
            LOGGER.debug(msg);
            if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
        }

        // start with an unknown timeformat
        LogFile openLogFile = indexer.openLogFile(logCanonical);
        if (openLogFile == null) {
            LOGGER.fatal("Failed to open LogFile:" + logCanonical);
            return;
        }
        logPotentialErrorMsgs(watch, openLogFile);
        writer.setLogId(openLogFile);
        writer.setLogFiletype(openLogFile.fieldSetId.toString());
        this.fieldSetId = openLogFile.fieldSetId.toString();
        logId = openLogFile.getId();
        firstFileLineForRollDetection = openLogFile.firstLine();
        rollDetector.setDetectorClass(watch.getFileSorter());
        setupLogFileToData(logfile, watch, indexer, openLogFile);

        // note: we need to increment the line number upon restart of tailing the file.... i.e. system was stopped
        if (givenFilePos == 0) givenLine = 1;
        else givenLine++;
        // need these values in case there is an initial roll detected
        this.currentPos = givenFilePos;
        this.line = givenLine;


        assignFieldSetInCaseWeMissedIt();

        rollDetector.setVerbose(false);


        this.currentPos = givenFilePos;
        this.line = givenLine;
        eventMonitor.raise(new Event("StartTailing")
                .with("logId", logId)
                .with("tag", openLogFile.getTags())
                .with("file", logFile.getAbsolutePath()));

        String msg2 = String.format(" << Started Tailing file:%s id:%d pos:%d line:%d", logFile.getName(), logId, currentPos, givenLine);
        LOGGER.debug(msg2);
        if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg2);
    }

    private boolean setupLogFileToData(File logfile, WatchDirectory watch, Indexer indexer, LogFile openLogFile) throws IOException {
        if (logfile.length() > 0 &&
                openLogFile.getNewLineRule().equals(BreakRule.Rule.Default.name()) ||
                openLogFile.firstLine() == null || openLogFile.firstLine().length() == 0
                ) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Setting up LogFile:" + logCanonical);

            final String line = FileUtil.getLine(logfile, 1);
            openLogFile.setFirstLine(truncate(line));

            /**
             * Setup the NEWLINE RULE
             */
            List<String> readLines = null;
            if (FileUtil.isCompressedFile(logfile.getName())){
                readLines = FileUtil.readLines(openLogFile.getFileName(), 1, initialScanCount, openLogFile.getNewLineRule());
            } else {
                readLines = FileUtil.readLines(openLogFile.getFileName(), openLogFile.getLineCount() - 100, initialScanCount, openLogFile.getNewLineRule());
            }
            openLogFile.setNewLineRule(BreakRuleUtil.getStandardNewLineRule(readLines, watch.getBreakRule(), ""));


            /**
             * Setup the TimeFormatter
             */
            if (openLogFile.getTimeFormat() == null || openLogFile.getTimeFormat().length() == 0) {
                DateTimeExtractor timeExtractor = new DateTimeExtractor(watch.getTimeFormat());
                String timeFormat = timeExtractor.getFormat(logFile, readLines);
                openLogFile.setTimeFormat(timeFormat);
                if (timeFormat != null) {
                    String lastLime = readLines.get(readLines.size()-1);
                    if (lastLime.length() > 0) {
                        Date extractTime = timeExtractor.getTime(lastLime, logFile.lastModified());
                        if (extractTime != null) openLogFile.setEndTime(extractTime.getTime());
                    }
                }
            }
            indexer.updateLogFile(openLogFile);
            firstFileLineForRollDetection = openLogFile.firstLine();
            return true;
        } else {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("NOT Setting up LogFile:" + logCanonical + " length:" + logfile.length() + " flineLength:" + openLogFile.firstLine().length());
            return false;
        }
    }

    private String truncate(String line) {
        if (line == null) return "";
        if (line.length() < TRUNCATE_FIRST_LINE_SIZE) {
            return line.trim();
        }
        return line.substring(0, TRUNCATE_FIRST_LINE_SIZE).trim();
    }

    private void logPotentialErrorMsgs(WatchDirectory watch, LogFile openLogFile) {
        try {
            if (openLogFile != null && !openLogFile.getTags().equals(watch.getTags())) {
                String emsg = "File was previously added using:" + openLogFile.getTags() + " And now given:" + watch.getTags() + " File:" + logCanonical + " ** Check for overlapping DataSources **";
                LOGGER.warn(emsg);
                if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), "ERROR ** " + emsg);
            }
        } catch (Throwable t) {
            LOGGER.error("LogErrorFailed", t);
        }
    }

    public void setLogService(AgentLogService service) {
        this.agentLogService = service;
    }

    /**
     * Called when we need to stop handling a file - i.e. it is being removed
     */
    public void interrupt() {
        this.cancelled  = true;
        writer.interrupt();
        if (LOGGER.isDebugEnabled()) LOGGER.debug("LOGGER - Tailer Interrupted:" + logCanonical);
        if (future != null) future.cancel(false);
        agentLogService.remove(this);
    }
    public String fileTag() {
        return watch.getTags();
    }
    public String fieldSetId() {
        return fieldSetId;
    }
    public void setWatch(WatchDirectory newWatchDirectory) {
        this.watch = newWatchDirectory;
    }

    public static int getBacklog(){
        return backlog.get();
    }

    public TailResult call() {
        if(importingFirstPass) backlog.decrementAndGet();

        TailResult result = TailResult.NO_DATA;
        if (cancelled) return result;
        try {

            long lastModified = logFile.lastModified();
            if (watch.isTooOld(lastModified)) return TailResult.FAILED;

            boolean fileExists = true;
            if (lastModified == 0) {
                fileExists = logFile.exists();
            }
            long newPos = logFile.exists() ? logFile.length() : -1;
            boolean rolled = false;

            if (!fileExists || (firstFileLineForRollDetection != null && rollDetector.hasRolled(logFile, firstFileLineForRollDetection, currentPos, lastRan))) {
                result = roll("Roll Detected:" + rollDetector.getReason());
                rolled = result == TailResult.DATA;
            }
            if (!rolled && (fileExists && (importingFirstPass || newPos != currentPos))) {
                boolean fromStart = this.line == 1;
                if (importingFirstPass && fromStart) {
                    String msg = ">> Importing:" + logCanonical + " source:" + this.watch.getTags() + " type:" + fieldSetId;
                    LOGGER.debug(msg);
                    if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
                }
                result = tail();
                if (importingFirstPass && fromStart) {
                    String msg = "<< Import Finished:" + logCanonical + " ElapsedSecs:" + (System.currentTimeMillis() - lastRan)/DateUtil.SECOND;
                    LOGGER.debug(msg);
                    if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
                }
            }

        } catch (FileNotFoundException e) {
            try {
                result = roll("File Not Found");
            } catch (IOException e1) {
                LOGGER.info("Error:" + e1);
            }

        } catch (IOException e) {
            failed(String.format("IOException processing %s - retry", logFile), e);
        } catch (Throwable e) {
            e.printStackTrace();;

            LOGGER.error("Failed to process:" + logFile + " pos:" + currentPos, e);
            if (e.toString().contains("LogFileNotFound")) {
                failed(String.format("Exception processing %s - giving up", logFile), e);
                return TailResult.FAILED;
            }
            return failed(String.format("Exception processing %s - retry", logFile), e);
        } finally {
            importingFirstPass = false;
            lastRan = System.currentTimeMillis();
        }
        enqueuNext(result);
        return result;
    }

    private void enqueuNext(TailResult result) {
        try {
            if (cancelled) return;

            long lastMod = Math.max(writer.getLastTimeExtracted(), logFile.lastModified());

            if (FileUtil.isCompressedFile(logCanonical) || lastMod < new DateTime().minusHours(LogProperties.getMaxTailDequeueHours()).getMillis() ) {
                LogFile ff = indexer.openLogFile(logCanonical);
                // allow it to be appended if its non-compressed
                if (ff == null) {
                    return;
                }

                LOGGER.info("ImportComplete file::" + logCanonical + " pos:" + ff.getPos()  + " line:" + line + " lastTime:" + DateUtil.shortDateTimeFormat1.print(lastMod));

                final Event event = new Event("ImportComplete")
                        .with("tag", ff.getTags())
                        .with("file", logCanonical)
                        .with("pos", ff.getPos())
                        .with("sizemb", logFile.length() / FileUtil.MEGABYTES)
                        .with("lastTime", DateUtil.shortDateTimeFormat1.print(lastMod))
                        .with("elapsed", System.currentTimeMillis() - lastRan);
                eventMonitor.raise(event.with("MaxAgeForTailing", "True"));



                ff.setAppendable(!FileUtil.isCompressedFile(logCanonical));
                indexer.updateLogFile(ff);
                writer.stopTailing();
                if (agentLogService != null) agentLogService.stopTailing(this);
                return;
            }


            if (logFile.exists() && watch.isTooOld(logFile.lastModified())) {
                LOGGER.info("File-Expired:" + logFile.getName() + " - has reached end-of-watch period :" + watch.getTags() + "/" + watch.getMaxAge() + " days, lastMod:" + DateUtil.shortDateTimeFormat1.print(logFile.lastModified()));

                if (agentLogService != null) agentLogService.stopTailing(this);
                writer.stopTailing();
                return;

            } else if (result == TailResult.DATA || result == TailResult.NO_DATA) {
                updateSchedule();
            }
            if (result != TailResult.FAILED) {
                if (agentLogService != null) agentLogService.enqueue(this, noDataSeconds, scheduleSeconds);
                //else LOGGER.warn("cannot enqueue - filteredLogServiceSet not set");
            }
            if (result == TailResult.FAILED) {
                if (agentLogService != null) agentLogService.stopTailing(this);
                writer.stopTailing();
            }
            if (line < 10 && watch.isRolling() && agentLogService.isRollCandidateForTailer(this.logCanonical, watch.getTags())) {
                LOGGER.info("Stop Tailing LOG-Roll-Target");
                if (agentLogService != null) agentLogService.stopTailing(this);
                writer.stopTailing();
            }
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
        }

    }

    private TailResult failed(String reason, Throwable e) {

        failureCount++;
        updateSchedule();
        if (failureCount == 3) {
            try {
                LOGGER.info(String.format("Fail:%s : %s count:%d", reason, e.getMessage(), failureCount));
                LOGGER.warn(reason, e);
                if (agentLogService != null) agentLogService.remove(this);

            } catch (Throwable t){}
            return TailResult.FAILED;
        }
        return TailResult.NO_DATA;
    }

    private void updateSchedule() {
        noDataSeconds = getNoDataSeconds(writer.getLastTimeExtracted());
        scheduleSeconds = calculateRescheduleSeconds(noDataSeconds);
    }
    /**
     * Basic reschedule calculation
     * Values (default - 1 second up to 3 hours, 3 seconds up to 6, 15 seconds up to max, then 30 seconds
     */
    public int calculateRescheduleSeconds(int noDataSeconds) {
        // under 1 minute   (60 secs -> 15
        int ageInMins = noDataSeconds / 60;
        if (ageInMins < 60) {
            int result = fileScanMinCheckSeconds/4;
            if (result == 0) result = fileScanMinCheckSeconds;
            return result;
        }
        // make it 1 seconds per unit hour...
        if (noDataSeconds < fileScanFallIntervalMax * 60) {
            double noDataMins = noDataSeconds / 60.0;
            int noDataHours = (int) (noDataMins / 60.0);
            noDataHours *= 2;
            return fileScanMinCheckSeconds * noDataHours +1;
        }

        return fileScanMaxCheckSeconds;
    }


    Thread tailThreadToTerminate = null;
    private TailResult tail() throws IOException {

        if (FileUtil.isCompressedFile(this.logCanonical) && currentPos > 0) return TailResult.NO_DATA;

        if (cancelled) {
            LOGGER.debug("Tailer was cancelled");
            return TailResult.FAILED;
        }
        // use last modified after we have done an initial import (i.e. first pass line == 1)
        tailThreadToTerminate = Thread.currentThread();

        if (line < 100) {
            // try and revalidate the timeFormat - if the file was truncated then this data could be invalid
            LogFile openLogFile = indexer.openLogFile(logCanonical);
            if (openLogFile != null) {
                setupLogFileToData(logFile, watch, indexer, openLogFile);
            } else {
                LOGGER.error("Didnt find LogFile in Index:" + logCanonical);
                return TailResult.FAILED;
            }
        }


        WriterResult writerResult = writer.readNext(currentPos, line);

        // interrupted
        if (writerResult == null || cancelled) {
            LOGGER.debug("Interrupted:" + this.filename());
            return TailResult.FAILED;
        }
        tailThreadToTerminate = null;

        // if the file changed check that it may now be suitable for a fieldset (if its pointing to basic)
        assignFieldSetInCaseWeMissedIt();

        line = writerResult.currentLine;
        currentPos = writerResult.currentPosition;

        return TailResult.DATA;
    }

    long lastTimeChecked = 0;
    private void assignFieldSetInCaseWeMissedIt() {

        if (LogProperties.isForwarder) return;
        if (!isBasicType()) return;



        long minuteSinceChecked = (System.currentTimeMillis() - lastTimeChecked)/DateUtil.MINUTE;
        if (LOGGER.isDebugEnabled()) LOGGER.debug("ASSIGN TypeCheck CHECKING:" + logCanonical + ":Basic:" + isBasicType() + " line:" + line + " lastCheckedElapsed:" + minuteSinceChecked);

        if (isBasicType() && (line < 500 || (minuteSinceChecked >= LogProperties.checkFieldSetAssignmentIntervalMinutes()))) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("ASSIGN TypeCheck REVAL Type:" + logCanonical + " Secs:" + minuteSinceChecked);
            lastTimeChecked = System.currentTimeMillis();
            LogFile openLogFile = indexer.openLogFile(logCanonical);
            String fieldSetId2 = openLogFile.fieldSetId.toString();
            if (openLogFile != null && fieldSetId2.equals(FieldSet.BASIC)) {
                try {
                    // grab all fieldsets = it might be that the file-format has changed so we need to reval everything
                    List<FieldSet> fieldSets = indexer.getFieldSets(new Indexer.Filter<FieldSet>(){
                        public boolean accept(FieldSet fieldSet) {
                            return true;
                        }});

                    // read the tail of the file
                    List<String> lines = FileUtil.readLines(logCanonical, openLogFile.getLineCount() - 100, initialScanCount, openLogFile.getNewLineRule());
                    FieldSet fieldSet = new FieldSetAssember().determineFieldSet(logCanonical, fieldSets, lines, false, openLogFile.getTags());
                    if (fieldSet != null && !fieldSet.id.equals(FieldSet.BASIC)) {
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("ASSIGN TypeCheck :" + fieldSet.id + " -> " + logCanonical);
                        indexer.assignFieldSetToLogFile(openLogFile.getFileName(), fieldSet.id);
                        this.fieldSetId = fieldSet.id;
                        indexer.sync();
                    } else {
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("ASSIGN TypeCheck Didnt find matching fieldset for:" + logCanonical);
                    }
                } catch (Throwable t) {
                    LOGGER.warn("Failed to assignFieldSet:" + t.toString(), t);
                }
            } else {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("ASSIGN TypeCheck File:" + logCanonical + " type:" + fieldSetId2);
            }
        }
    }

    private boolean isBasicType() {
        return this.fieldSetId.equals(FieldSet.BASIC);
    }

    int getNoDataSeconds(long lastMod) {
        // reschedule based upon how old the file is -
        long lastModMsSecondsAgo = (DateTimeUtils.currentTimeMillis() - lastMod);
        long lastModSecondsAgo = lastModMsSecondsAgo/1000;
        return (int) (lastModSecondsAgo);
    }




    public boolean isRollCandidate(String otherFile, String tag)  {
        if (!watch.isRolling()) return false;
        boolean rollCandidate = RollDetector.isRollCandidate(logCanonical, otherFile);
        return tag.equals("") && rollCandidate || rollCandidate && tag.equals(watch.getTags());
    }

    private TailResult roll(String reason) throws IOException {
        lock();
        if (LOGGER.isInfoEnabled()) LOGGER.info("Rolling:" + super.toString() + ":" +  logCanonical + " reason:" + reason);


        LogFile openLogFile = indexer.openLogFile(logCanonical);
        String tailingFile = this.logCanonical;
        try {
            // pause to allow for new log file writing....
            Thread.sleep(1000);

            Roller roller = new Roller(logCanonical, logFile.getName(), logFile.getParent(), FileUtil.getPath(logFile.getParentFile()), currentPos, writer, (RolledFileSorter) Convertor.clone(watch.getFileSorter()), indexer);
            String rolledTo = null;
            if (rollDetector.isRollDetection()) {
                rolledTo = roller.roll(line, firstFileLineForRollDetection);
            }

            // Found a ROLL Target
            if (rolledTo != null) {
                if (ContentBasedSorter.isNumericalRollFile(rolledTo)){
                    final Event event = new Event("ROLL_FILE").with("file", logCanonical);
                    eventMonitor.raise(event.with("NumericRolledTo", rolledTo).with("reason", reason));
                    roller.doNumericRoll(eventMonitor, logCanonical, rolledTo, writer);
                    writer.roll(logCanonical, rolledTo, currentPos, line);
//                    // we are numeric so keep tracking this file....so the next time it rolls we can detect it
//                    logCanonical = rolledTo;
//                    logFile = new File(logCanonical);
                    // One tailer per rolling file instance
                    // Tailer ONE  file.log.2 -> .3 -> .4 etc
                    // Tilae TWO file.log -> file.log.1 etc

                } else {
                    final Event event = new Event("ROLL_FILE").with("file", logCanonical);
                    eventMonitor.raise(event.with("rolledTo", rolledTo).with("reason", reason));
                    indexer.rolled(logCanonical, rolledTo);

                    // we have been offline when the file rolled
                    if (rolledTo != null && currentPos < new File(rolledTo).length()) {
                        long delta = new File(rolledTo).length() - currentPos;
                        LOGGER.warn("Tailer didnt catch the last of the file - currentPos:" + this.currentPos + " actual:"+ new File(rolledTo).length() + " Delta:" + delta);
                    }
                    writer.roll(logCanonical, rolledTo, currentPos, line);
                    if (rolledTo != null) {
                        this.rolledFilenames.add(rolledTo);
                    }
                }
            }   else {
                eventMonitor.raise(new Event("roll-restarting").with("file", logCanonical).with("reason", "'no replacement found'"));
                handleMissingRollDestination();
                this.rolledFilenames.remove(logCanonical);
            }


            // if the file exists then setup the disco again
            if (new File(logCanonical).exists()) {
                // reset to the new file
                resetFilePointers(openLogFile.getFieldSetId());
                return TailResult.DATA;
            } else {
                // make it fail out
                LOGGER.info("LOGGER File is now missing, stopping Tailing file:" + logCanonical);
                failureCount = 100;

                // remove this file - as the contents should be rolled
                this.rolledFilenames.remove(logCanonical);

                return TailResult.FAILED;
            }
        } catch (Throwable t) {
            if (logFile.exists()) {
                LOGGER.warn("LOGGER roll " + this.filename() + " had a problem but file exists, starting from beginning", t);

                handleMissingRollDestination();
                resetFilePointers(fieldSetId);
                return TailResult.DATA;
            } else {
                LOGGER.warn("LOGGER roll failed - file:" + this.logCanonical + " deleted and no roll found - removing from index", t);
                handleMissingRollDestination();
                return TailResult.FAILED;
            }
        } finally {
            if (agentLogService != null) agentLogService.roll(tailingFile);
            lock.writeLock().unlock();
        }
    }

    private void handleMissingRollDestination() {
        writer.deleted(logCanonical);
        LogFile logFile = indexer.openLogFile(logCanonical);
        if (logFile != null) indexer.removeFromIndex(java.util.Arrays.asList(logFile));
    }

    private void resetFilePointers(String fieldSetId) throws IOException {

        // the rolled-to log file is preserved, so the logId is now going to be different, redetect everything, timeformat, break rule etc.
        LogFile openLogFile = indexer.openLogFile(logCanonical, true, fieldSetId, watch.getTags());
        LOGGER.debug("Reset FilePointers:" + logCanonical + "[" + openLogFile.getId() +"]");
        openLogFile.setFirstLine("");
        setupLogFileToData(this.logFile, watch, indexer, openLogFile);

        writer.setLogId(openLogFile);
        writer.setLogFiletype(openLogFile.fieldSetId.toString());
        this.firstFileLineForRollDetection = openLogFile.firstLine();
        this.line = 1;
        this.currentPos = 0;
        this.scheduleSeconds = 1;
        this.noDataSeconds = 0;
    }

    private void lock() {
        while(!lock.writeLock().tryLock()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setFilters(String[] includes, String[] excludes) {
        writer.setFilters(includes, excludes);
    }

    public ReadWriteLock getLock() {
        return lock;
    }

    public boolean terminateIfMatches(WatchDirectory watchDir) {
        return true;
    }

    public void future(Future<TailResult> schedule) {
        this.future = schedule;
    }


    public boolean isFor(String other) {
        if (rolledFilenames.size() > 0 && rolledFilenames.contains(other)) return true;
        return other.equals(logCanonical);
    }

    public boolean hasRun() {
        return future.isDone();
    }


    public String filename() {
        return logCanonical;
    }
    public long lastMod() {
        return writer.getLastTimeExtracted();
    }

    @Override
    public boolean matchesPath(String fileAndPath) {
        String givenPath = FileUtil.getPath(fileAndPath);
        String myPath = FileUtil.getPath(logFile);
        return myPath.equals(givenPath);
    }

    public String toString(){
        DateTimeFormatter formatter = DateTimeFormat.shortDateTime();
        String tag = "___";
        try {
            LogFile openLogFile = indexer.openLogFile(logCanonical);
            tag = openLogFile.getTags();
        } catch (Throwable t) {

        }
        return String.format("Tailer file:%s logId:%d tag:%s length:%d line:%s type:%s schedule:%d noDataSecs:%d pos:%d lastMod:%s Ingester:%s LastRan:" + new DateTime(lastRan) + " TypeCheck:" + new DateTime(this.lastTimeChecked),
                FileUtil.getPath(logFile), logId, tag, logFile.length(), line, this.fieldSetId, scheduleSeconds, noDataSeconds, currentPos, formatter.print(logFile.lastModified()), writer.toString());
    }


    public Event fillIn(Event e) {
        return e.with("file", FileUtil.getPath(logFile)).with("logId", logId);
    }

    @Override
    public void stop() {
        writer.stopTailing();
    }

    public long getCurrentPos() {
        return currentPos;
    }

    public int getLine() {
        return line;
    }
    @Override
    public WatchDirectory getWatch() {
        return watch;
    }

    @Override
    public LogReader getWriter() {
        return writer;
    }

    public void addLiveHandler(LiveHandler liveHandler) {
        writer.addLiveHandler(liveHandler);
    }
}
