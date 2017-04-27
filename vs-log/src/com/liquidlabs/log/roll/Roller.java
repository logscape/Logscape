/**
 *
 */
package com.liquidlabs.log.roll;

import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;

public class Roller {

    private static final Logger LOGGER = Logger.getLogger(Roller.class);
    public static final Integer MAX_NUMERICAL_ROLL = Integer.getInteger("numeric.roll.max", 20);
    private final long lastPos;
    private final Indexer indexer;
    private final LogReader logReader;
    private final RolledFileSorter fileSorter;

    private String logCanonical;
    private final String logName;
    private final String parentName;
    private final String parentCanonical;


    public Roller(String logCanonical, String logName, String parentName, String patentCanonical, long lastPos, LogReader logReader, RolledFileSorter fileSorter, Indexer indexer) {
        this.logName = logName;
        this.parentName = parentName;
        this.parentCanonical = patentCanonical;
        this.lastPos = lastPos;
        this.logReader = logReader;
        this.fileSorter = fileSorter;
        this.logCanonical = logCanonical;
        this.indexer = indexer;
    }

    public String roll(int line, String firstFileLine) throws InterruptedException {
        flushChannel(line);
        String rolledTo = null;

        final Set<String> alreadyIndexedFiles = new HashSet<String>();
        this.indexer.indexedFiles(new DateTime().minusDays(1).getMillis(), System.currentTimeMillis(), false, new LogFileOps.FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {
                if (logFile.getFileName().contains(parentCanonical)) alreadyIndexedFiles.add(logFile.getFileName());
                return  false;
            }
        });

        String[] files = null;

        try {
            files = fileSorter == null ? new String[0] : fileSorter.sortedFileNames(true, logCanonical, logName, parentName, parentCanonical, alreadyIndexedFiles, lastPos, true, firstFileLine);
        } catch (Throwable t) {
            //t.printStackTrace();
            // FailedToMatchFilesException -  might be suffering from disk io during massive roll amount etc so pause and retry....
            Thread.sleep(10 * 1000);
            files = fileSorter == null ? new String[0] : fileSorter.sortedFileNames(true, logCanonical, logName, parentName, parentCanonical, alreadyIndexedFiles, lastPos, true, firstFileLine);
        }

        if (files.length > 0) {
            //rolledTo = indexer.rolled(logCanonical, files[0]);
            return files[0];
        } else {
            LOGGER.warn("RollFailed:"  + logCanonical + " No destination file given");
        }
        return rolledTo;
    }


    private void flushChannel(int line) {
        final File rolled = new File(logCanonical);
        FileChannel channel = null;
        try {
            channel = new FileInputStream(rolled).getChannel();
            if (logReader != null) logReader.readNext(lastPos, line);
        } catch (IOException e) {
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public void doNumericRoll(EventMonitor eventMonitor, String rollSource, String rolledTo, LogReader writer) {
        LOGGER.info("Performing NumericRoll:" + rollSource + " => " + rolledTo);
        // now roll each file for which we find a target file exists
        int rollingNext = ContentBasedSorter.getNumericRollNumber(new File(rolledTo).getName());
        NumericNamers.NumericNamer numericNamer = NumericNamers.getNumericNamers(rolledTo);
        if (numericNamer == null) {
            LOGGER.warn("Cannot roll file of convention:" + rolledTo);
            return;
        }


        /**
         * Move all the targets up
         */
        LOGGER.debug("RollingNext: " +rollingNext);
        if (rollingNext != -1) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug(" RollNext: " + rollingNext);

            for (int i = MAX_NUMERICAL_ROLL; i >= rollingNext; i-- ) {
                // move 3->4, 2->3, 1->2
                String currentLogFile = numericNamer.get(rolledTo, i);

                LogFile logFile = indexer.openLogFile(currentLogFile);

                String nextLogFile = numericNamer.get(rolledTo, i + 1);
                boolean currentExist = new File(currentLogFile).exists();
                boolean nextExits = new File(nextLogFile).exists();
                LOGGER.debug("CurrentExist:" + currentLogFile + ":" + currentExist + " NextExist:" + nextLogFile +":" + nextExits);

                if (LOGGER.isDebugEnabled()) LOGGER.debug("Prepare NumericRoll: logFile(indexed?):"+ (logFile != null) + " current(exists?): " + currentLogFile + ":" + currentExist + " next(exists?):" + nextLogFile + ":" + nextExits);
                if (logFile == null) {
                    LOGGER.debug("NumericRoll Ignoring, DidntExist:" + currentLogFile);
                    continue;
                }

                if (currentExist && nextExits) {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("NumericRoll:"+ currentLogFile + " => " + nextLogFile);
                    indexer.rolled(currentLogFile, nextLogFile);
                    writer.roll(currentLogFile, nextLogFile);

                } else  if (currentExist && !nextExits && !rollSource.equals(currentLogFile)) {
                    if (LOGGER.isDebugEnabled())  LOGGER.debug("NOT_NumericRoll:"+ currentLogFile + " Removing: " + currentLogFile + " Next:" + nextLogFile);
                    Event event = new Event("ROLL_FILE").with("file", currentLogFile);
                    eventMonitor.raise(event.with("Removed-No-Destination", ""));
                    indexer.removeFromIndex(currentLogFile);
                } else {
                    if (LOGGER.isDebugEnabled())  LOGGER.debug("NOTHING_NumericRoll:"+ currentLogFile + " Removing: " + currentLogFile);
                }
            }
            Event event = new Event("ROLL_FILE").with("file", rollSource).with("to", rolledTo);
            eventMonitor.raise(event);

            indexer.rolled(rollSource, rolledTo);
        }
    }
}