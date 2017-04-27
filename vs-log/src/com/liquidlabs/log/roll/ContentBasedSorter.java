package com.liquidlabs.log.roll;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.util.DateTimeExtractor;
import jregex.Pattern;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

/**
 * Looks as the first 1 to see if the contents match
 *
 */
public class ContentBasedSorter implements RolledFileSorter {
    private static final Logger LOGGER = Logger.getLogger(ContentBasedSorter.class);

    private static final int ROLL_TIME_TH_MINS = 5;

    int lineLimit = 50;

    transient DateTimeExtractor timeExtractor;

    private String timeFormat;

    private String[] filePatterns = new String[] { ".*" };

    public ContentBasedSorter() {
    }

    public ContentBasedSorter(String timeFormat) {
        this.timeFormat = timeFormat;
    }
    public String getType() {
        return this.getClass().getSimpleName();
    }

    public void setFilenamePatterns(String[] filePattern) {
        this.filePatterns = filePattern;
    }

    public String[] sortedFileNames(boolean pause, final String logCanonical, final String logName, String parentName, String parentCanonical, Set<String> avoidTheseFiles, long lastKnownFilePos, boolean verbose, String firstFileLine) throws InterruptedException {

        if (Thread.currentThread().isInterrupted()) {
            LOGGER.info("Was Interrupted!");
            throw new InterruptedException();
        }

        final TreeMap<Long, String> sorted = new TreeMap<Long, String>();
        List<String> files = filterFiles(logName, parentCanonical);
        if (files.size() == 0) {
             throw new RuntimeException("Failed to FilterFiles Found  this:" + logName + " File[0]: **Check your datasource.filename mask matches possible roll targets:" + Arrays.toString(filePatterns));
        }

        List<String> matchingNames = getMatchingNames(parentCanonical, logName, files, ".", lastKnownFilePos, verbose, avoidTheseFiles, firstFileLine);


        // Sometimes a roll can be slow - so we need to wait and retry - the destination file may exist but the roll incompete
        if (matchingNames.size() == 0 && pause) {
            Thread.sleep(Integer.getInteger("roll.wait.sec", 10) * 1000);
            files = filterFiles(logName, parentCanonical);
            matchingNames = getMatchingNames(parentCanonical, logName, files, ".", lastKnownFilePos, verbose, avoidTheseFiles, firstFileLine);
        }

        if (matchingNames.size() == 0) {
            String m = files.toString();
            if (m.length() > 4 * 1024) m = m.substring(0, 4 * 1024);
            throw new RuntimeException("Failed to MatchFiles this:" + logName + " [Content] - GivenFileList:"+m + " \nLine:" + firstFileLine);
        }


        List<String> errors = new ArrayList<String>();
        for (String name : matchingNames) {
            try {
                long millis = getTimeFromFile(parentCanonical, name);
                if (millis > 0) {
                    sorted.put(millis, parentCanonical + File.separator + name);
                }
            } catch(Throwable t) {
                errors.add(t.getMessage());
            }
        }

        if (sorted.size() == 0) {
            throw new RuntimeException(String.format("Sorted:%s [0 files extracted] this:" + logName + " sampleLineLength[%d] - Failed with \n --- all:%s \n --- matchingNames:%s \n --- errors:%s", sorted, firstFileLine.length(), Arrays.toString(files), matchingNames, errors));
        }

        LOGGER.info( logName + " RollTarget:" + StringUtil.truncateString(1024, sorted.toString()));

        return new String [] { sorted.get(sorted.lastKey())};
    }



    public List<String> filterFiles(final String rollFileSource, String parentCanonical) {
        final Pattern[] patterns = new Pattern[filePatterns.length];
        int pos = 0;
        for (String fileP : filePatterns) {
            patterns[pos++] = new Pattern(fileP);
        }
        // possible name using the first part only
        // a) agent.log -> agent.log.2014etc OR event.log -> event.log.1 OR
        // b) event.log.1 -> event.log.2
        //
        final String rollSourcenameOnly = FileUtil.getNamePartOnly(rollFileSource);
        String[] list = new File(parentCanonical).list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (new File(dir, name).isDirectory()) return false;
                // only roll to a.log => a.log.1 a.log.2014 etc - it must contain the source filename
                if (!name.contains(rollSourcenameOnly)) return false;

                for (Pattern pattern : patterns) {
                    if (RegExpUtil.matchesJRExp(pattern, pattern.matcher(), name, false).isMatch()) return true;
                }
                return false;
            }
        });
        if (list == null) return new ArrayList<String>();
        List<String> files = new ArrayList<String>(Arrays.asList(list));
        return files;
    }
    List<String> getMatchingNames(String parentDir, String rollFromFilename, List<String> rollCandidateFiles, String splitter, long lastKnownFilePos, boolean verbose, Set<String> avoidTheseFiles, String firstFileLine) {

        // rollers can roll to formats that replace or remove the extension
//		long passedTimeThreshold = getPassedTimeThreshold();
//		LOGGER.info("TimeThreshold:" + DateUtil.shortTimeFormat.print(passedTimeThreshold));
        ArrayList<File> resultFiles = new ArrayList<File>();
        firstFileLine = firstFileLine.trim();


        String fromNameOnly = new File(rollFromFilename).getName();
        String fromNamePart = fromNameOnly.contains(".") ? fromNameOnly.substring(0, fromNameOnly.indexOf(".")) : fromNameOnly;

        if (LOGGER.isDebugEnabled()) LOGGER.debug("getMatchingNames.CHECKING starting:");
        for (String filename : rollCandidateFiles) {

            boolean isNumeric = isNumericalRollFile(filename);

            try {
                File rollToFileMaybe = new File(String.format("%s%s%s", parentDir, File.separator, filename));
                if (filename.equals(rollFromFilename)) continue;
                if (!isNumeric && fromNameOnly.length() > rollToFileMaybe.getName().length()) continue;



                // This is costly - so only include files who have a first line that matches the expected line
                String toFileLine = FileUtil.getLine(rollToFileMaybe,1);
                if (LOGGER.isDebugEnabled()) LOGGER.debug("getMatchingNames.CHECKING:" + rollToFileMaybe + " :" + toFileLine);
                if (toFileLine == null) continue;
                if (!toFileLine.startsWith(firstFileLine)) {
                    if(LOGGER.isDebugEnabled()) LOGGER.debug("FAILED startsWith got:" + toFileLine + " expected:" + firstFileLine);
                    continue;
                }

                // only consider files that contain the original filename
                // i.e. agent.log agent.log-12.01.31 etc
                if (! rollToFileMaybe.getName().contains(fromNamePart)) {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("FAILED contains got:" + rollToFileMaybe.getName() + " expected:" + fromNamePart);
                    continue;
                }
                if (!rollToFileMaybe.exists()) {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("FAILED exists");
                    continue;
                }


                long length = rollToFileMaybe.length();
                if (rollToFileMaybe.getName().endsWith(".gz")) {
                    length = FileUtil.getGzipExpandedBytes(rollToFileMaybe);
                }
//                // only consider destinations that have similar file length
//                if (!isNumeric && ! (length >= lastKnownFilePos && length < lastKnownFilePos + 4 * 1024) ){
//                    //				if (verbose) LOGGER.info(String.format("RollFrom:%s IGNORED File:%s Reason 2", rollFromFilename, rollToFileMaybe));
//                    LOGGER.info("FAILED LENGTH : " + length + " : " + lastKnownFilePos);
//                    continue;
//                }


                if (isOlderThan1Day(rollToFileMaybe.lastModified())) {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("FAILED Older-than");
                    continue;
                }

                String fileCanonical = FileUtil.getPath(rollToFileMaybe);
                if (!isNumeric && avoidTheseFiles.contains(fileCanonical)) {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("FAILED Avoid");
                    continue;
                }

                resultFiles.add(rollToFileMaybe);
                if(LOGGER.isDebugEnabled()) LOGGER.debug(String.format("RollFrom:%s Matched File:%s lastMod:%s", rollFromFilename, rollToFileMaybe, new DateTime(rollToFileMaybe.lastModified())));
            } catch (Throwable t) {
                LOGGER.warn("Failed to view:" + filename, t);
            }
        }
        Collections.sort(resultFiles, new Comparator<File>() {
            public int compare(File o1, File o2) {
                return Long.valueOf(o1.lastModified()).compareTo(o2.lastModified());
            }
        });
        ArrayList<String> results = new ArrayList<String>();
        for (File file : resultFiles) {
            results.add(file.getName());
        }
        return results;
    }

    private String getFileLine(File rollToFileMaybe) {
        String fullFilePath = rollToFileMaybe.getAbsolutePath();
        FileStuff foundCached = getCached().get(fullFilePath);
        if (foundCached == null) {
            FileStuff newFileStuff = new FileStuff(rollToFileMaybe);
            cached.put(fullFilePath, newFileStuff);
            return newFileStuff.fileLine;
        } else {
            // check the cached value
            foundCached.updateIfNeeded(rollToFileMaybe);
            return foundCached.fileLine;
        }
    }
    private  Map<String, FileStuff> getCached() {
        if (cached == null) cached = new HashMap<String, FileStuff>();
        return cached;
    }
    Map<String, FileStuff> cached = new HashMap<String, FileStuff>();
    public static class FileStuff {
        public FileStuff(File file) {
            this.filename = file.getAbsolutePath();
            update(file);
        }
        private void update(File file) {
            this.fileLine = FileUtil.getLine(file, 1);
            this.lastChecked = System.currentTimeMillis();
            this.lastMod = file.lastModified();
        }
        public void updateIfNeeded(File file) {
            // file hasnt been touched
            if (file.lastModified() == lastMod) {
                // noop
                // file is older than our last check
            } else if (file.lastModified() < lastChecked) {
            } else {
                // file is more recent than last check so update it
                update(file);
            }
        }
        String filename;
        long lastChecked;
        long lastMod;
        String fileLine;
    }


    boolean isOlderThan1Day(long lastModified) {
        return (DateTimeUtils.currentTimeMillis() - lastModified) > LogProperties.LIVE_ROLL_HOURS  * DateUtil.HOUR;
    }

    public static boolean isNumericalRollFile(String filename) {
        return getNumericRollNumber(filename) != -1;
    }
    public static int getNumericRollNumber(String file) {

        int integerCount = 0;
        int matchedPos = 0;
        if (file.contains(".")) {
            String[] split = file.split("\\.");
            Set<Integer> validOffsets = new HashSet<>(java.util.Arrays.asList(split.length -1, split.length -2));

            int currentPos = 0;
            for (String filepart : split) {
                // try and parse the last bit as an integer
                try {
                    int number = Integer.parseInt(filepart);
                    if (validOffsets.contains(currentPos)) matchedPos++;
                    integerCount++;
                } catch (NumberFormatException nfe) {
                }
                currentPos++;
            }
        }
        if (integerCount == 1 && matchedPos == 1) {
            return matchedPos;

        }


        return -1;
    }

    long getPassedTimeThreshold() {
        return new DateTime().minusMinutes(ROLL_TIME_TH_MINS).getMillis();
    }

    long getTimeFromFile(String parent, String path) throws FileNotFoundException {
        File file = new File(parent, path);
        RAF raf = null;
        try {
            raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
            int attempts = 0;
            String line = "";
            while(((line = raf.readLine()) != null) && attempts < lineLimit) {
                Date time = getTimeExtractor().getTime(line, file.lastModified());
                if (time  != null) {
                    return time.getTime();
                }
            }
            // fallback to the lastmod time
            return file.lastModified();
        } catch (IOException e) {
        } finally {
            try {
                raf.close();
            } catch (IOException e) {
            }
        }
        return -1;
    }

    private DateTimeExtractor getTimeExtractor() {
        if (timeExtractor == null) timeExtractor = new DateTimeExtractor(timeFormat);
        return timeExtractor;
    }

    public void setFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }
    public String getFormat(){
        return timeFormat;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " "  + timeFormat;
    }

}
