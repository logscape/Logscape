package com.liquidlabs.log.index;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.indexer.IndexerDep;
import jregex.Pattern;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 11:12
 * To change this template use File | Settings | File Templates.
 */
public interface LogFileOps extends IndexerDep {
    interface FileDeletedListener {
        void deleted(String file);
    }
    void addFileDeletedListener(FileDeletedListener listener);

    public interface FilterCallback {
        boolean accept(LogFile logFile);
        public static FilterCallback ALWAYS = new FilterCallbackAlways();
    }
    public static class FilterCallbackAlways implements FilterCallback {
        @Override
        public boolean accept(LogFile logFile) {
            return true;
        }
    }
    public static class FilterCallbackRegEx implements FilterCallback {
        private final String[] string;
        private final List<Pattern> excludePatterns;

        public FilterCallbackRegEx(String string) {
            this.string = string.split(",");
            excludePatterns = this.getExcludePatterns(this.string);
        }
        @Override
        public boolean accept(LogFile logFile) {
            if (isExcluded(logFile)) {
                return false;
            }
            for (String s : string) {
                if (matches(logFile, s.trim())) return true;
            }
            return false;
        }


        private boolean isExcluded(LogFile logFile) {
            for (Pattern xPattern : excludePatterns) {
                if (xPattern.matches(logFile.getFileName())) return true;
            }
            return false;
        }

        private List<Pattern> getExcludePatterns(String[] parts) {
            ArrayList<Pattern> result = new ArrayList<Pattern>();
            for (String part : parts) {
                part = part.trim();
                if (part.startsWith("not(")) {
                    part = part.substring("not(".length(), part.length()-1);
                    result.add(new Pattern(part));
                }
                if (part.startsWith("!")) {
                    part = part.substring("!".length(), part.length());
                    result.add(new Pattern(part));
                }

            }
            return result;
        }


        private boolean matches(LogFile logFile, String string) {
            if (string.startsWith("tag:")) {
                String tag = (string.trim().split(":")[1]);
                return logFile.getTags().contains(tag);
            } else {
                if (string.contains("*"))return logFile.getFileName().matches(string);
                else return StringUtil.containsIgnoreCase(logFile.getFileName(), string);
            }
        }
    }


    LogFile openLogFile(String file, boolean create, String fieldSetId, String sourceTags);

    LogFile openLogFile(String logFilename);

    LogFile openLogFile(int logId);

    void close();

    void removeFromIndex(String file);

    void updateLogFile(LogFile openLogFile);

    int open(String file, boolean createNew, String fieldSetId, String sourceTags);

    void updateLogFileLines(String file, List<Line> lines);

    boolean isIndexed(String absolutePath);

    void add(String file, int line, long time, long pos);

    String rolled(String fromFile, String toFile);
    boolean assignFieldSetToLogFile(String logFile, String fieldSetId);

    void indexedFiles(FilterCallback callback);
    List<LogFile> indexedFiles(long startTimeMs, long endTimeMs, boolean sortByTime, FilterCallback callback);

    List<DateTime> getStartAndEndTimes(String filename);

    long[] getLastPosAndLastLine(String filename);
    void sync();
}
