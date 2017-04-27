package com.liquidlabs.log.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.collection.CompactCharSequence;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.log.LogProperties;
import org.apache.lucene.document.*;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

public class LogFile implements KryoSerializable {
    private static final String SERVER = LogProperties.getServiceDIRName();

    CompactCharSequence fileName = CompactCharSequence.EMPTY;

    int logId = -1;
    public CompactCharSequence timeFormat = CompactCharSequence.EMPTY;

    // both of these are optionally set when the file was not sourced locally.
    // the host is grabbed from the path where the item before it was 'Server/HOST'
    CompactCharSequence hostname;
    CompactCharSequence fwdPath;

    int lineCount;
    long startTime;
    long endTime;
    long pos;
    public CompactCharSequence tags = CompactCharSequence.EMPTY;

    // used in roll detection
    CompactCharSequence firstLine = CompactCharSequence.EMPTY;
    short minLineLength = -1;
    boolean isDW = false;

    // false when it is a file that was rolled to - i.e. dont append to rolled files.....
    boolean appendable = true;

    public CompactCharSequence newLineRule = CompactCharSequence.EMPTY;

    public CompactCharSequence fieldSetId = CompactCharSequence.EMPTY;

    public LogFile() {
    }

    public LogFile(String logFile, int logId, String fieldSetId, String tags) {
        if (logFile != null) this.fileName = new CompactCharSequence(logFile);
        this.logId = logId;
        if (fieldSetId != null) this.fieldSetId = new CompactCharSequence(fieldSetId);

        String autoTags = getAutoTags(tags, logFile);
        if (autoTags.length() > 0 && tags.length() > 0) tags += ",";
        this.tags = new CompactCharSequence(tags + autoTags);


        String[] hostPath = getHostnameFromPath(logFile);
        if (hostPath != null) {
            hostname = new CompactCharSequence(hostPath[0]);
            fwdPath = new CompactCharSequence(hostPath[1]);
        }
    }
    public static String getAutoTags(String existingTags, String logFile) {
        HashSet<String> existingTagsSet = new HashSet<String>(Arrays.asList(existingTags.split(",")));
        final Pattern slashPattern = Pattern.compile("/");
        final Pattern backSlashPattern = Pattern.compile("\\\\");

        String[] split = slashPattern.split(logFile);
        if (logFile.contains("\\")) split = backSlashPattern.split(logFile);
        StringBuilder sb = new StringBuilder();
        for (String pathPath : split) {
            if (pathPath.startsWith("_") && pathPath.endsWith("_") && !pathPath.equals("_SERVER_")) {
                String tag = pathPath.replace("_","");
                if (existingTagsSet.contains(tag)) continue;
                existingTagsSet.add(tag);
                if (sb.length() > 0) sb.append(",");
                sb.append(tag);
            }
        }
        return sb.toString();
    }

    public static String[] getHostnameFromPath(String logFile) {
        final Pattern slashPattern = Pattern.compile("/");
        final Pattern backSlashPattern = Pattern.compile("\\\\");

        String[] split = slashPattern.split(logFile);
        if (logFile.contains("\\")) split = backSlashPattern.split(logFile);
        String[] results = null;
        for (int j = 0; j < split.length-1; j++) {
            if (split[j].endsWith(SERVER)) {
                String servername = split[j + 1];
                results = new String[] { servername, logFile.substring(logFile.indexOf(servername) + servername.length()) };
            }
        }
        return results;
    }

    public LogFile(String newName, LogFile copyFrom) {
        if (newName != null) this.fileName = new CompactCharSequence(newName);
        this.logId = copyFrom.logId;
        this.timeFormat = copyFrom.timeFormat;
        this.hostname = copyFrom.hostname;
        this.fwdPath = copyFrom.fwdPath;
        this.lineCount = copyFrom.lineCount;
        this.startTime = copyFrom.startTime;
        this.endTime = copyFrom.endTime;
        this.pos = copyFrom.pos;
        if (copyFrom.getTags() != null) this.tags = new CompactCharSequence(copyFrom.getTags());
        this.firstLine = CompactCharSequence.EMPTY;

        this.newLineRule = copyFrom.newLineRule;
        this.isDW = copyFrom.isDW;


        if (copyFrom.getTimeFormat() != null) this.timeFormat = new CompactCharSequence(copyFrom.getTimeFormat());
        if (copyFrom.fieldSetId != null) this.fieldSetId = copyFrom.fieldSetId;

        // Roll target - dont append or tail it
        this.appendable = false;
        this.minLineLength = copyFrom.minLineLength;
    }

    final public int getId() {
        return logId;
    }
    public void update(List<Line> lines) {
        for (int i = 0; i < lines.size(); i++) {
            Line line = lines.get(i);
            update(line.filePos, line);
        }
    }

    public void update(long pos, Line line) {

        if (line.number() > lineCount) lineCount = line.number();
        if (startTime == 0 ||  startTime > line.time()) {
            startTime = line.time();
        }
        if (endTime < line.time()) {
            this.endTime = line.time();
        }
        this.pos = pos;
    }

    public long getEndTime() {
        return endTime;
    }
    public long getStartTime() {
        return startTime;
    }
    public int getLineCount() {
        return lineCount;
    }
    public long getPos() {
        return pos;
    }
    transient String cachedFilename = null;
    final public String getFileName() {
        if (cachedFilename == null) cachedFilename = fileName.toString();
        return cachedFilename;
    }
    public void setTimeFormat(String format) {
        if (format == null) this.timeFormat = CompactCharSequence.EMPTY;
        else this.timeFormat = new CompactCharSequence(format);
        if (this.timeFormat.toString().startsWith("yyyy")) {
            this.newLineRule = new CompactCharSequence(BreakRule.Rule.Year.name());
        }
    }
    public void setMinLineLength(int len) {
        if (minLineLength == -1) minLineLength = (short) len;
        else if (len < minLineLength) minLineLength = (short) len;
    }
    public boolean isWithinTime(long startTimeMs, long endTimeMs) {
        if (this.startTime == this.endTime && this.startTime == 0) return true;
        return this.startTime < endTimeMs && this.endTime > startTimeMs;
    }
    public String getTimeFormat() {
        return  timeFormat.toString();
    }
    public void setNewLineRule(String newLineRule) {
        this.newLineRule = new CompactCharSequence(newLineRule);
    }

    public List<DateTime> getStartEndTimes() {
        if (this.fileName == null) return new ArrayList<DateTime>();
//		File file = new File(this.fileName.toString());
//		if (file.exists()) {
//			// seem to be dropping tailer times for some crap reason!
//			if (this.endTime < file.lastModified()) {
//				this.endTime = file.lastModified();
//			}
//			if (this.startTime > this.endTime) {
//				this.startTime = this.endTime - 1000;
//			}
//		}
        // hack on 1 minute - in case of a race condition between indexing and events
        return java.util.Arrays.asList(new DateTime[] { new DateTime(this.startTime), new DateTime(this.endTime).plusMinutes(1) });
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("[LogFile:");
        buffer.append(" Name:");
        buffer.append(fileName);
        buffer.append(" Tags:");
        buffer.append(tags);
        buffer.append(" type:");
        buffer.append(fieldSetId);
        buffer.append(" LogId:");
        buffer.append(logId);
        buffer.append(" Lines:");
        buffer.append(lineCount);
        buffer.append(" MinLineLen:");
        buffer.append(minLineLength);
        buffer.append(" Times:");
        buffer.append(getStartEndTimes());
        buffer.append(" AgeDays:");
        buffer.append(getAgeInDays());
        buffer.append(" Pos:");
        buffer.append(pos);
        buffer.append(" NewLineRule:");
        buffer.append(newLineRule);
        buffer.append(" TimeFormat:");
        buffer.append(timeFormat);
        buffer.append(" Appendable:");
        buffer.append(appendable);
        buffer.append("]");
        return buffer.toString();
    }

    public boolean isToday() {
        return (new DateTime(this.startTime).getDayOfYear() == new DateTime().getDayOfYear());
    }

    public int getAgeInDays() {
        return Days.daysBetween(new DateTime(this.endTime), new DateTime()).getDays();
    }

    final public String getFieldSetId() {
        return fieldSetId.toString();
    }

    public void setFieldSetId(String id) {
        this.fieldSetId = new CompactCharSequence(id);
    }

    public String firstLine() {
        return firstLine.toString();
    }

    public void setFirstLine(String line) {
        if (line == null) line = "";
        this.firstLine = new CompactCharSequence(line);
    }

    public String getNewLineRule() {
        return newLineRule.toString();
    }

    public String getTags() {
        return tags.toString();
    }

    public void setTags(String tags2) {
        this.tags = new CompactCharSequence(tags2);

    }

    public void setStartEndTime(DateTime start, DateTime end) {
        this.startTime = start.getMillis();
        this.endTime = end.getMillis();
    }
    public void setEndTime(long lastModified) {
        this.endTime = lastModified;
    }

    /**
     * @param hostname the default value where a real value is not found
     * @return the source of where this file originated from
     */
    public String getFileHost(String hostname) {
        if (this.hostname != null && this.hostname.length() > 0) return this.hostname.toString();
        return hostname;
    }

    private void fixNullItems() {
        if (this.hostname == null) this.hostname = CompactCharSequence.EMPTY;
        if (this.timeFormat == null) this.timeFormat = CompactCharSequence.EMPTY;
        if (this.fwdPath == null) this.fwdPath = CompactCharSequence.EMPTY;
        if (this.fileName == null) this.fileName = CompactCharSequence.EMPTY;
        if (this.fieldSetId == null) this.fieldSetId = CompactCharSequence.EMPTY;
        if (this.firstLine == null) this.firstLine = CompactCharSequence.EMPTY;
        if (this.tags == null) this.tags = CompactCharSequence.EMPTY;
        if (this.newLineRule == null) this.newLineRule = CompactCharSequence.EMPTY;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        fixNullItems();

        kryo.writeObject(output, this.fileName);
        kryo.writeObject(output, this.logId);
        kryo.writeObject(output, this.timeFormat);
        kryo.writeObject(output, this.hostname);
        kryo.writeObject(output, this.fwdPath);
        kryo.writeObject(output, this.lineCount);
        kryo.writeObject(output, this.startTime);
        kryo.writeObject(output, this.endTime);
        kryo.writeObject(output, this.pos);
        kryo.writeObject(output, this.tags);
        kryo.writeObject(output, this.firstLine);
        kryo.writeObject(output, this.isDW);
        kryo.writeObject(output, this.appendable);
        kryo.writeObject(output, this.newLineRule);
        kryo.writeObject(output, this.fieldSetId);
        kryo.writeObject(output, this.minLineLength);

    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.fileName = kryo.readObject(input, CompactCharSequence.class);

        this.logId = kryo.readObject(input, int.class);
        this.timeFormat = kryo.readObject(input, CompactCharSequence.class);
        this.hostname = kryo.readObject(input, CompactCharSequence.class);
        this.fwdPath = kryo.readObject(input, CompactCharSequence.class);
        this.lineCount = kryo.readObject(input, int.class);
        this.startTime = kryo.readObject(input, long.class);
        this.endTime = kryo.readObject(input, long.class);
        this.pos = kryo.readObject(input, long.class);
        this.tags = kryo.readObject(input, CompactCharSequence.class);

        this.firstLine = kryo.readObject(input, CompactCharSequence.class);

        this.isDW = kryo.readObject(input, boolean.class);
        this.appendable = kryo.readObject(input, boolean.class);

        this.newLineRule = kryo.readObject(input, CompactCharSequence.class);
        this.fieldSetId = kryo.readObject(input, CompactCharSequence.class);
        this.minLineLength = kryo.readObject(input, short.class);

    }

    public boolean isAppendable() {
        return appendable;
    }

    public void setAppendable(boolean appendable) {
        this.appendable = appendable;
    }

    public short getMinLineLength() {
        return minLineLength;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public String getFileNameOnly() {
        return FileUtil.getFileNameOnly(fileName.toString());
    }
    public Document toDocument() {
        LogFile logFile = this;
        final Document document = new Document();
        document.add(new IntField("appendable", logFile.isAppendable() ? 1 : 0, Field.Store.YES));
        document.add(new LongField("endMs", logFile.getEndTime(), Field.Store.YES));
        document.add(new StringField("fieldSetId", logFile.getFieldSetId(), Field.Store.YES));
        document.add(new StringField("filename", logFile.getFileName(), Field.Store.YES));
        document.add(new StringField("firstLine", logFile.firstLine(), Field.Store.YES));
        document.add(new IntField("id", logFile.getId(), Field.Store.YES));
        document.add(new IntField("lineCount", logFile.getLineCount(), Field.Store.YES));
        document.add(new StringField("newLineRule", logFile.getNewLineRule(), Field.Store.YES));
        document.add(new LongField("pos", logFile.getPos(), Field.Store.YES));
        document.add(new IntField("minLineLength", logFile.getMinLineLength(), Field.Store.YES));
        document.add(new LongField("startMs", logFile.getStartTime(), Field.Store.YES));
        document.add(new StringField("tags", logFile.getTags(), Field.Store.YES));
        document.add(new StringField("timeFormat", logFile.getTimeFormat(), Field.Store.YES));

        return document;

    }
    public LogFile(Document doc) {
        this.appendable = doc.getField("appendable").numericValue().intValue() == 1;
        this.endTime = doc.getField("endMs").numericValue().longValue();
        this.fieldSetId = getStringField(doc, "fieldSetId");
        this.fileName = getStringField(doc, "filename");
        this.firstLine = getStringField(doc, "firstLine");
        this.logId = doc.getField("id").numericValue().intValue();
        this.lineCount = doc.getField("lineCount").numericValue().intValue();
        this.newLineRule = getStringField(doc, "newLineRule");
        this.pos = doc.getField("pos").numericValue().longValue();
        this.minLineLength = (short) doc.getField("minLineLength").numericValue().intValue();
        this.startTime = doc.getField("startMs").numericValue().longValue();
        this.tags = getStringField(doc, "tags");
        this.timeFormat = getStringField(doc, "timeFormat");

        String[] hostPath = getHostnameFromPath(fileName.toString());
        if (hostPath != null) {
            hostname = new CompactCharSequence(hostPath[0]);
            fwdPath = new CompactCharSequence(hostPath[1]);
        }
    }

    private CompactCharSequence getStringField(Document doc, String name) {
        return  new CompactCharSequence(doc.getField(name).stringValue());
    }



    public static void dedup(List<LogFile> results) {
        HashMap<CompactCharSequence, CompactCharSequence> cached = new HashMap<CompactCharSequence, CompactCharSequence>();
        for (LogFile result : results) {
            result.fieldSetId = getCachedValue(cached, result.fieldSetId);
            result.tags = getCachedValue(cached, result.tags);
            result.timeFormat = getCachedValue(cached, result.timeFormat);
            result.newLineRule = getCachedValue(cached, result.newLineRule);
            result.fieldSetId = getCachedValue(cached, result.fieldSetId);
            result.fwdPath = getCachedValue(cached, result.fwdPath);
            result.hostname = getCachedValue(cached, result.hostname);


        }

    }
    private static CompactCharSequence getCachedValue(HashMap<CompactCharSequence, CompactCharSequence> cached, CompactCharSequence result) {
        CompactCharSequence cValue = cached.get(result);
        if (cValue == null) {
            cValue = result;
            cached.put(cValue, cValue);
        }
        return cValue;
    }

}
