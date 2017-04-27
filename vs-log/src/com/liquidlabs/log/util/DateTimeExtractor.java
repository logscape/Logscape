package com.liquidlabs.log.util;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.LogProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DateTimeExtractor {
    final static Logger LOGGER = Logger.getLogger(DateTimeExtractor.class);

    String[] formatters = LogProperties.FORMATS;

    private Grabber formatterToUse;
    List<Grabber> dateTimeGrabbers = new CopyOnWriteArrayList<Grabber>();
    static Map<String, Grabber> grabbers = new ConcurrentHashMap<String, Grabber>();
    private boolean usingCustomGrabber = false;


    public static String testWithFormat(String formatter, String text) {
        try {
            DateTimeExtractor extractor = new DateTimeExtractor(formatter);
            String recognised = extractor.getFormat(text, 0);
            Date gotTime = extractor.getTime(text, 0);
            extractor.setExtractorFormat(formatter);
            Date extractedUsingGivenFormat = extractor.getTimeUsingFormatter(text, 0);

            StringBuilder result = new StringBuilder();
            if (formatter != null && formatter.length() > 0) {
                result.append("UserFormat:").append(extractedUsingGivenFormat);
            }
            result.append("\n\n ** Extracted:").append(gotTime);
            result.append("\n ** Formatter:").append(recognised);
            result.append("\n\nFormatters:\n");
            for (String format : LogProperties.FORMATS) {
                result.append(" -").append(format).append("\n");
            }
            return result.toString();
        } catch (Throwable t) {
            return "Failed to get results:" + t.getMessage();
        }
    }

    public DateTimeExtractor() {
    }
    public DateTimeExtractor(String customFormat) {
        if (customFormat != null && customFormat.length() > 0) {
            //formatters = new String[0];
            formatters = Arrays.preppend(formatters, customFormat);
            usingCustomGrabber = true;
        }
    }

    public String getActiveFormat() {
        if (formatterToUse == null) return null;
        return formatterToUse.format();
    }

    synchronized private List<Grabber> getGrabbers() {
        if(!dateTimeGrabbers.isEmpty()){
            return dateTimeGrabbers;
        }
        for (String formatter : formatters) {
            try {
                if (formatter != null && !formatter.equals("null")) dateTimeGrabbers.add(getGrabber(formatter));
            } catch (Throwable t) {
                LOGGER.warn("BadFormatter:'" + formatter + "' ex: " + t);
            }
        }
        return dateTimeGrabbers;
    }

    public void clearGrabbers() {
        dateTimeGrabbers.clear();
    }


    private Grabber getGrabber(String format) {
        Grabber result = grabbers.get(format);
        if (result != null) return result;
        result = getGrabberP(format);
        grabbers.put(format, result);
        return result;

    }

    private Grabber getGrabberP(String format) {
        format = format.trim();
        if (CharGrabber.isForMe(format)) return new CharGrabber(format);
        if (JsonKVGrabber.isForMe(format)) return new JsonKVGrabber(format);
        return new DateTimeGrabber(format);
    }

    public void setExtractorFormat(String format) {
        if (format != null && format.trim().length() > 0) {
            try {
                if (formatterToUse != null && !formatterToUse.format().equals(format)) {
                    formatterToUse = getGrabber(format);
                } else {
                    formatterToUse = getGrabber(format);
                }
                clearGrabbers();
            } catch (Throwable t) {
                LOGGER.warn(t);
            }
        }
    }



    public String getFormat(String nextLine, long fileLastMod) {
        Object[] internalGetFormat = internalGetFormat(nextLine, fileLastMod);
        if (internalGetFormat != null) return (String) internalGetFormat[0];
        return null;
    }

    public String getFormat(File logFile, int lines, String breakRule) {

        if (logFile.isDirectory()) return null;
        try {
            return getFormat(logFile, FileUtil.readLines(logFile.getAbsolutePath(), lines, breakRule));
        } catch (IOException e) {
            LOGGER.warn(e.toString(), e);
        }
        return null;
    }
    public String getFormat(File logFile, List<String> lines) {
        long now = DateTimeUtils.currentTimeMillis();

        for (String line : lines) {
            Object[] result = internalGetFormat(line, now);
            if (result != null) return (String) result[0];
        }
        return null;
    }


    public Date getTimeUsingFormatter(String nextLine, long fileLastMod) {
        if (formatterToUse == null) return getTime(nextLine, fileLastMod);
        if (nextLine.length() < 4) return new Date(fileLastMod);
        Object[] internalGetFormat = getTime(nextLine, formatterToUse, fileLastMod);
        if (internalGetFormat != null) {
            clearGrabbers();
            return (Date) internalGetFormat[1];
        }
        return getTime(nextLine, fileLastMod);
    }

    String lastFormat = null;
    public Date getTime(String nextLine, long defaultTime) {
        if (nextLine.length() < 4) return new Date(defaultTime);
        Object[] internalGetFormat = internalGetFormat(nextLine, defaultTime);
        if (internalGetFormat != null) {
            lastFormat = (String) internalGetFormat[0];
            return (Date) internalGetFormat[1];
        }
        return new Date(defaultTime);
    }

    public String getLastFormat() {
        return lastFormat;
    }

    public DateTime getDateTime(String nextLine) {
        Object[] internalGetFormat = internalGetFormat(nextLine, System.currentTimeMillis());
        if (internalGetFormat != null) return new DateTime( (Date) internalGetFormat[1]);
        return new DateTime(System.currentTimeMillis());
    }
    public Date getTimeWithFallback(String readLine, long time) {
        Date result = this.getTime(readLine, time);
        if (result == null) result = new Date(time);
        return result;
    }


    public Date getTime(String nextLine, String timeFormat, long fileLastMod) {
        if (timeFormat != null) {
            for (Grabber format : getGrabbers()) {
                if (format.format().equals(timeFormat)) {
                    Object[] result = getTime(nextLine, format, fileLastMod);
//					if (result != null) return (Date) fixResult(fileLastMod, format, result)[1];
                    if (result != null) return (Date) result[1];
                    else return new Date(fileLastMod);
                }
            }
        }
        return getTime(nextLine, fileLastMod);
    }

    private Object[] internalGetFormat(String nextLine, long fileLastMod) {
        if (nextLine == null) return null;
        List<Object[]> formatters = new ArrayList<Object[]>();
        int grabberIndex = 0;
        for (Grabber format : getGrabbers()) {
            // need to allow 2 chars less for format string formatting stuff
            if (nextLine.length() < format.format().length()-2) continue;

            Object[] result = getTime(nextLine, format, fileLastMod);

            if (result != null && !result.equals("null")) {
                formatters.add(result);
                if (usingCustomGrabber && grabberIndex == 0) return result;
                // return anything with millisecond accuracy
                if (format.format().contains("SSS")) return result;
//				if (format.formatString.contains("ss")) return result;
            }
            grabberIndex++;
        }
        if (formatters.size() == 0) return null;
        if (formatters.size() == 1) return formatters.get(0);



        // multiple hits - take a punt and get the closest to fileLastMod.

        // otherwise
        Object[] potentialResult = formatters.get(0);
        long potentialResultTime = ((Date)potentialResult[1]).getTime();
        for (Object[] objects : formatters) {
            Date date = (Date) objects[1];
            // if this one is closer to the last mod time then use it instead
            if (Math.abs(fileLastMod - date.getTime()) < Math.abs(fileLastMod - potentialResultTime) ) {
                potentialResult = objects;
                potentialResultTime = date.getTime();
            }
        }
        return potentialResult;
    }

    final private Object[] getTime(String nextLine, Grabber grabber, long fileLastMod) {
        if (grabber == null) return null;


        try {
            String cut = grabber.grab(nextLine);
            // save 'parse' cpu cycles by checking the line kind of matches
            if (cut != null && cut.trim().length() > 0 &&
                    grabber.isFormatStringCutParsable()    ||
                    grabber.isFormattingMatchCutSymbols(cut)
                    ) {
                try {
                    if (grabber.isRegularFormatter()) {
                        if (grabber.format().contains("/")) {
                            if (StringUtils.countMatches("/", grabber.format()) != StringUtils.countMatches("/", cut)) return null;
                            if (!grabber.isFormattingMatchCutSymbols(cut)) return null;
                        }
                        if (grabber.format().contains("-")) {
                            if (StringUtils.countMatches("-", grabber.format()) != StringUtils.countMatches("-", cut)) return null;
                            if (!grabber.isFormattingMatchCutSymbols(cut)) return null;

                        }
                    }
                    if (grabber.format().contains("'T'")) {
                        cut = cut.substring(0, cut.length() -2);
                    }

                    Date parse = grabber.parse(cut, fileLastMod);
//                    System.out.println("GOT:" + grabber.toString() + " GOT :" + parse + " P" + grabber.isFormattingMatchCutSymbols(cut));
                    if (parse == null || parse.getTime() <= 0 || parse.getTime() > System.currentTimeMillis() + DateUtil.HOUR) return null;
                    long year = new DateTime(parse.getTime()).getYear();
                    lastFormat = grabber.format();
                    if (year >= 2005 && year <= new DateTime().getYear()) return new Object[] { grabber.format(), parse };
                } catch (Throwable t) {
                    //t.printStackTrace();
                }
            }
        } catch (Throwable e) {
            // e.printStackTrace();;
        }
        return null;
    }
    public long getFileStartTime(File logfile, int scanCount) throws IOException {
        RAF raf = null;
        try {
            raf = RafFactory.getRaf(logfile.getAbsolutePath(), BreakRule.Rule.SingleLine.name());
            boolean done = false;
            int cPos = 0;
            while (!done && cPos < scanCount) {
                String readLine = raf.readLine();
//				Date time = getTime(readLine, logfile.lastModified());
                Object[] internalGetFormat = internalGetFormat(readLine, logfile.lastModified());
                if (internalGetFormat != null) {
                    done = true;
                    formatterToUse = getGrabber((String) internalGetFormat[0]);
                    clearGrabbers();
                    return ((Date)internalGetFormat[1]).getTime();
                }
                cPos++;
            }
            return logfile.lastModified();
        } finally {
            if (raf != null) raf.close();
        }
    }

}
