package com.liquidlabs.log.util;

import com.liquidlabs.common.SimpleDateFormatPool;
import com.liquidlabs.common.StringUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 09/04/2013
 * Time: 08:40
 * To change this template use File | Settings | File Templates.
 */
abstract class BaseGrabber implements Grabber {

//    private boolean errorOnParseDateTime;
    protected int timeZoneOffset;
    DateTimeFormatter dateTimeFormat;
    SimpleDateFormatPool simpleDateTimeFormat;

    enum Type {NORMAL, SPACE, TAB, CSV, CHAR, DELIM}
    Type type = Type.NORMAL;

    String formatString = "";
    String format;

    public BaseGrabber(String format) {
        this.format = format;
    }

    public String format() {
        return format;
    }
    static String[] containsit = new String[] { "char:", "t ", "csv" , "'T'" };

    @Override
    public boolean isRegularFormatter() {
        for (String item : containsit) {
            if (formatString.contains(item)) return false;
        }
        return true;
    }

    public boolean isFormatStringCutParsable() {
            // some loose rules where we expect simpledateformating
            return formatString.contains(UNIX_LONG) ||
                formatString.contains(ZZZ) ||
                formatString.contains(SEMICOLON) ||
                formatString.contains(QUOTE);
    }

    public boolean isFormattingMatchCutSymbols(String cut) {
        // some tight rules where we expect joda time
        return cut != null && cut.indexOf(DASH) == formatString.indexOf(DASH) &&
                cut.indexOf(SPACE) == formatString.indexOf(SPACE) &&
                cut.indexOf(COLON) == formatString.indexOf(COLON) &&
                cut.indexOf(DOR) == formatString.indexOf(DOR) &&
                cut.indexOf(SLASH) == formatString.indexOf(SLASH) &&
                cut.indexOf(COMMA) == formatString.indexOf(COMMA);
    }


    protected transient Boolean isUnixLong;
    private transient Boolean isZZZ;
    private transient Boolean isYY;
    private transient Boolean isMM;
    private transient Boolean isDD;

    @Override
    public Date parse(String string, long previousTimeExtracted) {
        if (isUnixLong == null) doPreprocessing();

        // Quick filtering test - cause the parsers are very slow in comparison -
        // i.e. expecting a number at char 0 and we dont have one then fail!
        if (formatString.charAt(0) == 'y' || formatString.charAt(0) == 'd') {
            if (!StringUtil.isIntegerFast(string.charAt(0) + "")) return null;
        }
        if (isUnixLong) {
            return getDateUNIX(string);
        }

        try {
            if (isZZZ && dateTimeFormat != null) {

                DateTime date = dateTimeFormat.parseDateTime(string.substring(0, timeZoneOffset));
                return date.plusYears(new DateTime(previousTimeExtracted).getYear() - date.getYear()).toDate();
            }

            boolean canRollDay = false;

            try {
                if (dateTimeFormat != null) {
                    if (string.length() > formatString.length()) {
                        int cut = formatString.contains("'") ? formatString.replace("'","").length() : formatString.length();
                        string = string.substring(0, cut);
                    }

                    DateTime date = dateTimeFormat.parseDateTime(string);
                    // compensate for unknown year
                    DateTime now = new DateTime();
                    if (!isYY) {
                        if (previousTimeExtracted != 0)
                            date = date.plusYears((new DateTime(previousTimeExtracted).getYear() - date.getYear()));
                        else date = date.plusYears(now.getYear() - date.getYear());
                        if (date.getMillis() > now.getMillis()) date = date.minusYears(1);
                    }
                    // compensate for unknown Month
                    if (!isMM) {
                        if (previousTimeExtracted != 0)
                            date = date.plusMonths(new DateTime(previousTimeExtracted).getMonthOfYear() - date.getMonthOfYear());
                        else date = date.plusMonths(now.getMonthOfYear());
                    }
                    // compensate for unknown Day
                    if (!isDD) {
                        date = date.plusDays(new DateTime(previousTimeExtracted).getDayOfYear() - date.getDayOfYear());
                        canRollDay = true;
                    }

                    // can roll to the next day when have AM/PM niceness - this is a bit crap - but will
                    // hopefully work for most cases
                    if (canRollDay) {
                        if (date.getMillis() < previousTimeExtracted &&
                                date.getHourOfDay() < 12 && new DateTime(previousTimeExtracted).getHourOfDay() > 12) {
                            date = date.plusDays(1);
                        }
                    }

                    if (isDateTooOld(date.getMillis())) throw new RuntimeException("Year too small");

                    return date.toDate();
                } else {
                    try {
                        return trySimpleDateFormat(string, previousTimeExtracted, true);
                    } catch (Throwable t) {
                        return null;
                    }
                }

            } catch (Throwable t) {
                return trySimpleDateFormat(string, previousTimeExtracted, true);
            }
        } catch (Exception iea) {
            // try and prevent mental times being guess from SimpleDateTimeParser - where EEE is used
          //  if (this.format.contains("EEE")) return null;
//            errorOnParseDateTime = true;
            return trySimpleDateFormat(string, previousTimeExtracted, true);

        }
    }

    protected void doPreprocessing() {
        isUnixLong = formatString.contains(UNIX_LONG);
        isZZZ = formatString.contains(ZZZ);
        isYY = format.contains(YY);
        isMM = format.contains(MM);
        isDD = format.contains(DD);


    }

    private Date getDateUNIX(String string) {
        String timeWMS = "1242973152454";
        String timeNO_MS = "1242973152";
        String timeWMS_AND_DOT = "1242973152.454";
       // string = string.substring(0, StringUtil.indexOfCharRange(string,true, new char[] { '0','9'})+1);
        if (string.length() > formatString.length()) {
            if (string.charAt(timeWMS_AND_DOT.length() + 1) == " ".charAt(0)) {
                string = string.substring(0, string.indexOf(" "));
            }
        }
        if (string.length() == timeWMS.length()) return new DateTime(Long.parseLong(string)).toDate();
        if (string.length() == timeNO_MS.length()) return new DateTime(Long.parseLong(string) * 1000).toDate();
        if (string.length() == timeWMS_AND_DOT.length() && string.contains(".")) {
            String sss = string.substring(0, string.indexOf("."));
            return new DateTime(Long.parseLong(sss) * 1000).toDate();
        } else if (string.length() > timeNO_MS.length()) {
            //string = string.substring(0, timeNO_MS.length());
            return new DateTime(Long.parseLong(string.trim()) ).toDate();
        }
        return new DateTime(Long.parseLong(string.trim()) * 1000).toDate();
    }

//    int errorOnSimpleDateFormat;

    private Date trySimpleDateFormat(String string, long previousExpectedTime, boolean throwEx) {
        // if we blow up loads then give up....

        if (simpleDateTimeFormat == null) {
            simpleDateTimeFormat = getSimpleDateParser(format);
        }
        if (!StringUtil.containsNumber(string)) return null;
        try {
            Date date = simpleDateTimeFormat.parse(string);
            if (date == null) return null;
            if (isDateTooOld(date.getTime())) {
                DateTime dt = new DateTime(date);
                if (dt.getYear() == 1970 && string.contains(YY)) {
                    if (throwEx) throw new RuntimeException("Year too small");
                    else return null;
                } else {
                    dt = dt.plusYears(new DateTime(previousExpectedTime).getYear() - dt.getYear());
                    // if we are still in the past - try adding a year to see if we are in the future... return the lastest valid time...
                    if (dt.plusYears(1).getMillis() < DateTimeUtils.currentTimeMillis())
                        return dt.plusYears(1).toDate();
                    return dt.toDate();
                }
            }
//            errorOnSimpleDateFormat = 0;
            return date;
        } catch (ParseException e) {
//            errorOnSimpleDateFormat++;
            if (throwEx) throw new RuntimeException(e);
            return null;
        }
    }

    protected SimpleDateFormatPool getSimpleDateParser(String format) {
        return new SimpleDateFormatPool(formatString, null, 10, Integer.getInteger("simple.parse.pool.max",50), true);
    }

    static long oldTimeMs = new DateTime().minusYears(20).getMillis();
    private boolean isDateTooOld(long date) {
        return date < oldTimeMs;
    }



    static final String ZZZ = "zzz";
    static final String UNIX_LONG = "UNIX_LONG";
    private static final String COMMA = ",";
    private static final String SLASH = "/";
    private static final String DOR = ".";
    private static final String COLON = ":";
    private static final String DASH = "-";
    static final String SPACE = " ";
    private static final String QUOTE = "'";
    private static final String SEMICOLON = ";";

}
