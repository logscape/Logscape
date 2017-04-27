package com.liquidlabs.log.util;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.LogProperties;
import org.joda.time.format.DateTimeFormat;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 09/04/2013
 * Time: 09:00
 * To change this template use File | Settings | File Templates.
 */
public class CharGrabber extends BaseGrabber {

    private char charStartPoint;
    int offset = 0;
    static int timeExtractLimit = LogProperties.getTimeExtractLimit();
    static boolean simpleFormatterLenient = System.getProperty("simpleFormatter.lenient", "true") == "true";

    public CharGrabber(String format) {
        super(format);
        char[] formatChars = format.toCharArray();
        if (format.equals(UNIX_LONG)) {
            format = "0s " + UNIX_LONG;
            formatString = format;

        }
        if (format.startsWith("char:")) {
            this.charStartPoint = formatChars["char:".length()];
            if (this.charStartPoint == 's') this.charStartPoint = ' ';
            if (this.charStartPoint == 't') this.charStartPoint = '\t';
            if (this.charStartPoint == 'n') this.charStartPoint = '\n';
            int formatOffset = format.indexOf(" ", "char:".length());
            char possibleChar = formatChars["char:".length() + 1];
            if (possibleChar != ' ' && (StringUtil.isInteger(new String(new char[]{possibleChar}))) != null) {
                try {
                    offset = Integer.parseInt(new String(new char[]{possibleChar}));
                } catch (Throwable t) {

                }
            } else if (possibleChar != ' ' && (StringUtil.isHex(new String(new char[]{possibleChar})))) {
                try {
                    offset = Integer.parseInt(new String(new char[]{possibleChar}), 16);
                } catch (Throwable t) {

                }
            }
            String dtFormat = format.substring(formatOffset + 1, format.length());
            format = dtFormat;
            type = Type.CHAR;
            formatString = format;
            if (format.contains(UNIX_LONG)) {
                formatString = UNIX_LONG + ".000";
                return;
            }
            if (format.contains(ZZZ)) {
                timeZoneOffset = format.indexOf(ZZZ);
                getSimpleDateParser(format);
                // FRICKIN Joda Time doesnt support 'zzz' its so shit so you end up with the wrong date
//				dateTimeFormat = DateTimeFormat.forPattern(format.substring(0, timeZoneOffset));
            } else {
                dateTimeFormat = DateTimeFormat.forPattern(format);
            }
        }

    }
    public boolean isRegularFormatter() {
        return false;
    }
    public static boolean isForMe(String format) {
        return format.startsWith("char:");
    }

    @Override
    public String grab(String nextLine) {
        if (isUnixLong == null) doPreprocessing();
        boolean done = false;
        int lineOffset = -1;
        int charHit = 0;
        if (formatString.startsWith("0s") && isUnixLong) {
            int endIndex = StringUtil.indexOfCharRange(nextLine, false, new char[]{'0', '9'});
            if (endIndex != -1 ) return nextLine.substring(0, endIndex);
            return "";
        }
        while (!done && lineOffset++ < timeExtractLimit && lineOffset < nextLine.length()) {
                if (nextLine.charAt(lineOffset) == charStartPoint) {
                    charHit++;
                    if (offset == 0 || charHit == offset) done = true;
                }
            }
        lineOffset++;
        while (lineOffset < nextLine.length() -1 && nextLine.charAt(lineOffset) == ' ') lineOffset++;
        if (!done) return "";
        if (nextLine.length() < lineOffset + formatString.length()) return "";
        return nextLine.substring(lineOffset, lineOffset + formatString.length()).trim();
    }

}
