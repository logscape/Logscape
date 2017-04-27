package com.liquidlabs.log.util;

import com.thoughtworks.xstream.core.util.ThreadSafeSimpleDateFormat;
import org.joda.time.format.DateTimeFormat;

import java.text.SimpleDateFormat;

public class DateTimeGrabber extends BaseGrabber {

        int offset = 0;
        char charGap = '\t';


        public DateTimeGrabber(String format) {
            super(format);
            if (format.length() > 2) {
                if (format.equals(UNIX_LONG)) format = "0s " + UNIX_LONG;
                char[] formatChars = format.toCharArray();
                if (formatChars[1] == 't' || formatChars[2] == 't') {
                    offset = getOffsetFromString(format, "t");
                    format = format.substring(format.indexOf(SPACE) + 1, format.length());
                    type = Type.TAB;
                } else if (formatChars[1] == 's' || formatChars[2] == 's') {
                    offset = getOffsetFromString(format, "s");
                    format = format.substring(format.indexOf(SPACE) + 1, format.length());
                    charGap = ' ';
                    type = Type.SPACE;

                } else if ((formatChars[1] == 'c' || formatChars[2] == 'c') && format.contains("csv")) {
                    charGap = ',';
                    offset = getOffsetFromString(format, "csv");
                    type = Type.CSV;
                    formatString = format;
                } else if ((formatChars[0] == 'd' || formatChars[1] == 'd') && format.contains("delim:")) {
                    int index = format.indexOf("delim:") + "delim:".length();
                    charGap = format.charAt(index);
                    offset = getOffsetFromString(format, "delim:" +charGap);
                    type = Type.DELIM;
                    formatString = format.substring(format.indexOf(" ")+1);
                    dateTimeFormat = DateTimeFormat.forPattern(formatString);

                    return;
                }
                if (format.contains(UNIX_LONG)) {
                    formatString = UNIX_LONG + "0";
                    return;
                }
            }

            formatString = format;
            if (format.contains(ZZZ)) {
                timeZoneOffset = format.indexOf(ZZZ);
                simpleDateTimeFormat = super.getSimpleDateParser(format);
                // FRICKIN Joda Time doesnt support 'zzz' its so shit so you end up with the wrong date
//				dateTimeFormat = DateTimeFormat.forPattern(format.substring(0, timeZoneOffset));
            } else {
                dateTimeFormat = DateTimeFormat.forPattern(format);
            }
        }

        final public String grab(final String nextLine) {

            if ( type.equals(Type.DELIM)) {
                if (charGap == 's') {
                    String[] splitLine = nextLine.split("\\s+");
                    if (this.offset < splitLine.length) return splitLine[this.offset];
                } else if (charGap == 't') {
                    String[] splitLine = nextLine.split("\\t");
                    if (this.offset < splitLine.length) return splitLine[this.offset];
                } else {
                    String[] splitLine = nextLine.split(charGap + "");
                    if (this.offset < splitLine.length) return splitLine[this.offset];

                }
                return null;
            }


            if (nextLine.length() < formatString.length()) return nextLine;
            if (this.offset == 0) return nextLine.substring(0, formatString.length());
            int lineOffset = 0;
            boolean done = false;
            int hitCount = 0;
            // count the SPACE, CSV or TAB hits to locate the correct offset
            boolean wasLastAHit = false;
            if (type.equals(Type.SPACE) || type.equals(Type.CSV) || type.equals(Type.TAB)) {

                while (!done && lineOffset < 128 && lineOffset < nextLine.length()) {
                    if (nextLine.charAt(lineOffset) == charGap) {
                        if (!wasLastAHit) hitCount++;
                        wasLastAHit = true;
                    } else {
                        wasLastAHit = false;
                    }
                    if (hitCount == offset) done = true;
                    lineOffset++;
                }
            } else {
                lineOffset = this.offset;
            }

            if (!done) {
                lineOffset = this.offset;
            }

            if (isLineTooShort(nextLine, lineOffset, formatString.length())) return nextLine;
            else {
                try {
                    if (type.equals(Type.CSV)) {
                        try {
                            int indexOf = nextLine.indexOf(charGap, lineOffset);
                            if (indexOf == -1) return nextLine;
                            return nextLine.substring(lineOffset, indexOf);
                        } catch (Throwable t) {
                            return nextLine;
                        }
                    }
                    while (lineOffset + formatString.length() < nextLine.length() && nextLine.charAt(lineOffset) == ' ' && lineOffset < 128)
                        lineOffset++;
                    return nextLine.substring(lineOffset, (lineOffset + formatString.length()));
                } catch (Throwable t) {
                    DateTimeExtractor.LOGGER.warn("failure on:" + nextLine + " format:" + formatString + " ex:" + t);
                    return nextLine;
                }
            }
        }

        boolean isLineTooShort(String nextLine, int offset, int formatLength) {
            return nextLine.length() < offset + formatLength;
        }

        int getOffsetFromString(String format, String indexOf) {
            int spaceIndex = format.indexOf(SPACE);
            if (spaceIndex == -1) spaceIndex = format.length();
            return Integer.parseInt(format.substring(0, spaceIndex).replaceAll(indexOf, ""));
        }

        public String toString() {
            StringBuffer buffer = new StringBuffer();
            buffer.append("[DateTimeFormatter:");
            buffer.append(" format:");
            buffer.append(format);
            buffer.append(" formatS:");
            buffer.append(formatString);
            buffer.append(" format:");
            buffer.append(dateTimeFormat);
            buffer.append("]\n");
            return buffer.toString();
        }
}