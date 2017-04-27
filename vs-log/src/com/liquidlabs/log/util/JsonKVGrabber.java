package com.liquidlabs.log.util;

import com.liquidlabs.log.LogProperties;
import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Locale;

/**
 * Created with IntelliJ IDEA.
 * { key="time:", format: "YYY-MM-DDD HHH-MM-SSS"  }
 */
public class JsonKVGrabber extends BaseGrabber {

    private final String key;
    private char charStartPoint;
    private final String locale;
    int offset = 0;
    int timeExtractLimit = LogProperties.getTimeExtractLimit();

    public JsonKVGrabber(String config) {
        super("");

        JSONObject parsed = (JSONObject) JSONValue.parse(config);
        key = (String) parsed.get("key");
        locale = (String) parsed.get("locale");
        format = (String) parsed.get("format");

        if (format.contains(UNIX_LONG)) {
            formatString = UNIX_LONG + ".000";
            return;
        }

        if (format.contains(ZZZ)) {
            timeZoneOffset = format.indexOf(ZZZ);
            // SimpleDateFormat has MT issues and 'parse' takes 50ms
            simpleDateTimeFormat = super.getSimpleDateParser(format);
            // FRICKIN Joda Time doesnt support 'zzz' its so shit so you end up with the wrong date
//				dateTimeFormat = DateTimeFormat.forPattern(format.substring(0, timeZoneOffset));
        } else {
            dateTimeFormat = DateTimeFormat.forPattern(format);
            if (locale != null) {
                Locale nl = new Locale(locale);
                dateTimeFormat = dateTimeFormat.withLocale(nl);
            }
        }
        formatString = format;
    }
    public static boolean isForMe(String format) {
        return format.startsWith("{") && format.contains("\"format\":");
    }

    @Override
    public String grab(String nextLine) {
        int from = 0;
        if (key != null) from = nextLine.indexOf(key);
        if (from == -1) return null;
        if (key != null) from += key.length();
        int tempFrom = from;
        while (!isNumber(nextLine.charAt(from), from) && from < nextLine.length()) {
            from++;
        }
        if (from == nextLine.length()) from = tempFrom;

        int to = from + format.length();
        if (to > nextLine.length()) to = nextLine.length();

        return nextLine.substring(from, to).trim();
    }

    private boolean isNumber(char nextFrom, int from) {
        return nextFrom >= '0' && nextFrom <= '9';
    }

}
