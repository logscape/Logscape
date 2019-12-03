package com.logscape.disco;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.vso.VSOProperties;
import org.joda.time.DateTime;


public class DiscoProperties {

    public static boolean isForwarder = VSOProperties.getResourceType().contains("Forwarder");


    // derive all other bucket times from this... we use minutes from 2010 so we can store IntValues
    public static long TIME_BASE = 0;
    static {
        TIME_BASE = DateUtil.convertToMin(new DateTime(2010, 1, 1, 0, 0, 0).getMillis());
    }
    // Stored as SEc - 1 Year == 31Million, Int Max == 21B = 67Years
    public static int fromMsToSec(long timeMs) {
        int result = (int)(DateUtil.convertToSec(timeMs) - DiscoProperties.TIME_BASE);
        if (result < 0) result = 0;
        return result;
    }
    public static long fromSecToMs(int timeSec) {
        return ((long) (timeSec + DiscoProperties.TIME_BASE) * DateUtil.SECOND);
    }


    public static String getDBRoot() {
        return System.getProperty("db.root", "work/DB");
    }
    public static String getEventsDB() {
        return getDBRoot() + "/" + "events";
    }
    public static String getKVIndexDB() {
        return getDBRoot() + "/" + "kv-index";
    }

    public static String getFunctionSplit() {
        return "!";
    }

    public static int getMaxKVDiscoveredFieldValueLength() {
        return Integer.getInteger("kv.max.value.length", 128);
    }

    public static int getMaxKVDictionaryLimit() {
        return Integer.getInteger("kv.max.dictionary.size", 10000);
    }

}
