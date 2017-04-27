package com.liquidlabs.common;

import org.joda.time.DateTime;

import java.util.Calendar;
import java.util.TimeZone;


/**
 * Calculates diff between this timezone and another one
 *
 */
public class TimeZoneDiffer {
    static int thisRaw  = TimeZone.getDefault().getOffset(new DateTime().getMillis());

    public static int getHoursDiff(String otherTzId) {
        // assume default if nothin specified
        if (otherTzId == null || otherTzId.length() == 0) return 0;
        TimeZone otherTz = TimeZone.getTimeZone(otherTzId);
        int otherTzOffset = otherTz.getOffset(new DateTime().getMillis());
        int delta = thisRaw - otherTzOffset;
        if (delta == 0) return 0;
        else return (int)  ( delta/DateUtil.HOUR);
    }
}
