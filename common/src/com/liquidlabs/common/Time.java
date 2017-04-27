package com.liquidlabs.common;

import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Time {
	static DateTimeFormatter forPattern = DateTimeFormat.forPattern("yyyy-MMM-dd HH:mm.ss.SSS");
	
	public static String nowAsString() {
		return forPattern.print(DateTimeUtils.currentTimeMillis());
	}
    public static String nowAsStringUTC() {
        return forPattern.withZoneUTC().print(DateTimeUtils.currentTimeMillis());
    }

    public static DateTimeFormatter formatter() {
		return forPattern;
	}

}
