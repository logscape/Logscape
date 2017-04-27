package com.liquidlabs.common;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.tz.CachedDateTimeZone;

import java.text.SimpleDateFormat;

public class DateUtil {
    static int standardTZOffset = 0;//DateTimeZone.getDefault().getOffset(new DateTime());

	
	public static final int SECOND =  1000;
	public static final long MINUTE = 60 * 1000;
	public static final long HOUR = MINUTE * 60;
	public static final long DAY = HOUR * 24;
	public static final long WEEK = 7 * DAY;
	public static final long DX = HOUR * 24;

	public static long convertToTopOfTheDay(long millis, int dayDelta) {
		if (millis == 0) return 0;
		return (millis/(DAY)) * DAY + (DAY * dayDelta);
	}
	public static long getMinutesDiff(long from, long to) {
		long delta = to - from;
		return delta/MINUTE;
	}
	public static long convertToMin(long valueMs) {
		if (valueMs == 0) return 0;
		return valueMs/MINUTE;
	}
	public static long convertToSec(long valueMs) {
		if (valueMs == 0) return 0;
		return valueMs/SECOND;
	}

	public static long floorMin(long valueMs) {
		return convertToMin(valueMs) * MINUTE;
	}
	public static long floorSec(long valueMs) {
		return convertToSec(valueMs) * SECOND;
	}
	public static long floorHour(long millis) {
		if (millis == 0) return 0;
		return (millis/(HOUR)) * HOUR;
	}
	public static long floorDay(long millis) {
        if (millis == 0) return 0;
		return ((millis/(DAY)) * DAY);
	}

	
	/**
	 * Converts time to fall into a Mod Bucket value = i.e. 16:05 with granularity of 10 would roll to 16:10
	 * @param givenTimeMs
	 * @param granularityMins
	 * @return
	 */
	public static long rollMsToMinuteMod(long givenTimeMs,int granularityMins, int delta) {
		long convertToMin = convertToMin(givenTimeMs);
		DateTime time = new DateTime(givenTimeMs);
		int minuteOfHour = time.getMinuteOfHour();
		int mod = minuteOfHour % granularityMins;
		if (mod == 0 || mod + delta == 0) return convertToMin * MINUTE;
		
		int newMinute = (mod + delta) * granularityMins;
		// scrub out minutes
		long millis = new DateTime(time.getYear(), time.getMonthOfYear(), time.getDayOfMonth(), time.getHourOfDay(), 0, 0, 0).getMillis();
		// add on calculated Minutes
		return millis + newMinute * MINUTE;
	}
	
	public static String getNowTimeString() {
		return DateUtil.shortTimeFormat.print(DateTimeUtils.currentTimeMillis());
	}

	
	static String shortFormat = "dd-MM-yy HH:mm";
	public static DateTimeFormatter shortDateTimeFormat = DateTimeFormat.forPattern(shortFormat);
	
	static String shortFormat1 = "dd-MM-yy_HHmm";
	public static DateTimeFormatter shortDateTimeFormat1 = DateTimeFormat.forPattern(shortFormat1);
	
	static String shortFormat1_1 = "dd-MM-yy_HHmm.ss";
	public static DateTimeFormatter shortDateTimeFormat1_1 = DateTimeFormat.forPattern(shortFormat1_1);

	
	static String shortFormat2 = "dd-MMM-yy HH:mm";
	public static DateTimeFormatter shortDateTimeFormat2 = DateTimeFormat.forPattern(shortFormat2);
    public static SimpleDateFormat shortDateTimeParser2 = new SimpleDateFormat(shortFormat2);

	
	static String shortFormat22 = "dd-MMM-yyyy HH:mm";
	public static DateTimeFormatter shortDateTimeFormat22 = DateTimeFormat.forPattern(shortFormat22);
	
	static String shortDTFormat3 = "dd-MMM-yy HH:mm:ss";
	public static DateTimeFormatter shortDateTimeFormat3 = DateTimeFormat.forPattern(shortDTFormat3);
	
	static String shortDTFormat4 = "dd-MMM-yyyy HH:mm:ss";
	public static DateTimeFormatter shortDateTimeFormat4 = DateTimeFormat.forPattern(shortDTFormat4);
	
	static String shortDTFormat5 = "dd-MMM-yy_HH-mm-ss";
	public static DateTimeFormatter shortDateTimeFormat5 = DateTimeFormat.forPattern(shortDTFormat5);
	
	//25-10-2009 15:00.06
	static String shortDTFormat6 = "dd-MMM-yy HH:mm:ss";
	public static DateTimeFormatter shortDateTimeFormat6 = DateTimeFormat.forPattern(shortDTFormat6);
	
	static String shortDTFormat7 = "yyyy-MM-dd HH:mm:ss";
	public static DateTimeFormatter shortDateTimeFormat7 = DateTimeFormat.forPattern(shortDTFormat7);

	//1985-04-12T23:20:50.52Z
	static String shortDTFormat8 = "yyyy-MM-dd'T'HH:mm:ssZ";
	public static DateTimeFormatter shortDateTimeFormat8 = DateTimeFormat.forPattern(shortDTFormat8);



	static String shortDTFormat1 = "HH:mm:ss.SSS";
	public static DateTimeFormatter shortTimeFormat = DateTimeFormat.forPattern(shortDTFormat1);
	static String shortDTFormat2 = "HH-mm-ss";
	public static DateTimeFormatter shortTimeFormat2 = DateTimeFormat.forPattern(shortDTFormat2);
	static String shortTimeFormat3 = "HH:mm:ss";
	public static DateTimeFormatter shortTimeFormatter3 = DateTimeFormat.forPattern(shortTimeFormat3);

    public static DateTimeFormatter shortTimeFormatter4 = DateTimeFormat.forPattern("HH:mm:ss");
	
	static String shortFormat3 = "yyMMMdd";
	public static DateTimeFormatter shortDateFormat = DateTimeFormat.forPattern(shortFormat3);

	//2008-11-11 17:02:19,538
	static String log4jFormatString = "yyyy-MM-dd HH:mm:ss,SSS";
	public static DateTimeFormatter log4jFormat = DateTimeFormat.forPattern(log4jFormatString);

	static String longDTFormat = "dd-MMM-yy HH:mm:ss,SSS";
	public static DateTimeFormatter longDTFormatter = DateTimeFormat.forPattern(longDTFormat);

    public static DateTimeFormatter yearFirstDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd");

	public static long nearestMin(long from, int base) {
        from = (Math.round(from / MINUTE)) * MINUTE;
        return ((from / (base * MINUTE)) * (base * MINUTE));
	}

//	static CachedDateTimeZone forZone = CachedDateTimeZone.forZone(CachedDateTimeZone.UTC);
//    static CachedDateTimeZone localZone = CachedDateTimeZone.forZone(CachedDateTimeZone.getDefault());
//
//	public static long toUTC(long time) {
//		return localZone.convertLocalToUTC(time, false);
//	}
//	public static long fromUTC(long time) {
//		return forZone.convertUTCToLocal(time);
//	}
	
}
