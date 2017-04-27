package com.logscape.disco;

import com.liquidlabs.common.DateUtil;
import com.logscape.disco.indexer.KvLutDb;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 08/04/2014
 * Time: 11:15
 * To change this template use File | Settings | File Templates.
 */
public class SystemIndexer {
    public DateTimeFormatter shortDateTimeFormat2 = DateTimeFormat.forPattern(System.getProperty("system.fields._date.format", "dd-MMM-yyyy"));

    public void indexA(long timeMs, HashMap<String, String> collecting) {

        if (timeMs == -1) return;
        timeMs = DateUtil.floorMin(timeMs);

        if (timeMs == local.lastTime) {
            for (int i = 0; i < TIMES.values().length; i++) {
                collecting.put(TIMES.values()[i].name(), local.times[i]);
            }

        } else {
            local.lastTime = timeMs;
            DateTime eventTime = new DateTime(timeMs);
            local.times[TIMES._day.ordinal()] = eventTime.dayOfWeek().getAsShortText();

            String _dayOfMonth = Integer.toString(eventTime.getDayOfMonth());
            local.times[TIMES._dayOfMonth.ordinal()] = _dayOfMonth;

            String _month = eventTime.monthOfYear().getAsShortText();
            local.times[TIMES._month.ordinal()] = _month;

            String _monthOfYear = Integer.toString(eventTime.getMonthOfYear());
            local.times[TIMES._monthOfYear.ordinal()] = _monthOfYear;

            String _hour = Integer.toString(eventTime.getHourOfDay());
            local.times[TIMES._hour.ordinal()] = _hour;

            String _minute = Integer.toString(eventTime.getMinuteOfHour());
            local.times[TIMES._minute.ordinal()] = _minute;


            String _date = shortDateTimeFormat2.print(eventTime);
            local.times[TIMES._date.ordinal()] = _date;

            for (int i = 0; i < TIMES.values().length; i++) {
                collecting.put(TIMES.values()[i].name(), local.times[i]);
            }
        }
    }

    private enum TIMES  { _day, _dayOfMonth, _month, _monthOfYear, _hour, _minute, _date};

    public Map<String, String> index(long timeMs) {
        TreeMap<String, String> result = new TreeMap<String, String>();
        DateTime eventTime = new DateTime(timeMs);
        result.put(TIMES._day.name(), eventTime.dayOfWeek().getAsShortText());
        result.put(TIMES._dayOfMonth.name(), Integer.toString(eventTime.getDayOfMonth()));

        result.put(TIMES._month.name(), eventTime.monthOfYear().getAsShortText());
        result.put(TIMES._monthOfYear.name(), Integer.toString(eventTime.getMonthOfYear()));

        result.put(TIMES._hour.name(), Integer.toString(eventTime.getHourOfDay()));
        result.put(TIMES._minute.name(), Integer.toString(eventTime.getMinuteOfHour()));
        result.put(TIMES._date.name(), shortDateTimeFormat2.print(eventTime));
        return result;
    }



    private static class LastResults {
        String[] times = new String[TIMES.values().length];
        long lastTime = 0;
    }
    LastResults local = new LastResults();

    public void indexA(long timeMs, KvLutDb.Addit addit) {

        if (timeMs == -1) return;
        timeMs = DateUtil.floorMin(timeMs);

        if (timeMs == local.lastTime) {
            for (int i = 0; i < TIMES.values().length; i++) {
                addit.append(TIMES.values()[i].name(), local.times[i]);
            }

        } else {
            local.lastTime = timeMs;
            DateTime eventTime = new DateTime(timeMs);
            local.times[TIMES._day.ordinal()] = eventTime.dayOfWeek().getAsShortText();

            String _dayOfMonth = Integer.toString(eventTime.getDayOfMonth());
            local.times[TIMES._dayOfMonth.ordinal()] = _dayOfMonth;

            String _month = eventTime.monthOfYear().getAsShortText();
            local.times[TIMES._month.ordinal()] = _month;

            String _monthOfYear = Integer.toString(eventTime.getMonthOfYear());
            local.times[TIMES._monthOfYear.ordinal()] = _monthOfYear;

            String _hour = Integer.toString(eventTime.getHourOfDay());
            local.times[TIMES._hour.ordinal()] = _hour;

            String _minute = Integer.toString(eventTime.getMinuteOfHour());
            local.times[TIMES._minute.ordinal()] = _minute;


            String _date = shortDateTimeFormat2.print(eventTime);
            local.times[TIMES._date.ordinal()] = _date;

            for (int i = 0; i < TIMES.values().length; i++) {
                addit.append(TIMES.values()[i].name(), local.times[i]);
            }
        }
    }


}
