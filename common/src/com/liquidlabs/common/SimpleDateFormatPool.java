package com.liquidlabs.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 */
public class SimpleDateFormatPool {

    private final String formatString;
    private final Pool pool;
    private final TimeZone timeZone;

    public SimpleDateFormatPool(String format, TimeZone timeZone, int initialPoolSize, int maxPoolSize, final boolean lenient) {
        formatString = format;
        this.timeZone = timeZone;
        // test the formatter to see if it blows up or not...
        try {
            new SimpleDateFormat(formatString);
        } catch(Throwable t) {
            System.err.println("FormatString:" + formatString);
            t.printStackTrace();
            pool = null;
            return;
        }
        pool = new Pool(initialPoolSize, maxPoolSize, new Pool.Factory() {
            public Object newInstance() {
                SimpleDateFormat dateFormat = new SimpleDateFormat(formatString);
                dateFormat.setLenient(lenient);
                return dateFormat;
            }

        });
    }

    public String format(Date date) {
        if (pool == null)  return null;
        DateFormat format = fetchFromPool();
        try {
            return format.format(date);
        } finally {
            pool.putInPool(format);
        }
    }

    public Date parse(String date) throws ParseException {
        if (pool == null)  return null;
        DateFormat format = fetchFromPool();
        try {
            return format.parse(date);
        } finally {
            pool.putInPool(format);
        }
    }

    private DateFormat fetchFromPool() {
        DateFormat format = (DateFormat)pool.fetchFromPool();
        TimeZone tz = timeZone != null ? timeZone : TimeZone.getDefault();
        if (!tz.equals(format.getTimeZone())) {
            format.setTimeZone(tz);
        }
        return format;
    }

    public String toString() {
        return formatString;
    }
}
