package com.liquidlabs.log.util.notifier;

import com.liquidlabs.common.DateUtil;
import jregex.util.io.PathPattern;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 21/10/2015
 * Time: 08:46
 * To change this template use File | Settings | File Templates.
 */
public class CachingFSPathNotifier implements PathNotifier {
    static final Logger LOGGER = Logger.getLogger(CachingFSPathNotifier.class);
    private static int resetPeriodMins = Integer.getInteger("fs.path.cache.reset.mins", 5);
    private static int cacheTimeSensiSeconds = Integer.getInteger("fs.path.cache.sensi.secs", 120);

    private final String dir;
    private DateTime cacheTimestamp = new DateTime(0);
    private Set<File> dirs = new HashSet<File>();
    private int maxAgeDays;
    private long mostRecentMod = System.currentTimeMillis();
    private long lastRun = 0;

    public CachingFSPathNotifier(String dir, int maxAgeDays) {
        this.maxAgeDays = maxAgeDays;
        this.dir = dir;
    }
    public void setCacheTimeSensiSeconds(int seconds) {
        cacheTimeSensiSeconds = seconds;
    }

    @Override
    public Set<File> getDirs() {

        if (shouldThrottleDueToAge()) {
            if (isStillThrottling()) {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("GetDirs()<<:SKIP-DUE-TO-AGE" + dir + " mostRecentMod:" + new DateTime(mostRecentMod));
                return new HashSet<File>();
            }
        }

        lastRun = System.currentTimeMillis();

        int cacheTimeSensi = Integer.getInteger("fs.notifier.timesensi.secs", cacheTimeSensiSeconds);

        if (LOGGER.isDebugEnabled()) LOGGER.debug("GetDirs()>>:" + dir + " CacheStamp:" + DateUtil.shortTimeFormat.print(cacheTimestamp) + " TimeSensi:" + cacheTimeSensi);

        if (isCacheExpired()) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("GetDirs() - RESET TimeStamp");
            cacheTimestamp = new DateTime(0);
        }

        // if within our caching period - keep using cached dirs
        if (cacheTimestamp.getMillis() > System.currentTimeMillis() - (DateUtil.SECOND * cacheTimeSensi)) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("GetDirsCache()<<:" + dir);
            return getDirsUsingCacheSince(cacheTimestamp.getMillis());
        }
        cacheTimestamp = new DateTime();

        Set<File> dirs = getDirsUsingPP();
        if (LOGGER.isDebugEnabled()) LOGGER.debug("GetDirs()<<: dirs:" + dirs.size());
        return dirs;
    }

    private boolean isCacheExpired() {
        return cacheTimestamp.getMillis() < System.currentTimeMillis() - DateUtil.MINUTE * Integer.getInteger("fs.notifier.reset.mins",resetPeriodMins);
    }

    // throttle rule =- 5 mins for each day - max of 30 mins
    private boolean isStillThrottling() {
        int daysOld = (int) ((System.currentTimeMillis() - mostRecentMod)/ DateUtil.DAY);
        int minuteDelay = 5 + daysOld;
        if (minuteDelay > 30) minuteDelay = 30;
        if (LOGGER.isDebugEnabled()) LOGGER.debug("ThrottleMins:"+ minuteDelay + " Mod:" + new DateTime(mostRecentMod));

        return lastRun > System.currentTimeMillis() - (DateUtil.MINUTE * minuteDelay);
    }

    private boolean shouldThrottleDueToAge() {
        return mostRecentMod != 0 &&  mostRecentMod < System.currentTimeMillis() - DateUtil.DAY * 2;
    }

    private Set<File> getDirsUsingCacheSince(long since) {
        Set<File> results = new HashSet<File>();
        for (File dir : dirs) {
            if (dir.lastModified() >= (since - 1000)) {
                results.add(dir);
            }
        }
        return results;
    }

    private Set<File> getDirsUsingPP() {

        DateTime filesSince = new DateTime().minusDays(maxAgeDays);
        long newestTimeStamp = 0;

        Set<File> results = new HashSet<File>();
        dirs.clear();
        PathPattern pp = new PathPattern(dir);
        Enumeration enumeration = pp.enumerateFiles();
        while (enumeration.hasMoreElements()) {
            File file = (File) enumeration.nextElement();

            if (file.isDirectory()) {
                dirs.add(file);
                if (file.lastModified() > filesSince.getMillis() ) {
                    results.add(file);
                    if (file.lastModified() > newestTimeStamp) newestTimeStamp = file.lastModified();

                }
            } else {
                if (file.lastModified() > filesSince.getMillis() )  {
                    results.add(file.getParentFile());
                }
            }
        }
        mostRecentMod = newestTimeStamp;
        return results;

    }
}
