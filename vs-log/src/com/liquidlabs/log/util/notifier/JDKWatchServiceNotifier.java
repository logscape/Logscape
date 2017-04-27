package com.liquidlabs.log.util.notifier;

import com.liquidlabs.common.DateUtil;
import jregex.util.io.PathPattern;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Based on the JDK Watch Service
 * https://docs.oracle.com/javase/7/docs/api/java/nio/file/WatchService.html
 *
 * folder.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
 * faster -
 * folder.register(watcher, new WatchEvent.Kind[]{StandardWatchEventKinds.ENTRY_MODIFY}, SensitivityWatchEventModifier.HIGH);
 */
public class JDKWatchServiceNotifier implements PathNotifier {

    private final PathPattern pp;
    private WatchService watchService;
    private DateTime cacheTimestamp = new DateTime(0);
    private int resetPeriodMins = 5;
    private int timeSensiSeconds = 60;
    private int maxAgeDays;


    public JDKWatchServiceNotifier(String dir, int maxAgeDays) {
        this.maxAgeDays = maxAgeDays;
        pp = new PathPattern(dir);

        try {
            FileSystem fileSystem = FileSystems.getDefault();
            watchService = fileSystem.newWatchService();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    public void setCacheTimeSensiSeconds(int seconds) {
        timeSensiSeconds = seconds;
    }

    @Override
    public Set<File> getDirs() {
        if (cacheTimestamp.getMillis() < System.currentTimeMillis() - DateUtil.MINUTE * resetPeriodMins) {
            cacheTimestamp = new DateTime(0);
        }
        if (cacheTimestamp.getMillis() > System.currentTimeMillis() - (DateUtil.SECOND * timeSensiSeconds)) {
            return getCachedFilesSince(0);
        }
        cacheTimestamp = new DateTime();
        return getDirsUsingPP();
    }

    private Set<File> getCachedFilesSince(long since) {
        Set<File> results = new HashSet<File>();

        try {
            WatchKey poll = watchService.poll(10, TimeUnit.MILLISECONDS);
            if (poll != null && poll.isValid()) {
                List<WatchEvent<?>> watchEvents = poll.pollEvents();
                for (WatchEvent<?> event : watchEvents) {
                    Path target = (Path)event.context();
                    //System.out.println("GotEvent" + event);
                    File e = target.toFile();
                    if (e.isDirectory()) {
                        results.add(e);
                    } else {
                        results.add(e.getAbsoluteFile().getParentFile());
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            results.clear();
        }
        return results;
    }

    private Set<File> getDirsUsingPP() {

        DateTime filesSince = new DateTime().minusDays(maxAgeDays);

        Set<File> results = new HashSet<File>();
        String firstDir = null;
        List<String> dirs = new ArrayList<String>();
        Enumeration enumeration = pp.enumerateFiles();
        while (enumeration.hasMoreElements()) {
            File file = (File) enumeration.nextElement();
            if (file.isDirectory()) {
                if (firstDir == null) firstDir = file.getAbsolutePath();
                else {
                    dirs.add(file.getAbsolutePath());
                    results.add(file);
                }
            } else {
                if (file.lastModified() > filesSince.getMillis() )  {
                    results.add(file.getParentFile());
                }
            }
        }

        if (firstDir != null) {
            Path directory = Paths.get(firstDir, (String[]) dirs.toArray(new String[0]));
            WatchEvent.Kind<?>[] events = { StandardWatchEventKinds.ENTRY_CREATE   };
            try {
                watchService.close();
                FileSystem fileSystem = FileSystems.getDefault();
                watchService = fileSystem.newWatchService();
                directory.register(watchService, events);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return results;

    }
}
