package com.liquidlabs.log.util.notifier;

import jregex.util.io.PathPattern;
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
public class FSPathNotifier implements PathNotifier {

    private final PathPattern pp;
    private final String dir;
    private final int maxAgeDays;

    public  FSPathNotifier(String dir, int maxAgeDays) {
        this.dir = dir;
        this.maxAgeDays = maxAgeDays;
        pp = new PathPattern(dir);
    }

    @Override
    public Set<File> getDirs() {
        DateTime filesSince = new DateTime().minusDays(maxAgeDays);

        Set<File> results = new HashSet<File>();
        Enumeration enumeration = pp.enumerateFiles();
        while (enumeration.hasMoreElements()) {
            File file = (File) enumeration.nextElement();
            if (!file.isDirectory()) file = file.getParentFile();
            if (file.lastModified() > filesSince.getMillis() )  results.add(file);
            results.add(file);
        }
        return results;
    }

    @Override
    public void setCacheTimeSensiSeconds(int seconds) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
