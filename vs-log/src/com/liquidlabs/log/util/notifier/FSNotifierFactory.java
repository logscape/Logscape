package com.liquidlabs.log.util.notifier;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 21/10/2015
 * Time: 15:53
 * To change this template use File | Settings | File Templates.
 */
public class FSNotifierFactory {
    static String type = System.getProperty("fs.notifier", "caching");

    static public PathNotifier getNotifier(String dir, int maxAgeDays) {
        if (type.equals("caching"))    return new CachingFSPathNotifier(dir, maxAgeDays);
        if (type.equals("jdkWatch"))    return new JDKWatchServiceNotifier(dir, maxAgeDays);
        return new FSPathNotifier(dir, maxAgeDays);

    }
}
