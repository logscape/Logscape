package com.liquidlabs.log.util.notifier;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 21/10/2015
 * Time: 13:23
 * To change this template use File | Settings | File Templates.
 */
public class JDKWatchServiceNotifierTest {
    String dir = "./build/JDKWatchServiceNotifierTest";
    private int maxAgeDays = 1;

    @Test
    public void testGetFilesInSameDir() throws Exception {

        new File(dir).mkdir();

        PathNotifier notifier = new JDKWatchServiceNotifier(dir + "/**", maxAgeDays);

        // prime the pattern
        Set<File> files1 = notifier.getDirs();

        File newFile = new File(dir, "test" + System.currentTimeMillis());
        FileOutputStream fos = new FileOutputStream(newFile);
        fos.write("yay1\nyay2".getBytes());
        fos.close();
        Thread.sleep(10 * 1000);

        Set<File> files2 = notifier.getDirs();
        Assert.assertEquals(files2.toString(), 1, files2.size());


    }
    @Test
    public void testNewFilesInSameDirUsingCache() throws Exception {

        new File(dir).mkdir();

        PathNotifier notifier = new JDKWatchServiceNotifier(dir + "/**", maxAgeDays);

        // prime the pattern
        Set<File> files1 = notifier.getDirs();

        Thread.sleep(10 * 1000);

        File newFile = new File(dir, "test" + System.currentTimeMillis());
        FileOutputStream fos = new FileOutputStream(newFile);
        fos.write("yay1\nyay2".getBytes());
        fos.close();

        // should hit the cached dirs and find only the changed files with the last mod
        Set<File> files2 = notifier.getDirs();
        Assert.assertEquals(1, files2.size());


    }
}
