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
 * Time: 12:37
 * To change this template use File | Settings | File Templates.
 */
public class CachingFSPathNotifierTest {
    String dir = "./build/testNotifier";
    private int maxAgeDays = 1;

    @Test
    public void testGetFilesInSameDir() throws Exception {
        dir = dir+"1";

        File dirr = new File(dir);
        dirr.mkdir();

        CachingFSPathNotifier notifier = new CachingFSPathNotifier(dir + "/**", maxAgeDays);
        Set<File> files1 = notifier.getDirs();


        new File(dir,"test" + System.currentTimeMillis()).mkdir();

        Set<File> files2 = notifier.getDirs();
        Assert.assertEquals(files2.toString(), 1, files2.size());


    }
    @Test
    public void testNewFilesInSameDirUsingCache() throws Exception {
        dir = dir+"2";
        new File(dir).mkdir();

        CachingFSPathNotifier notifier = new CachingFSPathNotifier(dir + "/**", maxAgeDays);
        Set<File> files1 = notifier.getDirs();

        notifier.setCacheTimeSensiSeconds(10);
        Thread.sleep(6 * 1000);

        File newFile = new File(dir, "test" + System.currentTimeMillis());
        FileOutputStream fos = new FileOutputStream(newFile);
        fos.write("yay1\nyay2".getBytes());
        fos.close();

        // should hit the cached dirs and find only the changed files with the last mod
        Set<File> files2 = notifier.getDirs();
        Assert.assertEquals(1, files2.size());


    }
}
