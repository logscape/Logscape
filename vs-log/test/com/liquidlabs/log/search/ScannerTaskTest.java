package com.liquidlabs.log.search;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/04/2014
 * Time: 14:55
 * To change this template use File | Settings | File Templates.
 */
public class ScannerTaskTest {
    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testIsComplete() throws Exception {

        ScannerTask scannerTask = new ScannerTask();
        Assert.assertTrue(!scannerTask.isComplete());
        scannerTask.done = true;
        Assert.assertTrue(scannerTask.isComplete());

    }

    @Test
    public void testGetCompleteEvents() throws Exception {

    }

    @Test
    public void testGetPercentComplete() throws Exception {
        ScannerTask t1 = new ScannerTask();
        ScannerTask t2 = new ScannerTask();
        ScannerTask t3 = new ScannerTask();
        ScannerTask t4 = new ScannerTask();
        List<ScannerTask> tasks = Arrays.asList(t1, t2, t3, t4);

        Assert.assertEquals(0, ScannerTask.getPercentComplete(tasks));
        t1.done = true;

        Assert.assertEquals(25, ScannerTask.getPercentComplete(tasks));

        t2.done = true;
        Assert.assertEquals(50, ScannerTask.getPercentComplete(tasks));

        t3.done = true;
        Assert.assertEquals(75, ScannerTask.getPercentComplete(tasks));

        t4.done = true;
        Assert.assertEquals(100, ScannerTask.getPercentComplete(tasks));


    }

    @Test
    public void testGetScannedCount() throws Exception {

    }

}
