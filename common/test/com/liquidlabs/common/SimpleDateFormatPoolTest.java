package com.liquidlabs.common;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 02/04/14
 * Time: 08:38
 * To change this template use File | Settings | File Templates.
 */
public class SimpleDateFormatPoolTest {

    @Test
    public void shouldExtractSubstringLinesFromFile() throws Exception {
        SimpleDateFormatPool formatPool = new SimpleDateFormatPool("--yyyy-MM-dd HH:mm:ss", TimeZone.getDefault(), 5, 7, true);
        for (int i = 0; i < 10; i++) {
            Date parse = formatPool.parse("--2014-10-01 12:00:0" + i);
            Assert.assertNotNull(parse);
            System.out.println("Parsed:" + parse);
        }
        System.err.println("Finished");
    }

    @Test
    public void shouldBeConcurrentlyGood() throws Exception {
        final SimpleDateFormatPool formatPool = new SimpleDateFormatPool("--yyyy-MM-dd HH:mm:ss", TimeZone.getDefault(), 5, 7, true);

        int size = 100;
        final AtomicInteger counted = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(size);
        for (int i = 0; i < size; i++) {
            final int cc = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Date parse = formatPool.parse("--2014-10-01 12:00:01");
                        Assert.assertNotNull(parse);
                        System.out.println(cc + " Parsed:" + parse);
                        counted.incrementAndGet();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        Assert.assertEquals(size, counted.get());

        System.out.println("Finished");
    }
}
