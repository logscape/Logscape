package com.logscape.disco.indexer;

import com.liquidlabs.common.concurrent.ExecutorService;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;


public class KvIndexFeedThreadTest {


    @Test
    public void shouldDoNothing() {

    }
//    @Test
    public void shouldSmokeItUp() throws Exception {
        final IndexFeed feed = KvIndexFactory.get();

        final ThreadPoolExecutor test = (ThreadPoolExecutor) ExecutorService.newFixedThreadPool(5, "Test");


        final Runnable runnable = new Runnable() {

            @Override
            public void run() {
                for (int j = 0; j < 100; j++) {
                    for (int i = 0; i < 100; i++) {
                        feed.index(j, "", i + 1, 1, "2013-09-18 10:29:11,702 INFO pool-2-thread-1 (license.TrialListener)\t Action:'Download' Email:'izam.my@gmail.com' IpAddress:'175.143.7.68' Company:'sadasd'", false, false, false, null);
                        try {
                            Thread.sleep(5L);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            }
        };

        test.submit(runnable);
        Thread.sleep(1000);
        feed.compact();
        while (test.getActiveCount() > 0) {
            Thread.sleep(5);
        }
        System.out.println(feed.get(1, 1));
        test.submit(new Runnable() {
            @Override
            public void run() {
                for (int j = 0; j < 100; j++) {
                    feed.remove(j);
                    try {
                        Thread.sleep(5L);
                    } catch (InterruptedException e) {

                    }
                }
            }
        });
        feed.compact();
        Thread.sleep(5000);
        final List<Pair> fieldIs = feed.get(100, 1);
        System.out.println(fieldIs);
        feed.compact();
    }

}



