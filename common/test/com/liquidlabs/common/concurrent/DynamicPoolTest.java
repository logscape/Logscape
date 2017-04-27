package com.liquidlabs.common.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class DynamicPoolTest {

	@Test
	public void shouldQueueTasksWhenPoolIsBusy() throws Exception {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) ExecutorService.newSizedThreadPool(4, 4, new NamingThreadFactory("yat"));
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName() + " waiting:" + count.incrementAndGet());
                    try {
                        Thread.sleep(100);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            pool.execute(task);
        }
        Thread.sleep(1000);
        // check that all tasks completed and were not rejected
        assertEquals(10, count.get());

	}


		@Test
	public void shouldGrowPoolToHandleTasks() throws Exception {
		
		// make the pool shrink back down
		System.setProperty("dpool.idle.timeout.secs", "2");
		System.setProperty("executor.log.depleted","true");

		ThreadPoolExecutor pool = (ThreadPoolExecutor) ExecutorService.newDynamicThreadPool("poo", "test");
		
		printStats(pool);

		
		long start = System.currentTimeMillis();
		int tasks = 10;
		for (int i = 0; i < tasks; i++) {
			pool.submit(getTask());
		}
		
		printStats(pool);

        pool.shutdown();
		pool.awaitTermination(10, TimeUnit.SECONDS);
		long elapsed  = System.currentTimeMillis() - start;
		
		assertTrue("Took:" + elapsed, elapsed < 3100);
		
		Thread.sleep(3 * 1000);
		
		printStats(pool);
		
		
		
	}

	private void printStats(ThreadPoolExecutor pool) {
		int corePoolSize = pool.getCorePoolSize();
		int poolSize = pool.getPoolSize();
		int maximumPoolSize = pool.getMaximumPoolSize();
		
		System.out.println("Core:" + corePoolSize + " Max:" + maximumPoolSize + " Pool:"  + poolSize + " q:" + pool.getQueue().size());
	}

	private Runnable getTask() {
		return new Runnable() {
			public void run() {
				System.out.println(this.toString() + " Thread:" + Thread.currentThread().getName() + " Running");
				try {
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
	}

}
