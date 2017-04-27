package com.liquidlabs.common.concurrent;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Provide Access to a DynamicGrowShrinkThreadPool executor thingy and also a
 * Warning based queue that generates log warnings when the Runnable work queue
 * is greater than 1000.
 * 
 * @author neil
 */
public class ExecutorService {

	private static final Logger LOGGER = Logger.getLogger(ExecutorService.class);
	static String TEST_MODE = "test.mode";


	/**
	 * Same as regular jdk version except this will log pool state. Use when limiting concurrent activity
	 * 
	 */
	public static java.util.concurrent.ExecutorService newFixedThreadPool(int tCount, String name) {
		ThreadPoolExecutor result = new ThreadPoolExecutor(tCount, tCount, 0L, TimeUnit.MILLISECONDS, 
				new LinkedBlockingQueue<Runnable>(), new NamingThreadFactory(name));
		return result;
	}
	
	public static java.util.concurrent.ExecutorService newFixedPriorityThreadPool(int tCount, int queueSize, String name) {
		return new PriorityThreadPoolExecutor(tCount, tCount, 0L, queueSize, TimeUnit.MILLISECONDS, new NamingThreadFactory(name));
	}
    public static java.util.concurrent.ExecutorService newFixedPriorityThreadPool(int tCount, int queueSize, String name, int threadPriority) {
        return new PriorityThreadPoolExecutor(tCount, tCount, 0L, queueSize, TimeUnit.MILLISECONDS, new NamingThreadFactory(name, true, threadPriority));
    }

	


	/***
	 * Same as regular jdk version except this will log pool state.
	 * 
	 */

	public static ScheduledThreadPoolExecutor newScheduledThreadPool(int tCount, final NamingThreadFactory threadFactory) {

		if (!Boolean.getBoolean(TEST_MODE) && pools.containsKey(threadFactory.getNamePrefix())) return (ScheduledThreadPoolExecutor) pools.get(threadFactory.getNamePrefix());

		ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(tCount, threadFactory);
        scheduler.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                    executor.shutdownNow();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                LOGGER.warn("Execution was rejected!)");
            }
        });
		scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		pools.put(threadFactory.getNamePrefix(), scheduler);
		return scheduler;
	}

	public static ScheduledExecutorService newScheduledThreadPool(int count, String prefix) {
		return newScheduledThreadPool(count, new NamingThreadFactory(prefix, true, Thread.NORM_PRIORITY + 1));
	}
    private static int defaultSchedulerCount = Integer.getInteger("scheduler.count", 8);
    public static ScheduledExecutorService newScheduledThreadPool(String prefix) {
        return newScheduledThreadPool(defaultSchedulerCount, new NamingThreadFactory(prefix, true, Thread.NORM_PRIORITY + 1));
    }


    public static ScheduledExecutorService newScheduledThreadPool(int count, String prefix, Thread.UncaughtExceptionHandler exceptionHandler) {
        return newScheduledThreadPool(count, new NamingThreadFactory(prefix, true, Thread.NORM_PRIORITY + 1, exceptionHandler));
    }

	public static ScheduledExecutorService newScheduledThreadPool(int count, String prefix, int priority) {
		return newScheduledThreadPool(count, new NamingThreadFactory(prefix, true, priority));
	}

    public static ScheduledExecutorService newScheduledThreadPool(int count, String prefix, int maxPriority, Thread.UncaughtExceptionHandler exceptionHandler) {
        return newScheduledThreadPool(count, new NamingThreadFactory(prefix, true, maxPriority, exceptionHandler));
    }

	/**
	 * Use when you dont want to limit anything, this impl will spin up to 500 threads as needs.
	 * It will reap idle threads after 60 seconds. When full, the rejection policy will run 
	 * unscheduleable threads in the calling thread so they dont block. 
	 * 
	 * If you want to limit concurrency dont use this pool, use the newFixedThreadPool
	 * 
	 */

    public static java.util.concurrent.ExecutorService newDynamicThreadPool(String category,String name) {
    	return newDynamicThreadPool(category, new NamingThreadFactory(name));
    }
    
    static Map<String,java.util.concurrent.ExecutorService> pools = new ConcurrentHashMap<String, java.util.concurrent.ExecutorService>();
	synchronized private static java.util.concurrent.ExecutorService newDynamicThreadPool(String category, final NamingThreadFactory threadFactory) {
		if (pools.containsKey(category) && !Boolean.getBoolean(TEST_MODE)) return pools.get(category);
		NamingThreadFactory myFactory = new NamingThreadFactory(category);

        if (category.equals("manager")) {
//            pools.put(category, Executors.newCachedThreadPool(new NamingThreadFactory("manager")));
            pools.put(category, new ThreadPoolExecutor(1, 100, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new NamingThreadFactory("manager")));
        } else {
            pools.put(category, java.util.concurrent.Executors.newFixedThreadPool(Integer.getInteger("pool.size", 25), myFactory));
        }
		return pools.get(category);
	}

	public static void setTestMode() {
		System.out.println("Setting TEST_MODE");
		pools.clear();
		System.setProperty(TEST_MODE, "true");
	}


    public static ThreadPoolExecutor newSizedThreadPool(int min, int max, NamingThreadFactory threadFactory) {

		return new ThreadPoolExecutor(min, max, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), threadFactory, new RejectedExecutionHandler() {
			ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
			@Override
			public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
				scheduler.execute(new Runnable() {
					@Override
					public void run() {
						executor.execute(r);
					}
				});


			}
		});
    }

	public static boolean isTestMode() {
		return Boolean.getBoolean(TEST_MODE);
	}
}
