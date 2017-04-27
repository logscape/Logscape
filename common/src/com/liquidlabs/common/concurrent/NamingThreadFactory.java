package com.liquidlabs.common.concurrent;

import org.apache.log4j.Logger;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamingThreadFactory implements ThreadFactory {
	static final AtomicInteger poolNumber = new AtomicInteger(1);
	final ThreadGroup group;
	final AtomicInteger threadNumber = new AtomicInteger(1);
	String namePrefix;
	private final boolean isDaemon;
	private final int priority;
	private Logger LOGGER;

    private UncaughtExceptionHandler exceptionHandler = new UncaughtExceptionHandler() {
        public void uncaughtException(Thread thread, Throwable throwable) {
            LOGGER.error(String.format("UNCAUGHT in thread[%s] ex:%s", thread.getName(), throwable.getMessage(), throwable), throwable);

        }
    };



    public NamingThreadFactory(String prefix) {
		this(prefix, true, Thread.NORM_PRIORITY);
	}
    public String toString() {
    	return getClass().getSimpleName() + " Prefix:" + namePrefix;
    }

    public NamingThreadFactory(String prefix, boolean isDaemon, int priority) {
		LOGGER = Logger.getLogger(NamingThreadFactory.class.getName() + prefix);
		this.isDaemon = isDaemon;
		this.priority = priority;
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
		namePrefix = setPrefix(prefix);
	}

    public NamingThreadFactory(String prefix, boolean isDaemon, int priority, UncaughtExceptionHandler exceptionHandler) {
        this(prefix, isDaemon, priority);
        this.exceptionHandler = exceptionHandler;
    }

    public NamingThreadFactory(String name, UncaughtExceptionHandler exceptionHandler) {
        this(name, false, Thread.NORM_PRIORITY, exceptionHandler);
    }


    public Thread newThread(Runnable r) {
		Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
		t.setUncaughtExceptionHandler(exceptionHandler);
		if (!t.isDaemon() && isDaemon)
			t.setDaemon(true);
		if (t.getPriority() != priority)
			t.setPriority(priority);
		return t;
	}

	public String setPrefix(String prefix) {
		return prefix + "-" + poolNumber.get() + "-";
	}

	public void setNamePrefix(String namePrefix) {
		this.namePrefix = namePrefix;
	}

	public String getNamePrefix() {
		return namePrefix;
	}

}
