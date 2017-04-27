package com.liquidlabs.common.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PriorityThreadPoolExecutor extends ThreadPoolExecutor {

	static int maxPriority = Integer.getInteger("pool.max.priority", 100);
	public PriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, int queueSize, TimeUnit unit, NamingThreadFactory namingThreadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<Runnable>(queueSize), namingThreadFactory);
	}
	
	protected <T extends Object> java.util.concurrent.RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		if(runnable instanceof PrioritisedRunnable){
			int priority = ((PrioritisedRunnable) runnable).getPriority();
			if (priority > maxPriority) priority = maxPriority;
			return new PriorityTask(priority, runnable, value);
		}
		return new PriorityTask(10, runnable, value);
		
	}
	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
		if(callable instanceof PrioritisedCallable){
			int priority = ((PrioritisedCallable) callable).getPriority();
			if (priority > maxPriority) priority = maxPriority;
			return new PriorityTask(priority, callable);
		}
		return new PriorityTask(10, callable);
	}
	

	public static interface PrioritisedCallable extends Callable {
		int getPriority();
	}
	
	public static interface PrioritisedRunnable extends Runnable {
		int getPriority();
	}
	
	private static class PriorityTask<T> extends FutureTask implements Comparable<PriorityTask<T>> {

		private final int priority;

		public PriorityTask(int priority, Runnable runnable, Object result) {
			super(runnable, result);
			this.priority = priority;
		}

		public PriorityTask(int priority, Callable<T> callable) {
			super(callable);
			this.priority = priority;
		}

		@Override
		public int compareTo(PriorityTask<T> o) {
			return o.priority - priority;
		}
		
	}
}
