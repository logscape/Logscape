/**
 * 
 */
package com.liquidlabs.common.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

/**
 * 
 * Auto-kill tasks started with execute(Runnable)
 * @author Neil
 */
public class CancellingThreadPool implements java.util.concurrent.ExecutorService {
	private static final Logger LOGGER = Logger.getLogger(CancellingThreadPool.class);
	
	private final ExecutorService delegate;
	private final ScheduledExecutorService scheduler;
	private final int killDelayMins;

	public CancellingThreadPool(ScheduledExecutorService scheduler, ExecutorService delegate, int killDelayMins) {
		this.scheduler = scheduler;
		this.delegate = delegate;
		this.killDelayMins = killDelayMins;
	}

	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return delegate.awaitTermination(timeout, unit);
	}

	/**
	 * Will auto-kill long running tasks
	 */
	public void execute(final Runnable command) {
		final String commandString = command.toString();
		final Future<?> task = delegate.submit(command);
		if (scheduler != null) scheduler.schedule(new Runnable() {
			@Override
			public void run() {
				if (!task.isDone()) {
					LOGGER.warn("Cancelling:" + commandString);
					boolean cancel = task.cancel(true);
					if (!cancel) LOGGER.warn("Could not cancel task:" + commandString);
				}
			}

		}, killDelayMins, TimeUnit.MINUTES);
	}

	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		return delegate.invokeAll(tasks, timeout, unit);
	}

	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return delegate.invokeAll(tasks);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return delegate.invokeAny(tasks, timeout, unit);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return delegate.invokeAny(tasks);
	}

	public boolean isShutdown() {
		return delegate.isShutdown();
	}

	public boolean isTerminated() {
		return delegate.isTerminated();
	}

	public void shutdown() {
		delegate.shutdown();
	}

	public List<Runnable> shutdownNow() {
		return delegate.shutdownNow();
	}

	public <T> Future<T> submit(Callable<T> task) {
		return delegate.submit(task);
	}

	public <T> Future<T> submit(Runnable task, T result) {
		return delegate.submit(task, result);
	}

	public Future<?> submit(Runnable task) {
		return delegate.submit(task);
	}
}