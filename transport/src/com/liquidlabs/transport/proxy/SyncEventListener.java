package com.liquidlabs.transport.proxy;

import java.lang.reflect.Method;
import com.liquidlabs.common.net.URI;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;


/**
 * Received incoming message, passes back the value and allows itself to be unregistered
 */
public class SyncEventListener {
	private static final Logger LOGGER = Logger.getLogger(SyncEventListener.class);

	private static final String _NULL_ = "_NULL_";
	private static final String _TIMEOUT_ = "_TIMEOUT_";
	private final String id;
	private final Method method;
	private final LinkedBlockingQueue<Object> waitingQueue = new LinkedBlockingQueue<Object>(10);
	private final int invocationTimeoutSeconds;

	private final URI endPoint;

	private final ScheduledExecutorService scheduler;

	private long registeredTimeMs;
	
	public SyncEventListener(String id, Method method, int invocationTimeoutSeconds, URI endPoint, ScheduledExecutorService scheduler) {
		this.id = id;
		this.method = method;
		this.invocationTimeoutSeconds = invocationTimeoutSeconds;
		this.endPoint = endPoint;
		this.scheduler = scheduler;
		this.registeredTimeMs = DateTimeUtils.currentTimeMillis();
	}
	
	/**
	 * somehow the future tasks can get by leaving the thread hanging for every - seen in fail-over and heavily loaded OS out of mem situations
	 * 
	 * so we double the value - to try and prevent any confusion where threads are slightly delayed
	 * @return 
	 */
	public boolean abortIfTooOld() {
		if (this.registeredTimeMs < DateTimeUtils.currentTimeMillis() - ((2 * invocationTimeoutSeconds) * 1000)) {
			scheduleNullTimeoutResult(1);
			LOGGER.info("Scheduling SyncEventListener. Aborting - NullTimeout for:" + id);
			return true;
		}
		return false;
		
	}
	public String getId() {
		return id;
	}
	public boolean shouldRemove() {
		return true;
	}

	/**
	 * Message Received
	 */
	public void setResult(Object value) {
		if (value == null) waitingQueue.add(_NULL_);
		else waitingQueue.add(value);		
	}
	public Method getMethod() {
		return method;
	}
	public LinkedBlockingQueue<Object> getWaitingQueue() {
		return waitingQueue;
	}
	public Object get() throws RetryInvocationException, InterruptedException {
		ScheduledFuture<?> future = scheduleNullTimeoutResult(this.invocationTimeoutSeconds);
		try {
			Object result = poll(waitingQueue, invocationTimeoutSeconds);
			if (result != null) {
				future.cancel(true);
				if (result instanceof Throwable) {
					LOGGER.warn(id + ":" + method + " Passing back RemoteException:" + ((Throwable)result).getMessage());
					throw new RuntimeException((Throwable)result);
				}
				
				if (result.equals(_TIMEOUT_)) {
					throw new RetryInvocationException("Timing out on clientId:" + id + " against ep:" + this.endPoint + method.toString());
				}
				if (result.equals(_NULL_)) {
					return null;
				}
				return result;
			}
			else {
				throw new RetryInvocationException("Timing out on clientId:" + id + " against ep:" + this.endPoint + method.toString());
			}
		} catch (InterruptedException e) {
			throw e;
		}
	}
	private ScheduledFuture<?> scheduleNullTimeoutResult(int timeoutSeconds) {
		ScheduledFuture<?> schedule = scheduler.schedule(new Runnable() {
			public void run() {
				try {
					waitingQueue.put(_TIMEOUT_);
				} catch (InterruptedException e) {
				}
			}
			
		}, timeoutSeconds, TimeUnit.SECONDS);
		return schedule;
	}
	
	/**
	 * Hack to get around bug - http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6460501
	 */
	private Object poll(LinkedBlockingQueue<Object> waitingQueue2, int timeoutPeriod) throws InterruptedException {
		return waitingQueue2.take();
	}
}
