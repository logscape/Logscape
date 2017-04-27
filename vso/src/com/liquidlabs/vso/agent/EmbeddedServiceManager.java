package com.liquidlabs.vso.agent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.LifeCycle;

@SuppressWarnings("unchecked")
public class EmbeddedServiceManager {
	final static Logger LOGGER = Logger.getLogger(EmbeddedServiceManager.class);
	private List<LifeCycle> lifeCycles = new ArrayList<LifeCycle>();
	private Future future;
	String key = new Date().toString();
	
	public EmbeddedServiceManager(String id) {
		this.key = new Date().toString() + " " + id;
	}

	public void registerLifeCycleObject(LifeCycle lifeCycle) {
		if (lifeCycle == null) {
			LOGGER.error("ESM: registerLifeCycleObject was called with  NULL Object:" + key);
		}
		this.lifeCycles.add(lifeCycle);
	}
	
	public void registerFuture(Future future) {
		this.future = future;
	}
	public boolean isRunning() {
		return !(future == null || future.isCancelled() || future.isDone());
	}
	
	public void stop() {
		// unregister them in the reverse order they were added
		Collections.reverse(lifeCycles);
		for (LifeCycle lifeCycle : this.lifeCycles) {
			try {
				LOGGER.info("Stopping Object:" + key + ":" + lifeCycle);
				lifeCycle.stop();
			} catch (Throwable t) {
				LOGGER.warn("LifeCycle.stop error id:" + this.key, t);
			}
		}
		if (future != null) {
			try {
				LOGGER.info("Stopping Future:" + key + ":" + future);
				future.cancel(true);
			} catch (Throwable t) {
				LOGGER.warn("Future.cancel error:" + this.key, t);
			}
		}
	}
	public String toString() {
        String futureString = future != null ? " Future:" + future + " canc:" + future.isCancelled() + " done:" + future.isDone()  : " NullFuture";
		return super.toString() + ":" + this.key + " lifeCycle:" + lifeCycles + futureString ;
	}

	private Throwable t;
	public String getErrorMsg() {
		try {
			if (t != null) throw t;
			if (future == null) return "NULL Task";
			Object object = future.get(14, TimeUnit.SECONDS);
			if (object == null) return "VOID";
			return object.toString();
		} catch (Throwable t) {
			if (this.t != null) this.t = t;
			LOGGER.warn("Task Failed", t);
			future = null;
			if (t == null) return "Null Throwable";
			return ExceptionUtil.stringFromStack(t, 4048);
		}
	}

}
