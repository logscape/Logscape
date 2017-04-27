package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.ThreadUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import org.apache.log4j.Logger;

import com.liquidlabs.log.space.LogConfigListener;
import com.liquidlabs.log.space.LogFilters;
import com.liquidlabs.log.space.WatchDirectory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class LogConfigurationHandler implements LogConfigListener {

	static final Logger LOGGER = Logger.getLogger(LogConfigurationHandler.class);

	transient private AgentLogServiceImpl logService;
	private String id;
    private EventMonitor eventMonitor;

	private WatchManager watchManager;

	public LogConfigurationHandler() {}

	public LogConfigurationHandler(AgentLogServiceImpl logService, String resourceId, WatchManager watchManager, EventMonitor eventMonitor) {
		this.logService = logService;
		this.watchManager = watchManager;
        this.eventMonitor = eventMonitor;
        id = getClass().getSimpleName() + resourceId;
	}
	
	public String getId() {
		return id;
	}

	public void setFilters(LogFilters filters) {
		logService.setFilters(filters);
	}
    final ReentrantLock activityLock = new ReentrantLock();

	public  void addWatch(final WatchDirectory watchItem) {
        tryLock(new Runnable() {
            public void run() {
                eventMonitor.raise(watchItem.fillIn(new Event("AGENT_ADD_WATCH")));
                watchManager.addWatch(watchItem);
            }});
	}



    public void removeWatch(final WatchDirectory watchItem) {
        tryLock(new Runnable() {
            public void run() {
                eventMonitor.raise(watchItem.fillIn(new Event("AGENT_REM_WATCH")));
                watchManager.removeWatch(watchItem);
            }
        });
    }

	public void updateWatch(final WatchDirectory watchItem) {
        tryLock(new Runnable() {
            public void run() {

                eventMonitor.raise(watchItem.fillIn(new Event("AGENT_UPD_WATCH")));
        		watchManager.updateWatch(watchItem, true);
            }});
    }
    int dumped;
    long lastDump = 0;
    private void tryLock(Runnable task) {
        try {
                if (!activityLock.tryLock(30, TimeUnit.SECONDS)) {
                    synchronized (this) {
                        if (lastDump > System.currentTimeMillis() - DateUtil.MINUTE) {
                            lastDump = System.currentTimeMillis();
                            LOGGER.warn("Failed To Get Indexing Lock, dumped:" + dumped);
                            if (dumped++ < 10) {
                                LOGGER.warn(ThreadUtil.threadDump("",""));
                            }

                        }
                }

            }
            // run anyways!
            task.run();

        } catch (InterruptedException e) {
        } finally {
            if (activityLock.isHeldByCurrentThread()) activityLock.unlock();
        }
    }
	
}
