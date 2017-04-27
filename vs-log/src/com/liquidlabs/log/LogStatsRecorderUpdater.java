package com.liquidlabs.log;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liquidlabs.log.index.IndexStats;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.LogStats;
import com.liquidlabs.log.space.WatchDirectory;

public class LogStatsRecorderUpdater {
	
	static final Logger LOGGER = Logger.getLogger(LogStatsRecorderUpdater.class);

	private final LogSpace logSpace;

	private final String resourceId;

	private final Indexer indexer;

	private final AgentLogService agentLogService;

	private final String hostname;

	public LogStatsRecorderUpdater(ScheduledExecutorService scheduler, LogSpace logSpace, String resourceId, String hostname, Indexer indexer, AgentLogService agentLogService, Map<String, WatchDirectory> watchDirSet) {
		this.logSpace = logSpace;
		this.resourceId = resourceId;
		this.hostname = hostname;
		this.indexer = indexer;
		this.agentLogService = agentLogService;
		
		// use FixedDelay - with fixed interval the scheduler can cause a event storm when a machine comes out of sleep 
		scheduler.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				logStatsUpdate();
			}
		}, 3, LogProperties.getLogStatsUpdateIntervalMins(), TimeUnit.MINUTES);
	}
	
	public void logStatsUpdate() {
		
		try {
			// fires every 'n'  minutes 
			IndexStats lastStats = this.indexer.getLastStats();
			if (lastStats == null) return;
			LogStats logStats = new LogStats(lastStats);
			logStats.hostname = hostname;
			logStats.agentId = resourceId;
			logStats.liveFiles = agentLogService.tailerCount();
			
			logSpace.writeLogStat(logStats);
			LOGGER.info(logStats.toString());
		} catch (Throwable t) {
			if (t.getMessage().contains("Network is unreachable")) return;
			LOGGER.warn("Failed to update Stats", t);
		}
	}	
}
