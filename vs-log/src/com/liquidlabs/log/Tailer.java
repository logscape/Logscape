package com.liquidlabs.log;

import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.streaming.LiveHandler;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;

public interface Tailer extends Callable<TailResult> {

	void setFilters(String[] includes, String[] excludes);

	void future(Future<TailResult> schedule);

	ReadWriteLock getLock();

	String filename();

	boolean terminateIfMatches(WatchDirectory watchItem);

	boolean isFor(String file);

	int getLine();

	WatchDirectory getWatch();

	LogReader getWriter();

	void addLiveHandler(LiveHandler liveHandler);

    /**
     * Check to determine is file is potential roll target. For perf reasons this is only called when the file to check is < 1 minute old - otherwise dont bother
     * @param file
     * @param tag
	 * @return
     * @throws InterruptedException
     */
	boolean isRollCandidate(String file, String tag) throws InterruptedException;

	void interrupt();

	String fileTag();

	String fieldSetId();

	void setWatch(WatchDirectory newWatchDirectory);

	long lastMod();

    boolean matchesPath(String fileAndPath);

    Event fillIn(Event event);

    void stop();
}
