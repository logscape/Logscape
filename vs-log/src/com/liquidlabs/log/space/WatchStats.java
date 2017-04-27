package com.liquidlabs.log.space;

import java.util.concurrent.atomic.AtomicLong;

import com.liquidlabs.log.index.LogFile;

public class WatchStats {
	
	public String watchId;
	
	public AtomicLong filesToday = new AtomicLong();
	public AtomicLong filesTotal = new AtomicLong();
	public AtomicLong dataIndexedToday = new AtomicLong();
	public AtomicLong dataIndexedTotal = new AtomicLong();
	public AtomicLong totalIndexedData = new AtomicLong();
	public AtomicLong linesToday = new AtomicLong();
	public AtomicLong linesTotal = new AtomicLong();
	public String hostname;

	private final WatchDirectory watch;
	
	public void roll() {
		linesToday.set(0);
		filesToday.set(0);
		dataIndexedToday.set(0);
	}
	
	public WatchStats(String id, WatchDirectory watch, String hostname) {
		this.watchId = id;
		this.watch = watch;
		this.hostname = hostname;
	}
	
	private long convertToMB(long size) {
		if (size == 0.0) return 0;
		return size/(1024 * 1024);
	}
	public String toString() {
		return String.format("WatchStats:%s host:%s filesToday:%s filesTotal:%s dataTodayMB:%d dataTotalMB:%d linesToday:%s linesTotal:%s", 	watch.toString(), hostname, 
									filesToday, filesTotal, 
									convertToMB(dataIndexedToday.get()), convertToMB(dataIndexedTotal.get()), 
									linesToday, linesTotal);
	}
	public int hashCode() {
		return watchId.hashCode();
	}
	public boolean equals(Object obj) {
		WatchStats other = (WatchStats) obj;
		return this.watchId.equals(other.watchId);
	}

	public void filesIncrement() {
		filesToday.incrementAndGet();
		filesTotal.incrementAndGet();
	}

	public void incrementLineAndData(int readBytes) {
		linesToday.incrementAndGet();
		linesTotal.incrementAndGet();
		dataIndexedToday.addAndGet(readBytes);
		dataIndexedTotal.addAndGet(readBytes);
	}

	public void update(LogFile logFile) {
		this.filesTotal.incrementAndGet();
		this.dataIndexedTotal.addAndGet(logFile.getPos());
		this.linesTotal.addAndGet(logFile.getLineCount());
		
		if (logFile.isToday()){
			this.filesToday.incrementAndGet();
			this.dataIndexedToday.addAndGet(logFile.getPos());
			this.linesToday.addAndGet(logFile.getLineCount());
		}
	}
}
