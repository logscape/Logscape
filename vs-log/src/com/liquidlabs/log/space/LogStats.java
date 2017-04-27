package com.liquidlabs.log.space;

import com.liquidlabs.log.index.IndexStats;
import com.liquidlabs.orm.Id;

import java.text.DecimalFormat;

public class LogStats {
	
	static DecimalFormat formatter = new DecimalFormat("#.##");
	
	enum SCHEMA { agentId, agentCount,liveFiles, indexedToday,indexedYesterday,indexedTotal,indexedFileCount, hostname, forwarderCount, logServerCount,socketServerCount, syslogServerCount,currentLiveFilesString,dataIndexedTodayString,dataIndexedTotalString,dataIndexedYesterdayString,indexedFileCountString};
	
	@Id
	public String agentId;
	
	public int agentCount;
	public int forwarderCount;
	public int liveFiles;
	public long indexedToday;
	public long  indexedYesterday;
	public long indexedTotal;
	public int indexedFileCount;
	public String hostname;

	public int logServerCount;
	public int socketServerCount;

	public int sysLogServerCount;

	public String currentLiveFilesString;

	public String indexedFileCountString;

	public String dataIndexedTodayString;

	public String dataIndexedYesterdayString;

	public String dataIndexedTotalString;
	
	public LogStats() {
	}
	public LogStats(IndexStats indexStats) {
		indexedFileCount = indexStats.totalFiles();
		indexedToday = indexStats.indexedToday();
		indexedTotal = indexStats.indexedTotal();
	}

	public void add(LogStats logStats2) {
		agentCount++;
		this.indexedFileCount += logStats2.indexedFileCount;
		this.liveFiles += logStats2.liveFiles;
		this.indexedTotal += logStats2.indexedTotal;
		this.indexedToday += logStats2.indexedToday;
		this.indexedYesterday += logStats2.indexedYesterday;
	}

	public double convertToMb(long size) {
		if (size == 0.0) return 0.0;
		return (double)size/(1024.0 * 1024.0);
	}

	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("LogStats:");
		buffer.append(" agentId:");
		buffer.append(agentId);
		buffer.append(" host:");
		buffer.append(hostname);
		buffer.append(" indexedFileCount:");
		buffer.append(indexedFileCount);
		buffer.append(" currentLiveFiles:");
		buffer.append(liveFiles);
		buffer.append(" indexedBytes:");
		buffer.append(indexedTotal);
		buffer.append(" indexTodayBytes:");
		buffer.append(indexedToday);
		buffer.append(" indexedYesterdayBytes:");
		buffer.append(indexedYesterday);
		return buffer.toString();
	}
	
}
