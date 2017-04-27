package com.liquidlabs.log.reader;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogFilters;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.transport.proxy.ProxyFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LogReaderFactoryForIngester implements LogReaderFactory {
	
	private String hostname;
	private AggSpace aggSpace;
	private final ProxyFactory proxyFactory;

	public LogReaderFactoryForIngester(AggSpace aggSpace, ProxyFactory proxyFactory) {
		this.proxyFactory = proxyFactory;
		this.hostname = NetworkUtils.getHostname();
		this.aggSpace = aggSpace;
	}
	
	public LogReader getReader(LogFile logFile, Indexer indexer, WatchDirectory watch, LogFilters filters, AtomicLong amountIndexedToday, String fieldSetId) {
        LocalIndexerIngestHandler client = new LocalIndexerIngestHandler(indexer);
        return new GenericIngester(logFile, indexer, client, proxyFactory.getEndPoint(), hostname, watch);

	}
	
	

}
