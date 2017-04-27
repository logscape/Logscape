package com.liquidlabs.logserver;

import java.util.concurrent.atomic.AtomicLong;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.LogReaderFactory;
import com.liquidlabs.log.space.LogFilters;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import org.apache.log4j.Logger;

public class LogReaderFactoryForForwarders implements LogReaderFactory {
    private final static Logger LOGGER = Logger.getLogger(LogReaderFactory.class);
	
	private final LogServer logServer;
    private final ProxyFactory proxyFactory;
    private Indexer indexer;

    public LogReaderFactoryForForwarders(final LogServer logServer, ProxyFactory proxyFactory, Indexer indexer) {
		this.logServer = logServer;
        this.proxyFactory = proxyFactory;
        this.indexer = indexer;
        this.indexer.addFileDeletedListener(new LogFileOps.FileDeletedListener() {
            @Override
            public void deleted(String filename) {
                LOGGER.info("DELETING FROM SERVER: " +filename);
                String hostname = NetworkUtils.getHostname();
                logServer.deleted(hostname+filename, hostname, filename);

            }
        });
    }

	public LogReader getReader(LogFile logFile, Indexer indexer, WatchDirectory watch, LogFilters filters, AtomicLong amountIndexedToday, String fieldSet) {
        LogReader.Handler client = new ForwarderHandler(logServer, indexer);
        return new GenericIngester(logFile, indexer, client, proxyFactory.getEndPoint(), NetworkUtils.getHostname(), watch);

    }

}
