package com.liquidlabs.log.reader;

import java.util.concurrent.atomic.AtomicLong;

import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.LogFilters;
import com.liquidlabs.log.space.WatchDirectory;

public interface LogReaderFactory {

	LogReader getReader(LogFile logFile, Indexer indexer, WatchDirectory watch, LogFilters filters, AtomicLong amountIndexedToday, String fieldSetId);

}
