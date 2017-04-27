package com.liquidlabs.log;

import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.log.streaming.LiveHandler;
import com.logscape.disco.indexer.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface LogReader {
    int currentLine();

    public static interface Handler {
        void startChunk();
        void handle(LogFile logFile, String line, int lineLength, int lineNumber, long time, long linePosition, int multiLineEventCount, Map<String, String> fields, List<Pair> discovered, Map<String, String> groupFields);
        void flush();

        void roll(String from, String to);
        void roll(String rolledFrom, String rolledTo, long currentPos, int line, String sourceURI, String hostname, WatchDirectory watch);

        boolean allowClockRewind();

        void deleted(String filename);

        boolean isDiscoveryEnabled();
    }
    WriterResult readNext(long startPos, int startingLine) throws IOException;
	
	Indexer getIndexer();

	void setFilters(String[] includes, String[] excludes);


	void roll(String from, String to, long currentPos, int line);

    void roll(String from, String to);

    long getLastTimeExtracted();

	long getTime(String filename, int lineNumber, String nextLine, long fileStartTime, long fileLastMod, long filePos, long fileLength);

	void deleted(String filename);

	void addLiveHandler(LiveHandler liveHandler);

	void setLogId(LogFile logFile);

	void stopTailing();

	void interrupt();

    void setLogFiletype(String fieldSetId);
}
