package com.liquidlabs.log.reader;

import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.WatchDirectory;
import com.logscape.disco.indexer.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/06/2014
 * Time: 14:18
 * To change this template use File | Settings | File Templates.
 */
public class LocalIndexerIngestHandler implements LogReader.Handler {
    private final static Logger LOGGER = Logger.getLogger(LocalIndexerIngestHandler.class);
    protected List<Line> currentLines = new ArrayList<Line>();
    private Indexer indexer;
    private LogFile logFile;
    IndexFeed discovery = KvIndexFactory.get();

    public LocalIndexerIngestHandler(Indexer indexer) {

        this.indexer = indexer;
    }

    @Override
    public void startChunk() {
    }

    @Override
    public void handle(LogFile logFile, String line, int lineLength, int lineNumber, long time, long linePosition, int multiLineEventCount, Map<String, String> allFields, List<Pair> discoveredFields, Map<String, String> groupFields) {

        // need to store our disco fields for seach time
        discovery.store(logFile.getId(), lineNumber, discoveredFields);

        this.logFile = logFile;
        Line line1 = new Line(logFile.getId(), lineNumber, time, linePosition);
        currentLines.add(line1);
        fillExtraLines(multiLineEventCount);

        if (currentLines.size() > 1000) {
            indexer.add(logFile.getFileName(), currentLines);
            currentLines = new ArrayList<Line>();
        }
    }
    private void fillExtraLines(int linesReadInLineEvent) {
        if (linesReadInLineEvent > 1) {
            Line line = this.currentLines.get(this.currentLines.size()-1);
            for (int i = 0; i < linesReadInLineEvent-1; i++) {
                this.currentLines.add(new Line(line.pk.logId,line.number()+i+1, line.time(), line.filePos));
            }
        }
    }


    @Override
    public void flush() {
        if (currentLines.size() > 0) {
            indexer.add(logFile.getFileName(), currentLines);
            currentLines.clear();
        }
        discovery.commit();
    }

    @Override
    public void roll(String rolledFrom, String rolledTo, long currentPos, int line, String sourceURI, String hostname, WatchDirectory watch) {
        // if we know it rolled to another file try and read the rest...
        if (rolledTo != null) {
            LogFile logFile = indexer.openLogFile(rolledTo);

            GenericIngester ingester = new GenericIngester(logFile, indexer, this, sourceURI, hostname, watch);
            try {
                LOGGER.debug("Read remaining Contents for ROLL: line:" + line + " pos:" + currentPos);
                ingester.readNext(currentPos, line);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (logFile != null) {
                logFile.setAppendable(false);
                indexer.updateLogFile(logFile);
            } else {
                LOGGER.warn("LogFile was Deleted:" + rolledFrom);
            }

        }
    }

    @Override
    public boolean allowClockRewind() {
        return false;
    }

    @Override
    public void deleted(String filename) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isDiscoveryEnabled() {
        return true;
    }

    @Override
    public void roll(String from, String to) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
