package com.liquidlabs.logserver;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.WatchDirectory;
import com.logscape.disco.indexer.Pair;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 03/07/2015
 * Time: 11:44
 * To change this template use File | Settings | File Templates.
 */
public class ForwarderHandler implements LogReader.Handler{
    private final static Logger LOGGER = Logger.getLogger(ForwarderHandler.class);
    private LogServer logServer;
    private Indexer indexer;
    private String hostname = null;
    private LogMessage logMessage;
    private long lastLinePos;

    public ForwarderHandler(LogServer logServer, Indexer indexer) {
        this.logServer = logServer;
        this.indexer = indexer;
    }
    public void startChunk() {



    }

    @Override
    public void handle(LogFile logFile, String line, int lineLength, int lineNumber, long time, long linePosition, int multiLineEventCount, Map<String, String> fields, List<Pair> discovered, Map<String, String> groupFields) {

        if (hostname == null) hostname = logFile.getFileHost(this.hostname = NetworkUtils.getHostname());

        try {
            logServer.isAvailable(hostname+logFile.getFileName());
        } catch (Throwable t) {
            LOGGER.warn("IndexStore Missing:" + logServer);
            waiting();
            //return new WriterResult(null, (int)linePosition, 0);
        }

        if (logMessage == null) {
            logMessage = new LogMessage(hostname, logFile.getFileName(), time, lineNumber, new File(logFile.getFileName()).length());
        }

        logMessage.addMessage(time, line, linePosition);
        lastLinePos = linePosition;
        Line line1 = new Line(logFile.getId(), lineNumber, time, linePosition);
        ArrayList<Line> lines = new ArrayList<Line>();
        lines.add(line1);
        indexer.add(logFile.getFileName(), lines);

        // optional flush
        logMessage.flush(logServer, false, linePosition);

    }
    private void waiting() {
        try {
            LOGGER.info("Waiting for LogServer");
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
        }
    }

    public void flush() {
        if (logMessage != null) {
            logMessage.flush(logServer, true, lastLinePos);
        }
        logMessage = null;
    }

    @Override
    public void roll(String from, String to) {
        if (hostname != null) {
            LOGGER.info("Forcing LogServerRoll:" + from + " to " + to);
            logServer.roll(hostname+from, hostname, from, to);
        }

    }

    @Override
    public void deleted(String filename) {
        if (hostname != null) {
            LOGGER.info("Forcing LogServerDelete:" + filename);
            logServer.deleted(hostname+filename, hostname, filename);
        }
    }

    @Override
    public boolean isDiscoveryEnabled() {
        return false;
    }

    @Override
    public void roll(String from, String to, long currentPos, int line, String sourceURI, String hostname, WatchDirectory watch) {
        if (to == null) return;
        LOGGER.info("Roll:" + from + " to " + to);
        logServer.roll(hostname+from, hostname, from, to);
    }

    @Override
    public boolean allowClockRewind() {
        return true;
    }

}
