package com.liquidlabs.log.server;

import com.google.gson.Gson;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.WebSocket;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class TheWebSocket implements WebSocket.OnTextMessage {
    private volatile Connection connection;
    private static final Logger LOGGER = Logger.getLogger(TheWebSocket.class);
    private Indexer indexer;

    public TheWebSocket(Indexer indexer) {
        this.indexer = indexer;
    }

    @Override
    public void onMessage(String json) {
        String file = "file";
        int index = json.indexOf(file);
        int start = json.indexOf(":", index) + 2;
        int endOfPath = json.indexOf("\"", start);
        String path = json.substring(start, endOfPath);
        try {
            String absolutePath = new File(path).getAbsolutePath();
            LogFile logFile = indexer.openLogFile(absolutePath);
            if (logFile == null) {
                LOGGER.error("File not found: " + path);
                connection.sendMessage("File not found: " + path);
                return;
            }
            long lineNo = logFile.getLineCount() - 30;
            if (lineNo < 1) lineNo = 1;
            long seekPos = indexer.filePositionForLine(absolutePath, lineNo);

            File f = new File(path);
            RAF raf = RafFactory.getRafSingleLine(path);
            if (FileUtil.isCompressedFile(f.getName())) {
                seekPos = getCompressedSeekPos(path);
            }

            raf.seek(seekPos);
            String line;
            TailMessage init = new TailMessage("init");
            init.fromLineNumber = lineNo;
            while ((line = raf.readLine()) != null) {
                lineNo = processLine(lineNo, line, init);
            }
            Gson gson = new Gson();
            connection.sendMessage(gson.toJson(init));

            while (connection != null) {
                TailMessage message = new TailMessage("data");
                String tailed;
                int i = 0;
                // if more than X behind then skip
                if (f.length() - raf.getFilePointer() > 100 * 1024) {
                    raf.seek(f.length() - (50 * 1024));
                }
                while ((tailed = raf.readLine()) != null && i++ < 5) {
                    lineNo = processLine(lineNo, tailed, message);
                }
                if (message.hasData() && connection != null) {
                    connection.sendMessage(gson.toJson(message));
                } else {
                    Thread.sleep(1000L);
                }
                if (i == 0) Thread.sleep(2000L);
            }
        } catch (Exception e) {
            LOGGER.warn("Exception when tailing path:" + path, e);
        }
    }
    private long getCompressedSeekPos(String file) {
        file = FileUtil.getPath(new File(file));
            LogFile logFile = indexer.openLogFile(file);
            if (logFile != null) {
                int line =  logFile.getLineCount() > 10 ? logFile.getLineCount() - 30 : logFile.getLineCount();
                int fline = line > 10 ? line - 10 : 1;
                List<Line> lines = indexer.linesForNumbers(file, (int) fline, (int) line + 10);
                if (lines != null && lines.size() > 0) {
                    return lines.get(0).position();
                }
            }
        return new File(file).length();
    }

    private long processLine(long lineNo, String line, TailMessage init) throws IOException {
        StringWriter writer = new StringWriter();
        LogLineOutput output = new LogLineOutput(writer);
        output.writeHtmlLine(0, line, lineNo++);
        init.add(writer.toString());
        return lineNo;
    }

    @Override
    public void onOpen(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void onClose(int i, String s) {
        LOGGER.info("websocketclosed:"+s);
        this.connection = null;
    }
}
