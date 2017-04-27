package com.liquidlabs.log;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.streamer.FileStreamer;
import com.liquidlabs.log.streamer.LogFileStreamer;
import com.liquidlabs.log.streamer.LogLine;

public class FileStreamerParent implements FileStreamer {
	private final static Logger LOGGER = Logger.getLogger(FileStreamerParent.class);
	private final Indexer indexer;
	private final LogFileStreamer metaFileStreamer;
	static DateTimeFormatter formatter = DateUtil.shortDateTimeFormat2;

	public FileStreamerParent(Indexer indexer, LogFileStreamer metaFileStreamer) {
		this.indexer = indexer;
		this.metaFileStreamer = metaFileStreamer;
	}

	public List<LogLine> getFilePage(String filename, long time, int pageSize) {
		LogFile openLogFile = indexer.openLogFile(filename);
		if (openLogFile == null) {
			LOGGER.warn("getFilePage[0] Couldnt Locate File:" + filename);
			return new ArrayList<LogLine>();
		}
		
		List<LogLine> filePage = metaFileStreamer.getFilePage(filename, time, pageSize);
		if (filename.length() > 0) filePage.get(0).lines = openLogFile.getLineCount();
		return filePage;
	}

	public List<LogLine> getFilePage(String filename, int startLine, int pageSize) {
		LogFile openLogFile = indexer.openLogFile(filename);
		if (openLogFile == null) {
			LOGGER.warn("getFilePage[1] Couldnt Locate File:" + filename);
			return new ArrayList<LogLine>();
		}
		// dont fall off the end
		if (startLine > openLogFile.getLineCount()) {
			startLine = openLogFile.getLineCount() - (pageSize + 1);
		}
		List<LogLine> filePage = metaFileStreamer.getFilePage(filename, startLine, pageSize);
		if (filename.length() > 0) filePage.get(0).lines = openLogFile.getLineCount();
		
		if (openLogFile.getLineCount() > 10) {
			if (filePage.size() == 1) {
				LOGGER.info("Oooops - trying to wind back the position:" + startLine);
				return getFilePage(filename, startLine - pageSize, pageSize);
			}
		}
		return filePage;

	}

	public List<LogLine> getFileTail(String filename, int existingStartLine, int pageSize) {
		LogFile openLogFile = indexer.openLogFile(filename);
		if (openLogFile == null) {
			LOGGER.warn("Tail: Couldnt Locate File:" + filename);
			return new ArrayList<LogLine>();
		}
		List<LogLine> filePage = metaFileStreamer.getFileTail(filename, existingStartLine, pageSize);
		
		// return an error when the file isnt found!!
		if (filePage.size() == 0) {
			filePage.add(new LogLine(System.currentTimeMillis(), new DateTime().toString(), 0, "FILE NOT FOUND ERROR:" + filename));
			return filePage;
		}
		
		LogLine lastLogLine = filePage.get(filePage.size()-1);
		if (!lastLogLine.text.contains("CURRENT END OF FILE")) {
			filePage.add(metaFileStreamer.getLastlineOfFile(lastLogLine.number+1));
		}
		
		// now make sure we see the tail of the file
		if (filePage.size() > pageSize-2) {
			filePage = new ArrayList<LogLine>(filePage.subList(2, filePage.size()));
		}

		if (filePage.size() > 0) filePage.get(0).lines = openLogFile.getLineCount();
		return filePage;
	}


    @Override
    public List<LogLine> getFilePage(String fileName, int line, int pageSize, String searchForThis, boolean searchForwards) {
        LogFile openLogFile = indexer.openLogFile(fileName);
        if (openLogFile == null) {
            LOGGER.warn("getFilePage[0] Couldnt Locate File:" + fileName);
            return new ArrayList<LogLine>();
        }

		List<LogLine> filePage = metaFileStreamer.getFilePage(fileName, line, pageSize, searchForThis,searchForwards);
		if (filePage.size() > 0) filePage.get(0).lines = openLogFile.getLineCount();
		return filePage;

    }

    public long getUpdateTimeStamp(String filename) {
		LogFile openLogFile = indexer.openLogFile(filename);
		if (openLogFile == null) {
			LOGGER.warn("TimeStamp: Couldnt Locate File:" + filename);
			return 0;
		}
		return metaFileStreamer.getUpdateTimeStamp(filename);
	}

}
