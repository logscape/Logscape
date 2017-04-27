package com.liquidlabs.log.streamer;

import java.util.List;

public interface FileStreamer {

	long getUpdateTimeStamp(String filename);
	List<LogLine> getFilePage(String fileName, long time, int pageSize);
	List<LogLine> getFilePage(String filename, int startLine, int pageSize);
	List<LogLine> getFileTail(String fileName, int existingStartLine, int pageSize);

    List<LogLine> getFilePage(String fileName, int line, int pageSize, String searchForThis, boolean searchForwards);
}
