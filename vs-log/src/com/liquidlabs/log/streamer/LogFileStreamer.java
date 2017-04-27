package com.liquidlabs.log.streamer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.util.DateTimeExtractor;

/**
 * Handles remote requests to either stream file segments or provide status
 * about file-update timestamps
 *
 */
public class LogFileStreamer implements FileStreamer {
	private static final Logger LOGGER = Logger.getLogger(LogFileStreamer.class);
	private Indexer indexer;
	DateTimeFormatter formatter = DateUtil.shortDateTimeFormat2;
	
	public LogFileStreamer() {
	}
	public LogFileStreamer(Indexer indexer){
		this.indexer = indexer;
	}
	public long getUpdateTimeStamp(String filename) {
		File file = new File(filename);
		if (!file.exists()) return -1;
		return file.lastModified();
	}
	public List<LogLine> getFilePage(String file, long	time, int pageSize) {
		LOGGER.info(String.format(">> Streaming [%s] startTime[%s], page[%d]", file, DateTimeFormat.mediumDateTime().print(time), pageSize));
		List<Line> lines = indexer.linesForTime(file, time, pageSize);
		if (lines.size() > 0) {
			int lineZero = lines.get(0).number();
			int startLine = lineZero > 10 ? lineZero - 10 : 0;
			return getFilePage(file,startLine, pageSize);
		} else {
			return new ArrayList<LogLine>();
		}
	}
	public List<LogLine> getFilePage(String file, int startLine, int pageSize) {
		try {
			if (startLine < 1) startLine = 1;
			LOGGER.info(String.format(">> Streaming [%s] startLine[%d], page[%d]", file, startLine, pageSize));
			
			List<Line> lines = indexer.linesForNumbers(file, startLine, startLine + pageSize);
			return getFilePageNow(file, pageSize, startLine, lines);
			
		} catch (Throwable t){
			LOGGER.warn(t.getMessage(), t);
			return new ArrayList<LogLine>();
		}
	}

    @Override
    public List<LogLine> getFilePage(String fileName, int line, int pageSize, String searchForThis, boolean searchForwards) {
    	LOGGER.info("FIND - Finding NEXT page for search string:" + searchForThis + ":" + line);
    	if (searchForThis == null || searchForThis.length() == 0) {
    		List<Line> lines = indexer.linesForNumbers(fileName, line, line + pageSize);
    		return getFilePageNow(fileName, pageSize, line, lines);
    	}
    	if (searchForwards){
	        final List<Line> lines = indexer.linesForNumbers(fileName, line, line + pageSize);
	        Line first = lines.size() > 0 ?  lines.get(0) : new Line(0, 1, 1, 0);
			int matchingLine = searchForStringIn(fileName, makeArray(searchForThis), first, line);
	        int startLine =  (matchingLine/pageSize) * pageSize;
	
	        return getFilePageNow(fileName, pageSize, startLine, indexer.linesForNumbers(fileName, startLine, pageSize));
    	} else {
	        LOGGER.info("FIND - Finding PREV page for search string:" + searchForThis + ":" + line);

    		int matchingLine = searchForLastStringBefore(makeArray(searchForThis), fileName, line);
    		if (matchingLine == -1 || matchingLine < pageSize) matchingLine = 1;
	        int startLine =  (matchingLine/pageSize) * pageSize;
	        return getFilePageNow(fileName, pageSize, startLine, indexer.linesForNumbers(fileName, startLine, pageSize));
    	}
    }
	private String[] makeArray(String matchingThis) {
		String[] items = matchingThis.split(",");
		ArrayList<String> itemsss = new ArrayList<String>();
		for (String item : items) {
			itemsss.add(item.trim());
		}
		return itemsss.toArray(new String[0]);
	}
	public List<LogLine> getFileTail(String fileName, int existingStartLine, int pageSize) {
		try {
			if (existingStartLine < 1) existingStartLine = 1;
			LOGGER.info(String.format(">> StreamingTail [%s] startLine[%d], page[%d]", fileName, existingStartLine, pageSize));
            if (!indexer.isIndexed(fileName))   return new ArrayList<LogLine>();
			
			LogFile file = indexer.openLogFile(fileName);

			// we want to map a fairly wide batch of lines so we handle newline events properly
			List<Line> lines = indexer.linesForNumbers(fileName, existingStartLine - 1000, existingStartLine + pageSize);
			
			int endPage = file.getLineCount()/pageSize;
			int startLineFromFile = endPage * pageSize;
			if (startLineFromFile <= 0) startLineFromFile = 1;
			
			List<LogLine> filePageNow = getFilePageNow(fileName, pageSize, startLineFromFile, lines);
			if (!filePageNow.isEmpty()) LOGGER.info(String.format("<< StreamingTail [%s] startLine[%d], page[%d]", fileName, filePageNow.get(0).number,  filePageNow.size()));
			return filePageNow;
		} catch (Throwable t){
			LOGGER.warn("SteamingTail Failed:" + t.getMessage(), t);
			return new ArrayList<LogLine>();
		}
	}

    private int searchForLastStringBefore(String[] searchForThis, String filename, int lineMax) {
    	int result = 0;
        if (!new File(filename).exists()) {
            return -1;
        }
        RAF raf = null;
        try {
            raf = RafFactory.getRafSingleLine(filename);
            String line = null;
            int lineNum = 1;
            while (lineNum < lineMax) {
            	line = raf.readLine();
            	for (String string : searchForThis) {
            		if (line.contains(string)) {
            			result = lineNum;
            		}
					
				}
            	lineNum++;
            }
            return result;
        } catch (IOException e) {
            
        } finally {
            try {
                raf.close();
            } catch(Throwable t){}
        }
		return result;
	}
	private int searchForStringIn(String filename, String[] searchForThis, Line first, int from) {
        if (!new File(filename).exists()) {
            return -1;
        }
        RAF raf = null;
        try {
            raf = RafFactory.getRafSingleLine(filename);
            raf.seek(first.filePos);
            String line = null;
            int lineNum = first.number();
            while((line = raf.readLine()) != null) {
            	for (String string : searchForThis) {
            		if(line.contains(string) && lineNum > from) {
            			return lineNum;
            		}
				}
                lineNum++;
            }
            return -1;
        } catch (IOException e) {
        	e.printStackTrace();
            
        } finally {
            try {
                raf.close();
            } catch(Throwable t){}
        }
        return -1;
    }

    List<LogLine> getFilePageNow(String filename, int pageSize, int startLine, List<Line> lines) {
		
		List<LogLine> result = new ArrayList<LogLine>();
		HashMap<Integer, Line> lineMap = new HashMap<Integer, Line>();
		for (Line line : lines) {
			lineMap.put(line.number(), line);
		}
		
		
		File f = new File(filename);
		if (!f.exists()) return result;
		DateTimeExtractor timeExtractor = new DateTimeExtractor();
		RAF raf = null;
		try {
			raf = RafFactory.getRafSingleLine(filename);
			long lineTime = f.lastModified();
			int lineNum = 1;
			long startPos = -1;
			// try and use the lines mapp
			if (lines.size() > 0) {
				Line firstLine = lines.get(0);
				startPos = firstLine.position();
				raf.seek(firstLine.position());
				lineTime = firstLine.time();
				lineNum = firstLine.number();
			}
			
			LOGGER.info(" Loading FilePage requestFrom:" + startLine + " @startAtLine:" + lineNum + " page:" + pageSize + " pos:" + startPos);
			// need to break the lines down - they are indexed multiline so we cannot do a simple iteration
			String line = "";
			while ((line = raf.readLine()) != null && result.size() < pageSize && lineNum < (startLine + pageSize * 2)) {
//				if ((lineNum % 100) == 0) LOGGER.info(" ---- line:" + lineNum);
				if (isOnPage(pageSize,lineNum,startLine)) {
					// try and find the matching line from the file position
					Line line2 = lineMap.get(lineNum);
					if (line2 != null) lineTime = line2.time();
					Date time = timeExtractor.getTimeWithFallback(line, lineTime);
					result.add(new LogLine(time.getTime(), formatter.print(time.getTime()), lineNum, line));
				} 
				lineNum++;
			}

			LOGGER.info(String.format("<< Streaming[%s] startLine[%d], page[%d] lines[%d]", filename, startLine, pageSize, result.size()));
			
			// if we are at EOF!
			if (line == null && result.size() < pageSize/2) {
				result.add(getLastlineOfFile(lineNum+1));
			}
			LOGGER.info(String.format("Returning start[%d] pageSize[%d] Lines[%d]", startLine, pageSize, result.size()));
			return result;
		} catch (Throwable e) {
			LOGGER.error("Streaming of file:" + filename + " ex:" + e.toString(), e);
			throw new RuntimeException(e);
		} finally {
			try {
				if (raf != null) raf.close();
			} catch (Throwable t) {}
		}
	}
	public boolean isOnPage(int pageSize, int line, int fromLine) {
		int pageRequested = fromLine/pageSize;
		return line/pageSize == pageRequested;
	}
	public LogLine getLastlineOfFile(int lineNumber) {
		return new LogLine(DateTimeUtils.currentTimeMillis(), formatter.print(DateTimeUtils.currentTimeMillis()),lineNumber,
			"==================================== CURRENT END OF FILE =======================================" );
	}
}
