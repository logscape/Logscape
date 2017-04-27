package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.file.FileUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Chunks of a file and closes/opens the file between each call
 * - 2000 lines can be handles in 10-15ms (short file open time)
 * - 10% slower end-to-end time than BufferedVRaf
 * @author neil
 *
 */
public class ChunkingRAFReader {
	
	private final String filename;
	private long lastPosition;
	
	// 2K lines is about 13-15ms of open time
	int maxLines = Integer.getInteger("raf.read.chunk.lines", 2000);
    int maxVolume = Integer.getInteger("raf.read.chunk.volume.mb", 3) * FileUtil.MEGABYTES;

	private RAF raf;
	long elapsed;
	private ArrayList<Long> positions;
	private ArrayList<Integer> linesRead;
	private final String breakRule;
	private int startingLine = 1;

	public ChunkingRAFReader(String filename, String breakRule) throws FileNotFoundException {
		this.filename = filename;
		this.breakRule = breakRule;
	}
	
	public void seek(long pos) {
		if (raf != null)
			try {
				raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		raf = null;
		this.lastPosition = pos;
	}
	
	public List<String> readLines(long pos) throws IOException {
		
		lastPosition = pos;
		try {
			if (raf == null) {
				raf = RafFactory.getRaf(filename, breakRule);
                raf.seek(pos);
			}
			positions = new ArrayList<Long>();
			linesRead = new ArrayList<Integer>();
			ArrayList<String> results = new ArrayList<String>();
			
			String nextLine = null;
			
			long previousPosition = pos;
			int lineNumber = startingLine;
            int bytesRead = 0;
			
			while (results.size() < maxLines && bytesRead < maxVolume && (nextLine = raf.readLine()) != null ) {
                bytesRead += nextLine.length();
                results.add(nextLine);
				positions.add(previousPosition);
				int linesRead2 = raf.linesRead();
				linesRead.add(linesRead2);
				lastPosition = raf.getFilePointer();
				previousPosition = lastPosition;
				lineNumber += linesRead2;
			}
			if (results.size() == 0) return null;
			
			startingLine = lineNumber;
			
			return results;
		} finally {
         
		}
	}
	public long getFilePointer() throws IOException {
		return lastPosition;
	}
	public ArrayList<Long> getPositions() {
		return positions;
	}

	public long getLinePosition(int i) {
		return positions.get(i);
	}

	public int getLinesRead(int lineItem) {
		return linesRead.get(lineItem);
	}

	public boolean wasEOLFound() {
		return raf.wasEOLFound();
	}
	public void close() throws IOException {
	   if (raf != null) {
		    raf.close();
		    raf = null;
       }
	}

	public void setStartLine(int startingLine) {
		this.startingLine = startingLine;
	}
    public int getMinLineLength() {
        return raf.getMinLineLength();
    }
}
