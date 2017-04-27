package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.file.raf.BzipRaf;

import java.io.IOException;

public class MLineBzip extends BzipRaf {
	
	/**
	 * The number of lines read on the last readLine() call 
	 */
	private LineReader lineReader; 

	public MLineBzip(String gzipFilename) throws IOException {
		super(gzipFilename);
		lineReader = new LineReader(this); 
	}
	
	public void setBreakRule(String breakRule) {
		lineReader.setBreakRule(breakRule);
	}
	public String readLine() throws IOException {
		return lineReader.readLine();
	}
	public boolean wasEOLFound() {
		return lineReader.wasEOLFound();
	}
    public int getMinLineLength() {
        return lineReader.getMinLength();
    }
    @Override
    public long getFilePointer() throws IOException {
        if (lineReader != null) {
            return lineReader.lastRafPos;
        }
        return super.getFilePointer();
    }
}
