package com.liquidlabs.common.file.raf;

import java.io.IOException;

public class MLineSnapRAF extends SnapRaf {

	private LineReader lineReader;

	public MLineSnapRAF(String file) throws IOException {
		super(file);
		lineReader = new LineReader(this); 
	}

	public int linesRead() {
		return lineReader.linesRead();
	}
	public String toString() {
		return getClass() + " " +  super.toString();
	}
	public void setBreakRule(final String breakRule) {
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
