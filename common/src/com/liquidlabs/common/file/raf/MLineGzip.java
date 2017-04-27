package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.file.raf.GzipRaf;

import java.io.IOException;

public class MLineGzip extends GzipRaf {

	private LineReader lineReader;

	public MLineGzip(String gzipFilename) throws IOException {
		super(gzipFilename);
		lineReader = new LineReader(this);
	}
	public void setBreakRule(String breakRule) {
		this.lineReader.setBreakRule(breakRule);
	}

	public String readLine() throws IOException {
		return lineReader.readLine();
	}
	public int linesRead() {
		return lineReader.linesRead();
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
