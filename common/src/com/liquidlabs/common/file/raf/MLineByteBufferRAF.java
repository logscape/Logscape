package com.liquidlabs.common.file.raf;

import java.io.IOException;

public class MLineByteBufferRAF extends ByteBufferRAF {
	
	
	private LineReader lineReader;
	
	public MLineByteBufferRAF(String file) throws IOException {
		super(file);
		lineReader = new LineReader(this); 
	}

	public int linesRead() {
		return lineReader.linesRead();
	}
	public void setBreakRule(final String breakRule) {
		lineReader.setBreakRule(breakRule);
	}

    public String readLine() throws IOException {
        return lineReader.readLine();
    }

    public String readLine(int minLength) throws IOException {
        return lineReader.readLine(minLength);
    }

    public boolean wasEOLFound() {
		return lineReader.wasEOLFound();
	}
    public String toString() {
        return super.toString() + " file:" + super.file.getPath();
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
