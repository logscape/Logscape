package com.liquidlabs.log;

public class WriterResult {
	public final StringBuilder buffer;
	public final int currentLine;
	public final int linesRead;
	public long currentPosition;
	
	public WriterResult(StringBuilder buffer, int currentLine, int linesRead) {
		this.buffer = buffer;
		this.currentLine = currentLine;
		this.linesRead = linesRead;
	}

	public WriterResult(StringBuilder buffer, int currentLine, int linesRead, long position) {
		this.buffer = buffer;
		this.currentLine = currentLine;
		this.linesRead = linesRead;
		this.currentPosition = position;
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[WriterResult:");
		buffer.append(" buffer: ");
		buffer.append(buffer);
		buffer.append(" currentLine: ");
		buffer.append(currentLine);
		buffer.append(" currentPosition: ");
		buffer.append(currentPosition);
		buffer.append("]");
		return buffer.toString();
	}

	
}
