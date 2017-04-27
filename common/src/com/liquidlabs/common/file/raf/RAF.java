package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.file.FileUtil;

import java.io.IOException;

public interface RAF {
	static final int MAX_LINES_PERLINE = Integer.getInteger("raf.mlines.per.event",256);
	static final int maxTotalLength = MAX_LINES_PERLINE * 1024;
    static final int BUFFER_LENGTH = Integer.getInteger("raf.buffer.size.mb",1) * FileUtil.MEGABYTES;
	static final int maxLineLength = Integer.getInteger("log.max.line.length.kb", 2) * FileUtil.MEGABYTES;
    static final char EOL_R = '\r';
    static final char EOL_N = '\n';
    static final int  EOF = '\26';
    static final int  MINUS_ONE = -1;
    public String RAF_DIRECT = "raf.direct";
    static final boolean isDirect = System.getProperty(RAF_DIRECT,"true").equals("true");


    byte read() throws IOException;
    String getFilename();

	String readLine() throws IOException;
    String readLine(int minLength) throws IOException;

    String readLineSingle(int minLength) throws IOException;

    int getNewLineLength();
    int getMinLineLength();

	long getFilePointer() throws IOException;
    long getFilePointerRAF() throws IOException;

	void seek(long pos) throws IOException;

	long length() throws IOException;

	void close() throws IOException;

	int linesRead();

	void setBreakRule(String breakRule);

    boolean wasEOLFound();

	/**
	 * Peer ahead - return byte[0] when not enough bytes
	 * @param i
	 * @param breakLength
	 * @return
	 */
	byte[] peek(int i, int breakLength);
}
