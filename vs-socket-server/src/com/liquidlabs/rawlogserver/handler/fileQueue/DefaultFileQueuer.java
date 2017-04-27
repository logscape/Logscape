/**
 * 
 */
package com.liquidlabs.rawlogserver.handler.fileQueue;

import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler;
import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler.TypeMatcher;
import org.joda.time.DateTimeUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DefaultFileQueuer implements FileQueuer {
	public String destinationFile;
	Integer bufferSecs = Integer.getInteger("raw.server.fq.delay.secs", 3);
	LinkedBlockingQueue<String> buffer = new LinkedBlockingQueue<String>(2024);
	long lastFired = -1;
	private boolean isMLine;
	private File parentFile;
	private String filename;

	public DefaultFileQueuer(String destinationFile, boolean isMLine) {
        this.destinationFile = destinationFile;
        this.isMLine = isMLine;
        File destFile = new File(destinationFile);
        filename = destFile.getName();
        parentFile = destFile.getParentFile();
        parentFile.mkdirs();
    }
	public void append(String lineData) {
		try {
			buffer.add(lineData);
			if (buffer.size() > 1000 && !isMLine) {
				flush();
			}
		} catch (Exception ex) {
			flush();
		}
	}
	@Override
	public int hashCode() {
		return destinationFile.hashCode();
	}
	
	@Override
	public String toString() {
		return String.format("\n%s dest:%s buffer:%d", getClass().getSimpleName(), this.destinationFile, this.buffer.size());
	}
	@Override
	public boolean equals(Object obj) {
		String destinationFile2 = ((DefaultFileQueuer)obj).destinationFile;
		return destinationFile2.equals(destinationFile);
	}
	@Override
	synchronized public void run() {
		if (lastFired < DateTimeUtils.currentTimeMillis() - bufferSecs * 1000 || buffer.size() > 1000) {
			lastFired = DateTimeUtils.currentTimeMillis();
			if (!isMLine) flush();
		}
	}
	synchronized public void flush() {
		OutputStream fos = null;
		try {
			fos = new BufferedOutputStream(new FileOutputStream(this.destinationFile, true));

			while (buffer.size() > 0) {
				String nextLine = buffer.poll(1, TimeUnit.SECONDS);
				if (nextLine == null) continue;
				if (isMLine && (nextLine.length() == 1 || nextLine.length() == 2)) continue;
				fos.write(nextLine.getBytes());
			}
			fos.close();
		} catch (Exception e) {
			// dont want to risk OOM
			buffer.clear();
			ContentFilteringLoggingHandler.LOGGER.warn("WriteFailed:" + destinationFile + " e:" + e.toString(), e);
		} finally {
			try {
				if (fos != null) fos.close();
			} catch (Throwable t) {
			}
		}
	}

    @Override
    public void setTokenAndTag(String sourcehost, String token, String tag) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void flushMLine(TypeMatcher lastType) {
		String postProcessPart = lastType.postProcess(buffer);
		this.destinationFile = parentFile.getAbsolutePath() + "/" + postProcessPart + "/" + filename;
		new File(this.destinationFile).getParentFile().mkdirs();
		flush();
	}
}