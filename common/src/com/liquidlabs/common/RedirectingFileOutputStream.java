package com.liquidlabs.common;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class RedirectingFileOutputStream extends OutputStream {
	
	String currentName = "";
	private final FilenameGetter getter;
	private OutputStream fos;
	long lastCheck = -1;
	int written = 0;
	
	public static class FilenameGetter {
		public String getFilename() {
			return "out.log";
		}
	}
	public RedirectingFileOutputStream(FilenameGetter getter) throws FileNotFoundException {
		this.getter = getter;
		fos = getOutputStream();
	}

	private BufferedOutputStream getOutputStream() throws FileNotFoundException {
		String name = this.getter.getFilename();
		this.currentName = name;
		return new BufferedOutputStream(new FileOutputStream(name, true));
	}

	public void write(int b) throws IOException {
		fos.write(b);
		written++;
		fos.flush();
		// auto-flip the new stream after 1 minute
		if (System.currentTimeMillis() > lastCheck + 60 * 1000 && (b == '\n' || b == '\r')){
			lastCheck = System.currentTimeMillis();
			checkOutname();
		}
	}
	
	public void close() throws IOException {
		fos.close();
	}
	public void flush() throws IOException {
		fos.flush();
	}
	public String toString() {
		return super.toString() + " File:" + currentName + " written:" + written;
	}
	public void checkOutname() throws FileNotFoundException {
		String newName = getter.getFilename();
		if (newName.equals(currentName)) return;
		else {
			try {
				fos.flush();
				fos.close();
			} catch (IOException e) {
			}
			fos = getOutputStream();
		}
	}
}
