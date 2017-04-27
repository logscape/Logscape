/**
 * 
 */
package com.liquidlabs.vso.agent.process;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


abstract class VSProcess implements AProcess {
	final Process theProcess;
	private final File pidDir;

	public VSProcess(Process theProcess, File pidDir) {
		this.theProcess = theProcess;
		this.pidDir = pidDir;		
	}
	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" + theProcess + " : " + isRunning(theProcess);
	};
	
	private static boolean isRunning(Process process) {
		try {
			process.exitValue();
			return false;
		} catch (IllegalThreadStateException e) {
			return true;
		} 
	}
	public OutputStream getOutputStream() {
		return theProcess.getOutputStream();
	}
	
	public InputStream getInputStream() {
		return theProcess.getInputStream();
	}
	
	public InputStream getErrorStream() {
		return theProcess.getErrorStream();
	}
	
	public int exitValue() {
		return theProcess.exitValue();
	}
	
	public int waitFor() throws InterruptedException {
		return theProcess.waitFor();
	}
	
	void createPidFile() {
		try {
			new File(pidDir, String.format("%d.pid",pid())).createNewFile();
		} catch (IOException e) {
		}
	}
	
	public void removePidFile() {
		new File(String.format("pids/%d.pid",pid())).delete();
	}
}