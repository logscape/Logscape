/**
 * 
 */
package com.liquidlabs.vso.agent.process;

import java.io.File;

import org.jvnet.winp.WinProcess;

public class WindowsProcess extends VSProcess {
	private WinProcess winProcess;
	public WindowsProcess(Process theProcess, File pidDir) {
		super(theProcess, pidDir);
		winProcess = new WinProcess(theProcess);
		createPidFile();
	}

	
	public void destroy() {
		winProcess.killRecursively();
		removePidFile();
	}


	public void quit() {
		winProcess.killRecursively();
		removePidFile();
	}


	public int pid() {
		return winProcess.getPid();
	}
	public String toString() {
		return super.toString() + " : " + winProcess.toString() + " pid:" + pid();
	}
	
	
}