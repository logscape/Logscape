/**
 * 
 */
package com.liquidlabs.vso.agent.process;

import java.io.File;

import com.liquidlabs.common.UnixProcessUtils;

public class UnixProcess extends VSProcess {

	private int pid;
	
	public UnixProcess(Process theProcess, File pidDir) {	
		super(theProcess, pidDir);
		pid = UnixProcessUtils.getUnixPid(theProcess);
		createPidFile();
	}

	public void quit() {
		String osName = System.getProperty("os.name").toUpperCase();
		if (osName.equals("SUNOS")) {
			// open solaris is crap and kill -TERM is stuffed on java spawned processes - it is ignored by everything apart from -9
			theProcess.destroy();
		} else {
			UnixProcessUtils.quitTree(pid);
		}
		removePidFile();
	}

	public void destroy() {
		UnixProcessUtils.killTree(pid);
		removePidFile();
	}
	
	public int pid() {
		return pid;
	}
}