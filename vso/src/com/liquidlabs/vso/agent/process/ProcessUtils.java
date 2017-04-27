package com.liquidlabs.vso.agent.process;


import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.Date;
import java.util.TreeMap;

import com.liquidlabs.common.DateUtil;
import org.apache.log4j.Logger;
import org.jvnet.winp.WinProcess;

import com.liquidlabs.common.UnixProcessUtils;
import com.liquidlabs.vso.agent.ResourceAgentImpl;

public class ProcessUtils {

	private File pidDir;

	final static Logger LOGGER = Logger.getLogger(ProcessUtils.class);

	private File myPidFile;
	
	public ProcessUtils() {
		pidDir = new File("pids");
		if (!pidDir.exists()) pidDir.mkdir();
	}
	public AProcess convertProcess(Process process) {	
		if (UnixProcessUtils.isUnixProcess(process)) {
			return new UnixProcess(process, pidDir);
		} else {
			return new WindowsProcess(process, pidDir);
		}
	}
	
	public void cleanUpOldProcesses() {
		String [] names = pidDir.list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return !name.contains("agent") &&  name.endsWith(".pid");
			}});
		
		if (names == null) {
			throw new RuntimeException("'pids' dir does not exist - check that the agent process has sufficient permissions to create the dir");
		}
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		String thisPid = runtimeMXBean.getName();
		String msg = String.format("%s myPID[%s] - Pids to kill %s", ResourceAgentImpl.TAG, thisPid, Arrays. toString(names));
		LOGGER.info(msg);
		System.out.println(msg);
		boolean windows = System.getProperty("os.name").contains("Windows");
		
		
		for (String name : names) {
			System.out.println("Pid File= " + name);
			String pid = name.split("\\.")[0];
			
			if (thisPid.contains(pid)) {
				msg = "Cannot kill myself -pid:" + pid;
				System.out.println(msg);
				LOGGER.info(msg);
				continue;
			}



			try {
				new File(pidDir, name).delete();
				if (windows) {
                    WinProcess winProcess = new WinProcess(Integer.valueOf(pid));

                    TreeMap<String,String> environmentVariables = winProcess.getEnvironmentVariables();
                    String cmd = winProcess.getCommandLine();
                    if (isLogscape(cmd) || isAppProcess(cmd)) {
                        printKillMsg(pid);
                        LOGGER.info("Killing:" + pid + " cmd:" + cmd);
                        winProcess.killRecursively();
                    }
				} else {
                    String fileHandlesForWorkingDir = UnixProcessUtils.getPSInfo(Integer.valueOf(pid));
                    if (isLogscape(fileHandlesForWorkingDir) || isAppProcess(fileHandlesForWorkingDir)) {
                        printKillMsg(pid);
                        UnixProcessUtils.killTree(Integer.valueOf(pid));
                    }
				}
			} catch (Throwable t) {
			}
		}
		writeMyPid();
	}

    private void printKillMsg(String pid) {
        String msg;
        msg = String.format("%s - Forceably killing %s", ResourceAgentImpl.TAG, pid);
        LOGGER.info(msg);
        System.out.println(msg);
    }

    private boolean isLogscape(String cmd) {
        return cmd.contains("logscape") || cmd.contains("liquidlabs");
    }
    private boolean isAppProcess(String cmd) {
        return cmd.contains("App-") && cmd.contains("deployed-bundles");
    }

    private void writeMyPid() {
		String myPid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		try {
			myPidFile = new File(pidDir, String.format("%s.pid",myPid));
			myPidFile.createNewFile();
			myPidFile.deleteOnExit();
		} catch (IOException e) {
		}
	}
    public void writePid(int pid, String cmdSysJon) {
        myPidFile = new File(pidDir, pid + ".pid");
        try {
            myPidFile.createNewFile();
            FileOutputStream fos = new FileOutputStream(myPidFile);
            fos.write(cmdSysJon.getBytes());
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
	
	public void deleteMyPid() {
		if (myPidFile != null) myPidFile.delete();
	}

    public void deletePid(int pid) {
        new File(pidDir, pid + ".pid").delete();
    }
}
