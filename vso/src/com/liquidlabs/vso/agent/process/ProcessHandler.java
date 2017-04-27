package com.liquidlabs.vso.agent.process;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.vso.work.WorkAssignment;

public class ProcessHandler {
	
	ProcessUtils processUtils = new ProcessUtils();
	class WorkItem {
		String assignmentId;
		int profileId;
		WorkAssignment workAssignment;
		
		
		public WorkItem(String assignmentId, int profileId, WorkAssignment workAssignment) {
			this.assignmentId = assignmentId;
			this.profileId = profileId;
			this.workAssignment = workAssignment;
		}
		
		public WorkItem(String assignmentId, int id) {
			this(assignmentId, id, null);
		}

		public boolean equals(Object obj) {
			WorkItem other = (WorkItem) obj;
			return assignmentId.equals(other.assignmentId);
		}
		
		@Override
		public int hashCode() {
			return assignmentId.hashCode();
		}

		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append("[WorkItem:");
			buffer.append(" assignmentId: ");
			buffer.append(assignmentId);
			buffer.append("]\n");
			return buffer.toString();
		}
	}
	
	private Map<WorkItem, AProcess> processes  = new ConcurrentHashMap<WorkItem, AProcess>();
	private Map<Integer, String> errorMsgs = new ConcurrentHashMap<Integer, String>();
	private ScheduledExecutorService executor;
	private static final Logger LOGGER = Logger.getLogger(ProcessHandler.class);
	private static final Logger CHILD_LOGGER = Logger.getLogger("ChildProcess");
	private ProcessListener processListener;
	
	public String listProcesses() {
		List<WorkItem> keySet = new ArrayList<WorkItem>(processes.keySet());
		Collections.sort(keySet, new Comparator<WorkItem>(){
			public int compare(WorkItem o1, WorkItem o2) {
				return o1.workAssignment.getId().compareTo(o2.workAssignment.getId());
			}
		});
		StringBuilder results = new StringBuilder();
		for (WorkItem workItem : keySet) {
			try {
				AProcess process = processes.get(workItem);
				results.append(workItem.toString()).append(process.toString());
				results.append("\n\n");
			} catch (Throwable t){
				LOGGER.warn("ex:" + workItem, t);
			}
		}
		return results.toString();
		
		
	}
	public ProcessHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
			public void run() {
				shutdownNow();
				processUtils.deleteMyPid();
			}}));
	}
	
	
	void shutdownNow() {
		LOGGER.info("ShutHook - Destroying outstanding processes, count:" + processes.keySet().size());
		for (Map.Entry<WorkItem, AProcess> entry : new HashSet<Map.Entry<WorkItem, AProcess>>(processes.entrySet())) {
			try {
				AProcess value = entry.getValue();
				dumpStreams(value, entry.getKey());
				entry.getValue().quit();
				LOGGER.info(String.format("Destroyed process %s with exit code %d", entry.getKey().assignmentId, entry.getValue().exitValue()));
			} catch (Throwable t) {}
		}	
		getScheduler().shutdownNow();
	}
	
	private int lastPid = 0;
	synchronized public int manage(WorkAssignment workAssignment, final Process process) {
		AProcess theProcess = processUtils.convertProcess(process);
		String msg = String.format(" Managing process:%s - pid:%d", workAssignment.getId(),theProcess.pid());
		LOGGER.info(msg);
		WorkItem key = new WorkItem(workAssignment.getId(), workAssignment.getProfileId(), workAssignment);
		if (processes.containsKey(key)) {
			LOGGER.info("Going to kill previous process:" + workAssignment.getId() + process.toString());
			this.stop(key);
			waitAbit();
		}
		processes.put(key, theProcess);
		lastPid = theProcess.pid();
		return theProcess.pid();
	}
	private void waitAbit() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
	}
	public int getLastPid() {
		return lastPid;
	}
	
	private Runnable createStreamReaders() {
		return new Runnable() {
			public void run() {
				for (Map.Entry<WorkItem, AProcess> entry : new HashSet<Map.Entry<WorkItem, AProcess>>(processes.entrySet())) {
					AProcess process = entry.getValue();
					if (stillRunning(entry.getKey(), process)) {
						dumpStreams(process, entry.getKey());
					}
				}
				
			}
		};
	}
	
	public void addListener(ProcessListener processListener) {
		this.processListener = processListener;
		start();
	}
	private void start() {
		processUtils.cleanUpOldProcesses();
		getScheduler().scheduleWithFixedDelay(createStreamReaders(), 0, 100, TimeUnit.MILLISECONDS);
	}


	private void dumpStreams(AProcess process, WorkItem workItem) {
		dumpStream(process.getInputStream(), System.out, workItem, "out");
		dumpStream(process.getErrorStream(), System.err, workItem, "err");
	}
	
	private void dumpStream(InputStream stream, PrintStream out, WorkItem workItem, String outOrErr) {
		try {
			if (stream.available() == 0) {
				return;
			}
			
			byte [] buf = new byte[stream.available()];
			int read = stream.read(buf, 0, buf.length);
			String data = new String(buf, 0, read);
			
			if (outOrErr.equals("err")) {
				int pid = processes.get(workItem).pid();
				errorMsgs.put(pid, data);
			}
			
			if (!workItem.workAssignment.isSystemService()) {
				if (data.contains("log:")) {
					String[] split = data.split("\n");
					for (String string : split) {
						if (string.startsWith("log:")) {
							string = string.substring("log:".length());
							outOrErr = "log";
						} 
						if (!string.endsWith("\n")) string += "\n";
						writeUserBundleAppOutput(workItem.workAssignment, string, outOrErr);
						
					}
				} else {
					if (!data.endsWith("\n")) data += "\n";
					writeUserBundleAppOutput(workItem.workAssignment, data, outOrErr);
				}
			} else {
				CHILD_LOGGER.info(data);
			}
			if (stream.available() > 0) {
				dumpStream(stream, out, workItem, outOrErr);
			}
		} catch (IOException e) {
			LOGGER.info(e);
		}
	}
	private void writeUserBundleAppOutput(WorkAssignment workAssignment, String result, String outOrErr) {
		String dateDir = DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
		String outdir = "work/" + workAssignment.getBundleId() + "/" + dateDir + "/";
		
		
		String scriptOutFile = outdir + "/"  + workAssignment.getServiceName() + "." + outOrErr;
		new File(scriptOutFile).getParentFile().mkdirs();
		FileOutputStream fos = null;
		try {
			if (result.length() > 0) {
				fos = new FileOutputStream(scriptOutFile, true);
				fos.write(result.getBytes());
			}
		} catch (Throwable e) {
			LOGGER.error("Script Output Error:" + workAssignment.getServiceName() + " Path:" + scriptOutFile, e);
		} finally {
			try {
				if(fos != null) {
					fos.close();
				}
			} catch (IOException e) {
			
			}
		}
	}
    File pidDir = new File("pids");
	private boolean stillRunning(WorkItem item, AProcess process) {
		try {
			dumpStreams(process, item);
			int exitValue = process.exitValue();
			String msg = errorMsgs.get(process.pid());
			if (msg == null) msg = "";
			if (exitValue != 0) {
				String format = String.format("Process %s pid:%d exited value %d Msg:%s", item.assignmentId, process.pid(), exitValue, msg);
				LOGGER.info(format);
				System.err.println(format);
			}
			processes.remove(item);
            new File(pidDir,process.pid()+".pid").delete();
			processListener.processExited(item.workAssignment, exitValue != 0, exitValue, null, msg);

			// dont leak msgs
			if (errorMsgs.size() > 1024) errorMsgs.clear();
			return false;
		} catch(IllegalThreadStateException ise) {
			return true;
		}
	}

	public boolean stop(WorkAssignment assignment) {
		return stop(new WorkItem(assignment.getId(), assignment.getProfileId(), assignment));
	}
	
	public boolean stop(WorkItem name) {
		AProcess process = processes.get(name);
		
		if (process != null) {
			try {
				LOGGER.info("StoppingProcess:" + name.assignmentId + " pid:" + process.pid());
				process.quit();
				processes.remove(name);
                new File(pidDir,process.pid()+".pid").delete();
				if (!shutdown) processListener.processExited(name.workAssignment, false, 0, null, "");
			} catch (Throwable t){
				LOGGER.info(t.getMessage(), t);
			}
		} else {
			LOGGER.info("StoppingProcess - Cannot find process:" + name.assignmentId);
		}
		return process != null;
		
	}

	private boolean shutdown = false;
	public void shutdown() {
		shutdown = true;
		LOGGER.info("Shutdown:" + processes.keySet());
		Set<WorkItem> processIds = processes.keySet();
		java.util.concurrent.ExecutorService executor = Executors.newCachedThreadPool(new NamingThreadFactory("ProcessHandler"));
		for (final WorkItem processId : processIds) {
			executor.submit(new Runnable() {
				public void run() {
					stop(processId);
				}
			});
		}
		this.executor.shutdownNow();
	}
	private ScheduledExecutorService getScheduler(){
		if (this.executor == null) this.executor = ExecutorService.newScheduledThreadPool(1, "processHandler");
		return executor;
	}


	public int getPid(String assignmentId) {
		WorkItem key = new WorkItem(assignmentId, 0, null);
		if (processes.containsKey(key)) {
			return processes.get(key).pid();
		}
		return 0;
	}
	public boolean isRunning(int lastPid2) {
		Collection<AProcess> processList = processes.values();
		for (AProcess process : processList) {
			if (process.pid() == lastPid2) return true;
		}
		return false;
	}
	public String stop(String workId) {
		Set<WorkItem> keySet = processes.keySet();
		for (WorkItem workItem : keySet) {
			if (workItem.assignmentId.equals(workId)) {
				LOGGER.info("Kill -9 on:" + workId);
				AProcess process = processes.get(workItem);
				process.destroy();
				return "Destroyed:" + process;
			}
		}
		return "Didnt find process to kill";
	}
}
