package com.liquidlabs.common;

import java.io.FileOutputStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ThreadUtil {

	public static String threadDump(String filename, String commaSeperatedFilter){
		if (commaSeperatedFilter == null) commaSeperatedFilter = "";
        try {
		
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            ThreadInfo[] dumpAllThreads = threadMXBean.dumpAllThreads(true, true);
            StringBuilder stringBuilder = new StringBuilder();
            String[] substrings = commaSeperatedFilter.split(",");
            for (ThreadInfo threadInfo : sort(dumpAllThreads)) {
                String threadInfoS = toString(threadInfo);
                if (substrings.length > 0) {
                    for (String filter : substrings) {
                        if (StringUtil.containsIgnoreCase(threadInfoS, filter)) stringBuilder.append(threadInfoS);
                    }
                } else {
                    stringBuilder.append(threadInfoS);
                }
            }
            if (filename != null && filename.length() > 0) {
                try {
                    FileOutputStream fos = new FileOutputStream(filename);
                    fos.write(stringBuilder.toString().getBytes());
                    fos.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return stringBuilder.toString();
        } catch (Throwable t) {
            t.printStackTrace();
            try {
            Thread.sleep(1 * 1000);
            } catch(Throwable t2) {}
        }
        return "";
	}
	
	private static List<ThreadInfo> sort(ThreadInfo[] dumpAllThreads) {
		List<ThreadInfo> allThreads = new ArrayList<ThreadInfo>(Arrays.asList(dumpAllThreads));
		Collections.sort(allThreads, new Comparator<ThreadInfo>(){
			@Override
			public int compare(ThreadInfo o1, ThreadInfo o2) {
				return o1.getThreadName().compareTo(o2.getThreadName());
			}
		});
		return allThreads;
	}
	public static String getMethodName() {
		final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
		return ste[2].getMethodName();
	}
	  
	 static public String toString(ThreadInfo threadInfo) {
	        StringBuilder sb = new StringBuilder("\"" + threadInfo.getThreadName() + "\"" +
	                                             " Id=" + threadInfo.getThreadId() + " " +
	                                             threadInfo.getThreadState());
	        if (threadInfo.getLockName() != null) {
	            sb.append(" on " + threadInfo.getLockName());
	        }
	        if (threadInfo.getLockOwnerName() != null) {
	            sb.append(" owned by \"" + threadInfo.getLockOwnerName() +
	                      "\" Id=" + threadInfo.getLockOwnerId());
	        }
	        if (threadInfo.isSuspended()) {
	            sb.append(" (suspended)");
	        }
	        if (threadInfo.isInNative()) {
	            sb.append(" (in native)");
	        }
	        sb.append('\n');
	        int i = 0;
	        StackTraceElement[] stackTrace = threadInfo.getStackTrace();
	        for (StackTraceElement ste : stackTrace) {
//	        for (; i < threadInfo.getStackTrace().length && i < 64; i++) {
//	            StackTraceElement ste = stackTrace[i];
	            sb.append("\tat " + ste.toString());
	            sb.append('\n');
	            if (i == 0 && threadInfo.getLockInfo() != null) {
	                Thread.State ts = threadInfo.getThreadState();
	                switch (ts) {
	                    case BLOCKED: 
	                        sb.append("\t-  blocked on " + threadInfo.getLockInfo());
	                        sb.append('\n');
	                        break;
	                    case WAITING:
	                        sb.append("\t-  waiting on " + threadInfo.getLockInfo());
	                        sb.append('\n');
	                        break;
	                    case TIMED_WAITING:
	                        sb.append("\t-  waiting on " + threadInfo.getLockInfo());
	                        sb.append('\n');
	                        break;
	                    default:
	                }
	            }

	            for (MonitorInfo mi : threadInfo.getLockedMonitors()) {
	                if (mi.getLockedStackDepth() == i) {
	                    sb.append("\t-  locked " + mi);
	                    sb.append('\n');
	                }
	            }
	       }
	       if (i < stackTrace.length) {
	           sb.append("\t...");
	           sb.append('\n');
	       }
	 
	       LockInfo[] locks = threadInfo.getLockedSynchronizers();
	       if (locks.length > 0) {
	           sb.append("\n\tNumber of locked synchronizers = " + locks.length);
	           sb.append('\n');
	           for (LockInfo li : locks) {
	               sb.append("\t- " + li);
	               sb.append('\n');
	           }
	       }
	       sb.append('\n');
	       return sb.toString();
	    }

}
