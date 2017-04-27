package com.liquidlabs.vso.agent.metrics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.agent.MBeanGetter;

public class LinuxOSGetter extends MBeanGetter implements OSGetter {
	
	private static final Logger LOGGER = Logger.getLogger(LinuxOSGetter.class);
	
	String cpuModel = "unknown";
	String cpuSpeed = "unknown";
	String domain = "unknown";
	String subnetMask = "unknown";
	String totalCoreCount = "unknown";
	long lastUpdate;
	long lastCpuValue;
	
	String totalSystemMemory;
	String gateway = "unknown";
	
	String iostat = "/usr/bin/iostat";
	String netstat = "/usr/sbin/netstat";
	String free = "/usr/bin/free";
	
	
	private CPUGetter cpuThing;
	
	public LinuxOSGetter() {
		if (!new File(iostat).exists()) iostat = "iostat";
		if (!new File(netstat).exists()) {
			netstat = "/bin/netstat";
			if (!new File(netstat).exists()) {
				netstat = "netstat";
			}
		}
		
	}

	public static class CPUGetter implements Runnable {
		int val;
		public String iostat; 
		Process process;
		public void run() {
			ProcessBuilder processBuilder = new ProcessBuilder(iostat, "-c", "60");
			try {
				process = processBuilder.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String data = null;
				while((data = reader.readLine()) != null) {
					parseCpuPercent(data.trim());
					Thread.sleep(1000);
				}
			} catch (IOException e) {
			} catch (InterruptedException e) {
			}
			if (process != null) {
				process.destroy();
			}
		}

		// redhat gives 4 results
		// avg-cpu:  %user   %nice    %sys %iowait   %idle
        // 1.89    3.85    1.42    1.21   91.63

		//
		// fedora give 5
		// avg-cpu: %user %nice %system %iowait %steal %idle 
		// 47.03 0.00 6.88 15.82 0.00 30.26 
		public int parseCpuPercent(String data) {
			if (data.length() == 0) return -1;
			if (data.contains("avg-cpu")) return -1;
			if (data.contains("Linux")) return -1;
			
			try {
			
				String replaceAll = data.replaceAll("   ", " ");
				replaceAll = replaceAll.replaceAll("  ", " ");
				String[] split = replaceAll.split(" ");

				// 100% - idle%
				Double d = 100 - Double.valueOf(split[split.length-1]);
				d += 0.5;
				val = d.intValue();
				return val;
			} catch(Throwable t) {}
			return -1;
		}
		
		public void stop() {
			if (process != null) process.destroy();
		}
		
	}
	public int getTotalMemoryMb() {
		return super.getPhysicalMemTotalMb();
	}
	public int getAvailMemMb() {
		return super.getPhysicalMemFreeMb();
	}

	public int getCPULoadPercentage() {
		if (cpuThing == null) {
			cpuThing = new CPUGetter();
			cpuThing.iostat = this.iostat;
			Thread cpu = new Thread(cpuThing,"OSgetter");
			cpu.setDaemon(true);
			cpu.start();
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				public void run() {
					cpuThing.stop();
				}}));
		}
		return cpuThing.val;
	}

	private void closeProcess(Process exec) {
		try {
			exec.getInputStream().close();
			exec.getErrorStream().close();
			exec.destroy();
		} catch (IOException e) {
		}
	}

	public String getCPUModel() {
		
		try {
			cpuModel = getItemFromSystemProfiler("/proc/cpuinfo",cpuModel, "model name");
		} catch (Throwable t) {
			cpuModel = "unknown";
			LOGGER.warn(t.getMessage());
		}
		return cpuModel;
	}
	public String getCPUSpeed() {
		try {
			cpuSpeed = getItemFromSystemProfiler("/proc/cpuinfo",cpuSpeed, "cpu MHz");
		} catch (Throwable t) {
			cpuSpeed = "unknown";
			LOGGER.warn(t.getMessage());
		}
		return cpuSpeed;
	}
	
	public String getGateway() {
		try {
			if (gateway.equals("unknown")) {
				String cmd = netstat + " -r";
				Process exec = exec(cmd.split(" "));
				BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
				String readLine = reader.readLine();
				while (readLine != null) {
					if (readLine.contains("default")) {
						while (readLine.contains("  ")) {
							readLine = readLine.replaceAll("  ", " ");
						}
						String[] split = readLine.split(" ");
						gateway = split[1].trim();
					}
					readLine = reader.readLine();
				}
				reader.close();
				closeProcess(exec);
			}
		} catch (Throwable t) {
			gateway = "unknown-0";
			LOGGER.warn(t.getMessage());
		}
		return gateway;
	}


	public String getDomain() {
		try {
			if (domain.equals("unknown")) {;
			
				String cmd = "/bin/hostname";
				Process exec = exec(cmd.split(" "));
				BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
				String readLine = reader.readLine();
				if (readLine.indexOf(".") == -1) {
					domain = "unknown-0";
				}
				else {
					String hostname = readLine.split("\\.")[0];
					domain = readLine.substring(hostname.length()+1);
				}
				reader.close();
				closeProcess(exec);
			}
		} catch (Throwable t) {
			LOGGER.warn(t.getMessage());
		}
		return domain;
	}

	public String getSubnetMask() {
		try {
			subnetMask = getItemFromSystemProfiler("/proc/cpuinfo", subnetMask, "Subnet Mask:");
		} catch (Throwable t) {
			LOGGER.warn(t.getMessage());
		}
		return subnetMask;
	}

	public int getTotalCPUCoreCount() {
		if (totalCoreCount.equals("unknown")) {
			try {
				totalCoreCount = "" + countItemFromSystemProfiler("/proc/cpuinfo" , totalCoreCount, "processor");
				return Integer.parseInt(totalCoreCount);
			} catch (Throwable t) {
				LOGGER.warn(t.toString());
				return 4;
			}
		}
		return Integer.valueOf(totalCoreCount);
	}
	
	private String getItemFromSystemProfiler(String source, String existingValue, String key) throws IOException {
		if (existingValue != null && !existingValue.equals("unknown")) return existingValue;

		Process exec = exec("cat",source);
		BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
		String line = null;
		while ((line = reader.readLine()) != null) {
			if (line.contains(key)) {
				return line.replaceAll(key, "").replaceAll(":","").trim();
			}
		}
		reader.close();
		closeProcess(exec);
		return "unknown";
	}
	private int countItemFromSystemProfiler(String source, String existingValue, String key) throws IOException {
		
		Process exec = exec("cat", source);
		BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
		String line = null;
		int count = 0;
		while ((line = reader.readLine()) != null) {
			if (line.contains(key)) {
				count++;
			}
		}
		reader.close();
		closeProcess(exec);
		return count;
	}
	private Process exec(String... args) throws IOException {
		ProcessBuilder builder = new ProcessBuilder(args);
		builder.redirectErrorStream(true);
		Process p = builder.start();
		p.getOutputStream().flush();
		p.getOutputStream().close();

		return p;
	}
	public static void main(String[] args) {
		LinuxOSGetter getter = new LinuxOSGetter();
		System.out.println("primeCPU:" + getter.getCPULoadPercentage());
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("2CPU:" + getter.getCPULoadPercentage());
		System.out.println("Model:" + getter.getCPUModel());
		System.out.println("Speed:" + getter.getCPUSpeed());
		System.out.println("Domain:" + getter.getDomain());
		System.out.println("Subnet:" + getter.getSubnetMask());
		System.out.println("CoreCount:" + getter.getTotalCPUCoreCount());
	}




	public static boolean isA() {
		return System.getProperty("os.name").toUpperCase().contains("LINUX");
	}	

}
