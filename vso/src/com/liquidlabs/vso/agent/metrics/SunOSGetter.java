package com.liquidlabs.vso.agent.metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.agent.MBeanGetter;

public class SunOSGetter extends MBeanGetter implements OSGetter {
	
	private static final Logger LOGGER = Logger.getLogger(SunOSGetter.class);
	
	String cpuModel = "unknown";
	String cpuSpeed = "unknown";
	String domain = "unknown";
	String subnetMask = "unknown";
	String totalCoreCount = "unknown";
	long lastUpdate;
	long lastCpuValue;
	
	String gateway = "unknown";
	
	
	private CPUGetter cpuThing;
	
	public SunOSGetter() {
		
	}

	public static class CPUGetter implements Runnable {
		int val;
		Process process;
		public void run() {
			ProcessBuilder processBuilder = new ProcessBuilder("/usr/bin/iostat", "-c", "60");
			try {
				process = processBuilder.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String data = null;
				while((data = readLine(reader)) != null) {
					parseCpuPercent(data.trim());
				}
			} catch (IOException e) {
				LOGGER.error(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (process != null) {
				process.destroy();
			}
		}

		private String readLine(BufferedReader reader) throws IOException, InterruptedException {
			String readLine = "";
			while (isEmptyLine(readLine)){
				readLine = reader.readLine();
				if (showReadLines) System.out.println("CPU READLINE[" + readLine + "]");
				if (isEmptyLine(readLine)) {
					Thread.sleep(1000);
				}
			}
			if (showReadLines) System.out.println("RETURN CPU READLINE[" + readLine + "]");
			return readLine;
		}

		private boolean isEmptyLine(String readLine) {
			return readLine == null || readLine != null && readLine.trim().length() == 0;
		}

		// sunos gives 5
		//-bash-3.00$ iostat -c
		// cpu
	    //  us sy wt id
	    //   2  1  0 97

		public int parseCpuPercent(String data) {
			if (data.length() == 0) return -1;
			if (data.contains("cpu")) return -1;
			if (data.contains("id")) return -1;
			
			try {
			
				String replaceAll = data.replaceAll("   ", " ");
				replaceAll = replaceAll.replaceAll("  ", " ");
				String[] split = replaceAll.split(" ");
				
				Double d = 100 - Double.valueOf(split[split.length-1]);
				d += 0.5;
				val = d.intValue();
				return val;
			} catch(Throwable t) {}
			return -1;
		}
		
		public void stop() {
			process.destroy();
		}
		
	}

	public int getCPULoadPercentage() {
		if (cpuThing == null) {
			cpuThing = new CPUGetter();
			Thread cpu = new Thread(cpuThing);
			cpu.setName("CPULoadReader");
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
			cpuModel = getItemFromCMD("/usr/bin/kstat",cpuModel, "brand", false);
		} catch (Throwable t) {
			cpuModel = "unknown";
			LOGGER.warn(t.getMessage());
		}
		return cpuModel;
	}
	public String getCPUSpeed() {
		try {
				cpuSpeed = getItemFromCMD("/usr/bin/kstat",cpuSpeed, "clock_MHz", false);
		} catch (Throwable t) {
			cpuSpeed = "unknown";
			LOGGER.warn(t.getMessage());
		}
		return cpuSpeed;
	}
	
	public String getGateway() {
		try {
			if (gateway.equals("unknown")) {
				String cmd ="/usr/bin/netstat -r";
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
			if (true) return "unknown";
			
//			if (domain.equals("unknown")) {;
//			
//				String cmd = "/bin/hostname";
//				Process exec = exec(cmd.split(" "));
//				BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
//				String readLine = reader.readLine();
//				if (readLine.indexOf(".") == -1) {
//					domain = "unknown-0";
//				}
//				else {
//					String hostname = readLine.split("\\.")[0];
//					domain = readLine.substring(hostname.length()+1);
//				}
//				reader.close();
//				closeProcess(exec);
//			}
		} catch (Throwable t) {
			LOGGER.warn(t.getMessage());
		}
		return domain;
	}

	public String getSubnetMask() {
		try {
			subnetMask = "";//getItemFromCMD("/proc/cpuinfo", subnetMask, "Subnet Mask:", false);
		} catch (Throwable t) {
			LOGGER.warn(t.getMessage());
		}
		return subnetMask;
	}

	public int getTotalCPUCoreCount() {
		if (totalCoreCount.equals("unknown")) {
			try {
				totalCoreCount = "" + countItemFromSystemProfiler("/usr/bin/kstat" , totalCoreCount, "ncpus");
				return Integer.parseInt(totalCoreCount);
			} catch (Throwable t) {
				LOGGER.error(t);
				totalCoreCount = "" + Runtime.getRuntime().availableProcessors();
			}
		}
		return Integer.parseInt(totalCoreCount);
	}
	
	private String getItemFromCMD(String source, String existingValue, String key, boolean countIt) throws IOException {
		if (existingValue != null && !existingValue.equals("unknown")) return existingValue;

		Process exec = exec(source);
		BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
		String line = null;
		int count = 0;
		String lastValue = "";
		while ((line = reader.readLine()) != null) {
			if (line.contains(key)) {
				count++;
				lastValue = line.replace(key, "").trim();
			}
		}
		reader.close();
		closeProcess(exec);
		if (countIt) return new Integer(count).toString();
		else return lastValue;
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
	
	public int getTotalMemoryMb() {
		return super.getPhysicalMemTotalMb();
	}
	public int getAvailMemMb() {
		return super.getPhysicalMemFreeMb();
	}
	private Process exec(String... args) throws IOException {
		ProcessBuilder builder = new ProcessBuilder(args);
		builder.redirectErrorStream(true);
		Process p = builder.start();
		p.getOutputStream().flush();
		p.getOutputStream().close();

		return p;
	}
	public static boolean showReadLines = false;
	public static void main(String[] args) {
		SunOSGetter getter = new SunOSGetter();
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
		
		System.out.println("Going to display CPU info....");
		showReadLines = true;
		
		try {
			Thread.sleep(30 * 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done...");
		
	}




	public static boolean isA() {
		return System.getProperty("os.name").toUpperCase().contains("SUNOS");
	}	

}
