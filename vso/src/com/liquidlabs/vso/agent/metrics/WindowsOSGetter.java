package com.liquidlabs.vso.agent.metrics;

import com.liquidlabs.vso.agent.MBeanGetter;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * look at wmic /? to view aliases to query
 */
public class WindowsOSGetter extends MBeanGetter implements OSGetter {

	private final static Logger LOGGER = Logger.getLogger(WindowsOSGetter.class);
	String system32 = System.getProperty("win.sys32", "C:\\WINDOWS\\SYSTEM32");
	String pathToVbs = System.getProperty("os.getter.path.cpu", "scripts");
	Integer waitForFirstCPURead = Integer.getInteger("os.getter.cpu.read.wait", 60);
	String cmdSys = String.format("%s\\cscript.exe %s\\cpu.vbs //nologo ", system32, pathToVbs);


	private static final String REGQUERY_UTIL = "reg query ";
	  private static final String REGSTR_TOKEN = "REG_SZ";
	  private static final String REGDWORD_TOKEN = "REG_DWORD";

	private static final String CPU_NAME_CMD = REGQUERY_UTIL +
	   "\"HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0\"" 
	     + " /v ProcessorNameString";
	
	  private static final String CPU_SPEED_CMD = REGQUERY_UTIL +
	    "\"HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0\"" 
	     + " /v ~MHz";
	  
	private DecimalFormat m_format;
	private int cpuLoad;
	private int totalCpuCoreCount = 0;
	private String cpuModel = "unknown";
	private int maxMemoryMb = -1;
	private int availMemoryMb = -1;
	private int availVMMb = -1;
	private String gateway = "unknown";
	

	public WindowsOSGetter(ScheduledExecutorService scheduler) {
		try {

			if (isWin32()) {				
			} else if (scheduler != null) {
				
				scheduler.scheduleAtFixedRate(new Runnable() {

					public void run() {
						try {
							BufferedReader procout = exec(cmdSys);
							String line = procout.readLine();
							procout.close();
							cpuLoad = Integer.parseInt(line.split(":")[1]); 
						} catch (Exception e) {
						}
					}
					
				}, waitForFirstCPURead, 3 * 60, TimeUnit.SECONDS);
			}
//			m_PID = SystemInformation.getProcessID();
			m_format = new DecimalFormat();
			m_format.setMaximumFractionDigits(1);
		} catch (Throwable t){
		}
	}

	private boolean isWin32() {
		String osName = System.getProperty("os.name").toUpperCase();
		if (osName.contains("2008") || osName.contains("WINDOWS 7") || osName.contains("WINDOWS")) return false;
		
		return true;
	}

	public int getCPULoadPercentage() {
		return this.cpuLoad;
	}

	public int getTotalCPUCoreCount() {
		try {
			if (totalCpuCoreCount > 0) return totalCpuCoreCount;
			String processors = System.getenv("NUMBER_OF_PROCESSORS");
			totalCpuCoreCount = Integer.parseInt(processors);
			return totalCpuCoreCount;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return 1;
		}
	}



	public String getCPUModel() {
		try {
			if (!cpuModel.equals("unknown")) return cpuModel;
			
			BufferedReader procout = exec(CPU_NAME_CMD);
			String value = readLine("unknown", procout);
			int p = value.indexOf(REGSTR_TOKEN);
			if (p == -1) return null;
	
		      String cpuModel = value.substring(p + REGSTR_TOKEN.length()).trim();
			return cpuModel;
		} catch (IOException ex) {
			ex.printStackTrace();
			return "unknown2";
		}
	}
	public String getCPUSpeed() {
		try {
			BufferedReader procout = exec(CPU_SPEED_CMD);
			String value = readLine("unknown", procout);
			  // CPU speed in Mhz (minus 1) in HEX notation, convert it to DEC
			int p = value.indexOf(REGDWORD_TOKEN);

		      String temp = value.substring(p + REGDWORD_TOKEN.length()).trim();
		      return Integer.toString
		          ((Integer.parseInt(temp.substring("0x".length()), 16) + 1));

		} catch (IOException ex) {
			ex.printStackTrace();
			return "unknown2";
		}
	}

	public String getSubnetMask() {
		try {
			BufferedReader reader = exec("ipconfig.exe");
			String value = null;
			String line;
			while ((line = reader.readLine()) != null && value == null) {
				if (line.toUpperCase().trim().contains("SUBNET MASK")) {
					value = line.split(":")[1].trim();
				}
			}
			return value;
		} catch (IOException e) {
			e.printStackTrace();
			return "unknown2";
		}
	}

	public String getDomain() {
		try {
			BufferedReader reader = exec("ipconfig.exe");
			String value = "";
			String line;
			while ((line = reader.readLine()) != null) {
				line = line.trim().toUpperCase();
				if (line.contains("DNS Suffix".toUpperCase())) {
					String[] split = line.split(":");
					if (split.length >= 2) value = split[1].trim();
				}
			}
			return value;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	public String getGateway() {
		if (!gateway.equals("unknown")) return gateway;
		try {
			BufferedReader procout = exec("ipconfig.exe");
			String line = null;
			while ((line = procout.readLine()) != null) {
					if (line.contains("Default Gateway")) gateway = line.split(":")[1].trim();
			}
			return gateway;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public int getTotalMemoryMb() {
		if (maxMemoryMb != -1) return maxMemoryMb;
		refreshMemStats();
		if (maxMemoryMb == -1) maxMemoryMb = super.getPhysicalMemTotalMb();
		return maxMemoryMb;
	}
	public int getAvailMemMb() {
		refreshMemStats();
		return availMemoryMb;
	}

	private void refreshMemStats()  {
		try {
			BufferedReader procout = exec("wmic os get FreePhysicalMemory,TotalVisibleMemorySize,FreeVirtualMemory");
			String keysString = procout.readLine();
			procout.readLine();
			String[] keys = keysString.split("\\s+");
			String valuesString = procout.readLine();
			String[] values = valuesString.split("\\s+");
			HashMap<String, String> map = new HashMap<String, String>();
			for (int i = 0; i < keys.length; i++) {
				map.put(keys[i],values[i]);
			}
			procout.close();
			maxMemoryMb = Integer.parseInt(map.get("TotalVisibleMemorySize")) / (1024);
			availMemoryMb = Integer.parseInt(map.get("FreePhysicalMemory")) / (1024);
			availVMMb = Integer.parseInt(map.get("FreeVirtualMemory")) / (1024);
		} catch (Throwable t) {
			
		}
	}

	private BufferedReader exec(String args) throws IOException {
		String[] splitArgs = args.split(" ");
		ProcessBuilder builder = new ProcessBuilder(splitArgs);
		Process p = builder.start();
		p.getOutputStream().flush();
		p.getOutputStream().close();
		return new BufferedReader(new InputStreamReader(p.getInputStream()));
	}
	private String readLine(String value, BufferedReader procout) throws IOException {
		String line = procout.readLine();
		while ((line = procout.readLine()) != null) {
			if (line.length() > 0) value = line;
		}
		procout.close();
		return value;
	}
	public static void main(String[] args) {
//		String pathToVbs = System.getProperty("os.getter.path.cpu", "system/boot-1.0");
//		Integer waitForFirstCPURead = Integer.getInteger("os.getter.cpu.read.wait", 60);

		System.setProperty("os.getter.path.cpu", ".");
		System.setProperty("os.getter.cpu.read.wait", "1");
		System.setProperty("winOs.show.exs", "true");
		WindowsOSGetter getter = new WindowsOSGetter(Executors.newScheduledThreadPool(1));
		try {
			System.out.println("waiting.... ");
			Thread.sleep(5 * 1000);
			int c = 0;
			while (true) {
				System.out.println("=========== " + c++);
				
				System.out.println("CPU:" + getter.getCPULoadPercentage());
				System.out.println("maxMem:" + getter.getTotalMemoryMb());
				System.out.println("Model:" + getter.getCPUModel());
				System.out.println("Speed:" + getter.getCPUSpeed());
				System.out.println("Domain:" + getter.getDomain());
				System.out.println("Subnet:" + getter.getSubnetMask());
				System.out.println("CoreCount:" + getter.getTotalCPUCoreCount());
				Thread.sleep(10 * 1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

    public static boolean isA() {
        String osName = System.getProperty("os.name");
        return osName.toUpperCase().contains("WINDOW");
    }


}
