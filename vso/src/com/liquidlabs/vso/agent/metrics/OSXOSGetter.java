package com.liquidlabs.vso.agent.metrics;

import com.liquidlabs.vso.agent.MBeanGetter;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class OSXOSGetter extends MBeanGetter implements OSGetter {

    private static final Logger LOGGER = Logger.getLogger(OSXOSGetter.class);

    String CPUModel = "unknown";
    String cpuSpeed = "unknown";
    String domain = "unknown";
    String subnetMask = "unknown";
    int totalCoreCount = -1;
    private CPUGetter cpuThing;
    private String gateway = "unknown";

    public OSXOSGetter() {
    }

    public static class CPUGetter implements Runnable {
        int val;
        Process process;
        public void run() {
            // - specify number of devices to be 0, and turn off load average
            ProcessBuilder processBuilder = new ProcessBuilder("/usr/sbin/iostat", "-n", "0", "-d", "-C", "60");
            try {
                process = processBuilder.start();
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String data = null;
                while((data = reader.readLine()) != null) {
                    parseCpuPercent(data.trim());
                }
            } catch (IOException e) {}
            if (process != null) {
                process.destroy();
            }
        }

        public int parseCpuPercent(String data) {
            if (!data.contains("cpu") && !data.contains("sy")) {
                String replaceAll = data.replaceAll("   ", " ");
                replaceAll = replaceAll.replaceAll("  ", " ");
                String[] split = replaceAll.split(" ");
                Double d = 100 - Double.valueOf(split[2]);
                d += 0.5;
                val = d.intValue();
                return val;
            }
            return 0;
        }

        public void stop() {
            if (process != null) process.destroy();
        }

    }

    public int getCPULoadPercentage() {
        if (cpuThing == null) {
            cpuThing = new CPUGetter();
            Thread cpu = new Thread(cpuThing);
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
            if (CPUModel.equals("unknown")) {
                CPUModel = getItemFromSystemProfiler(CPUModel,"Processor Name:");
            }
        } catch (Throwable t) {
            CPUModel = "unknown-0";
            LOGGER.warn(t.getMessage());
        }
        return CPUModel;
    }
    public String getGateway() {
        try {
            if (gateway.equals("unknown")) {
                String cmd ="/usr/sbin/netstat -nr";
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
                String hostname = readLine.split("\\.")[0];
                domain = readLine.substring(hostname.length()+1);
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
            if (subnetMask.equals("unknown")) {
                subnetMask = getItemFromSystemProfiler(subnetMask, "Subnet Mask:");
            }
        } catch (Throwable t) {
            subnetMask = "unknown-0";
            LOGGER.warn(t.getMessage());
        }
        return subnetMask;
    }

    public int getTotalCPUCoreCount() {
        try {
            if (totalCoreCount == -1) {
                String totalCoreCountS = getItemFromSystemProfiler("unknown" , "Total Number Of Cores:");
                totalCoreCount = Integer.parseInt(totalCoreCountS);
            }
        } catch (Throwable t) {
            totalCoreCount = 1;
            LOGGER.warn(t.getMessage());
            return 4;
        }
        return totalCoreCount;
    }
    public String getCPUSpeed() {
        try {
            if (cpuSpeed.equals("unknown")) {
                cpuSpeed = getItemFromSystemProfiler(cpuSpeed , "Processor Speed:");
                if (cpuSpeed.contains("GHz")) {
                    String value = cpuSpeed.replaceAll("GHz","");
                    Double valueOf = Double.valueOf(value);
                    cpuSpeed = Double.toString(valueOf * 1024);
                }
            }
        } catch (Throwable t) {
            cpuSpeed = "unknown-0";
            LOGGER.warn(t.getMessage());
        }
        return cpuSpeed;
    }

    private List<String> lines = new ArrayList<String>();

    private String getItemFromSystemProfiler(String existingValue, String key) throws IOException {
        if (!existingValue.equals("unknown")) return existingValue;

        if (lines.size() > 0) return scanLinesForKey(key);

        String cmd = "/usr/sbin/system_profiler SPHardwareDataType SPNetworkDataType";

        Process exec = exec(cmd.split(" "));
        BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (line.length() == 0) continue;
            lines.add(line.trim());
        }
        reader.close();
        closeProcess(exec);
        return scanLinesForKey(key);
    }

    private String scanLinesForKey(String key) {
        if (lines.size() > 0) {
            for (String line : lines) {
                if (line.contains(key)) {
                    return line.replaceAll(key, "").trim();
                }
            }
        }
        return "unknown";
    }
    private Process exec(String... args) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(args);
        builder.redirectErrorStream(true);
        Process p = builder.start();
        p.getOutputStream().flush();
        p.getOutputStream().close();

        return p;
    }
    public int getTotalMemoryMb() {
        return super.getPhysicalMemTotalMb();
    }
    public int getAvailMemMb() {
        return super.getPhysicalMemFreeMb();
    }

    public static void main(String[] args) {
        OSXOSGetter getter = new OSXOSGetter();
        while (true) {
            System.out.println("CPU:" + getter.getCPULoadPercentage());
            System.out.println("Model:" + getter.getCPUModel());
            System.out.println("Speed:" + getter.getCPUSpeed());
            System.out.println("Domain:" + getter.getDomain());
            System.out.println("Subnet:" + getter.getSubnetMask());
            System.out.println("CoreCount:" + getter.getTotalCPUCoreCount());
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
            }
        }
    }

    public static boolean isA() {
        String osName = System.getProperty("os.name");
        return osName.toUpperCase().contains("DARWIN") || osName.toUpperCase().contains("MAC OS X");
    }

}
