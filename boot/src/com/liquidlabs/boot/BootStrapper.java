package com.liquidlabs.boot;

import java.io.*;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BootStrapper {

    private static final String VMARGS = "vmargs";
    public static final String SYSPROPS = "sysprops";
    public static final String MANAGEMENT = "Management";
    public static final String RESOURCE_TYPE = "-Dvso.resource.type";
    public static final String RESOURCE_TYPE_1 = "-Dagent.role";
    public static final String FAILOVER = "Failover";
    public static final String DEPLOY = "deploy";
    public static final String DOWNLOADS = "downloads";
    public static final String BOOT_PROPERTIES = "boot.properties";
    public static final int MIN_DISK_REQUIRED = 500;
    private String zipDir;
    private String sysBundleDir;
    private BundleUnpacker unpacker = new BundleUnpacker();
    private Thread stdoutReaderThread;
    private Thread keepChildProcessIOHappyForWin32;

    public BootStrapper() {

    }
    public BootStrapper(String zipDir, String sysBundles) {
        this.zipDir = zipDir;
        this.sysBundleDir = sysBundles;
    }

    public static int getDiskLeftMB(String dir) {
        File file = new File(dir);
        if (!file.exists()) return  MIN_DISK_REQUIRED * 2;
        return (int) (file.getUsableSpace() / (1024 * 1024));
    }

    public boolean shouldRedeploy(String bundleDir, String sysBundleName) throws NoSuchAlgorithmException, IOException {
        try {
            if (!sysBundleName.endsWith(".zip")) sysBundleName += ".zip";
            File file = new File(zipDir, sysBundleName);
            String hash = extractHash(file);
            File hashFile = new File(new File(bundleDir, sysBundleName), "vs.hash");
            if (!hashFile.exists()) {
                return true;
            }

            FileInputStream hashInput = new FileInputStream(hashFile);
            byte [] bytes = new byte[hashInput.available()];
            hashInput.read(bytes, 0, hashInput.available());
            hashInput.close();
            String currentHash = new String(bytes, 0, bytes.length);
            return !hash.equals(currentHash);
        } catch (Throwable t) {
            t.printStackTrace();
            System.err.println("Cannot re-deploy:" + sysBundleName);
            return false;
        }
    }


    private String extractHash(File file) throws NoSuchAlgorithmException, IOException {
        return new HashGenerator().createHash(file.getName(), file);
    }

    public void initialize() throws NoSuchAlgorithmException, IOException {
        File[] systemBundles = findSystemBundles();
        if (systemBundles != null) {
            // unpack system bundles
            for (File bundle : systemBundles) {
                if (bundle.getName().startsWith(".")) continue;
                if (shouldRedeploy(sysBundleDir, getBundleName(bundle))) {
                    deleteDir(new File(sysBundleDir, getBundleName(bundle)));
                    unpacker.unpack(new File(sysBundleDir), bundle);
                }
            }
            // remove any bundles-dirs where the zip has been deleted
            List<File> systemBundleDirs = getSystemBundleDirs();
            for (File sysBundleDir : systemBundleDirs) {
                boolean found = false;
                for (File systemBundleZip : systemBundles) {
                    String sysBundleZipName = systemBundleZip.getName();
                    String sysBundleDirName = sysBundleDir.getName();
                    if (shouldKeepSystemBundleDir(sysBundleZipName,sysBundleDirName)) found = true;
                }
                if (!found) {
                    System.out.println("Deleting:" + sysBundleDir);
                    deleteDir(sysBundleDir);
                }
            }
        } else {
            System.out.println("ERROR = did not find any systembundles in:" + zipDir);
        }
//	f	File[] appBundles = findAppBundles();
//		if (appBundles != null) {
//			for (File bundle : systemBundles) {
//				if (shouldRedeploy(deployedBundles, getBundleName(bundle))) {
//					unpacker.unpack(new File(deployedBundles), bundle);
//				}
//			}
//		}

    }
    public boolean shouldKeepSystemBundleDir(String sysBundleZipName,
                                             String sysBundleDirName) {
        return sysBundleZipName.contains(sysBundleDirName);
    }

    private List<File> getSystemBundleDirs() {
        File[] listFiles = new File(sysBundleDir).listFiles();
        ArrayList<File> result = new ArrayList<File>();
        if (listFiles == null) return result;
        for (File file : listFiles) {
            if (file.isDirectory()) result.add(file);
        }
        return result;
    }
    private String getBundleName(File bundle) {
        String bundleName = bundle.getName();
        bundleName = bundleName.substring(0, bundleName.lastIndexOf("."));
        return bundleName;
    }

    private File [] findSystemBundles() {
        File file = new File(zipDir);
        return file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".zip") && unpacker.isSystemBundle(pathname);
            }});
    }

    private File [] findAppBundles() {
        File file = new File(zipDir);
        return file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".zip") &&  !unpacker.isSystemBundle(pathname);
            }});
    }
    private int deleteDir(File dir) {
        int count = 0;
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files!= null) {
                for (File file : files) {
                    if (file.isDirectory()) deleteDir(file);
                    else {
                        // file in use etc
                        try {
                            file.delete();
                            count++;
                        } catch (Throwable t) {}
                    }
                }
            }
            count++;
            dir.delete();
        }
        return count;
    }

    private Process process;
    public String pid;
    public boolean boot(String...args) throws Throwable {
        writeToStatus("BOOTSTRAPPER SETUP");


        if (getDiskLeftMB(".") < MIN_DISK_REQUIRED || getDiskLeftMB("work") < MIN_DISK_REQUIRED) {
            writeToStatus("BOOTSTRAPPER FATAL:InsufficientDiskSpace AvailMB:" + getDiskLeftMB("."));
            writeToStatus("BOOTSTRAPPER FATAL:InsufficientDiskSpace AvailMB:" + getDiskLeftMB("work"));

        }
        while (getDiskLeftMB(".") < MIN_DISK_REQUIRED || getDiskLeftMB("work") < MIN_DISK_REQUIRED) {
            Thread.sleep(60 * 1000);
        }
        writeToStatus("DiskLeft (mb): Home:" + getDiskLeftMB("work") + " Work:"+ getDiskLeftMB("work"));


        final Properties properties = getBootProperties(BOOT_PROPERTIES);
        initialize();
        ProcessBuilder pb = new ProcessBuilder();
        pb.redirectErrorStream(true);
        String javaCmd = getJavaCmd();

        File sysBundles = new File(sysBundleDir);

        StringBuilder builder = new StringBuilder("\"");

        builder.append(".").append(File.pathSeparator);

        configureClassPath(sysBundles, builder);
        List<String> command = new ArrayList<String>();
        command.add(javaCmd);
        String jolokia = agentProps(properties);
        if (jolokia != null) command.add(jolokia);
        addArgs(properties, command, VMARGS);
        command.add("-cp");
        command.add(builder.toString());
        addSystemProperties(command);
        addArgs(properties, command, SYSPROPS);
        addMissingProps(command);
        if (command.toString().contains("Forwarder")) {
            modifyMaxHeapForForwarder(command);
        }
        List<String> cmdLineArgs = new ArrayList<String>(Arrays.asList(args));
        for (Iterator<String> iterator = cmdLineArgs.iterator(); iterator.hasNext();) {
            String arg =  iterator.next();
            if (arg.startsWith("-D")) {
                command.add(arg);
                iterator.remove();
            }
        }

        command.add(properties.getProperty("mainclass"));
        command.addAll(cmdLineArgs);
        writeToStatus("Exec:" + cmdLineArgs);
        pb.command(command);
        Map<String, String> env = pb.environment();

        String javaHome = System.getProperty("java.home");
        env.put("JAVA_HOME", javaHome);
        System.out.println("Using JAVA_HOME:" + env.get("JAVA_HOME"));
        System.out.println("ProcessEnvironment:" + pb.environment());

        System.out.println(new Date().toString() + " Command:" + command);
        writeToStatus("BOOTSTRAPPER LOADING AGENT");
        process = pb.start();

        final BufferedReader stdOut = new BufferedReader(new InputStreamReader(process.getInputStream()));
        stdoutReaderThread = new Thread("StdOutReader"){
            public void run() {
                try {
                    while(!Thread.currentThread().isInterrupted()) {
                        try {
                            String readLine = stdOut.readLine();
                            if (readLine != null && readLine.trim().length() > 0) {
                                System.out.println(readLine);
                                if (readLine.contains("PID:") && readLine.contains("@")) {
                                    writeToStatus("GOT PID:" + readLine);
                                    pid = getPID(readLine);
                                    BootStrapper.this.renice(process, properties);
                                }
                            } else Thread.sleep(200);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (InterruptedException ex){}
                writeToStatus("BOOTSTRAPPER EXIT");
                System.out.println("StdOutReader Thread exiting");
            }

            private String getPID(String pidd) {
                return pidd.substring(pidd.indexOf("PID:") + "PID:".length(), pidd.indexOf("@"));
            }
        };

        stdoutReaderThread.setDaemon(true);
        stdoutReaderThread.start();


        outputStream = process.getOutputStream();
        keepChildProcessIOHappyForWin32 = new Thread("KeepChildHappy"){
            public void run() {
                try {
                    int pause = Integer.getInteger("child.write.interval.sec",1);
                    byte[] bytes = "-nada-\r\n".getBytes();
                    while (!Thread.currentThread().isInterrupted()){
                        Thread.sleep( pause * 1000);
                        outputStream.write(bytes);
                        outputStream.flush();
                    }
                    System.out.println("KeepChildHappy Thread exiting");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        keepChildProcessIOHappyForWin32.setDaemon(true);
        keepChildProcessIOHappyForWin32.start();

        writeToStatus("BOOTSTRAPPER RUNNING");
        int exitCode = process.waitFor();
        System.out.println("Exited, exitCode:" + exitCode);
        writeToStatus("Exited, exitCode:" + exitCode);
        Thread.sleep(1500);
        try {
            stdoutReaderThread.interrupt();
            keepChildProcessIOHappyForWin32.interrupt();
            System.out.println("Waiting for IO threads");
            try {
                stdoutReaderThread.join(60 * 1000);
            } catch (Throwable t) {
                t.printStackTrace();
            }
            try {
                keepChildProcessIOHappyForWin32.join(60 * 1000);
            } catch (Throwable t) {
                t.printStackTrace();
            }
            System.out.println("Done waiting for IO threads");
        } catch (Throwable t){
            t.printStackTrace();
        }

        // 0 is normal termination
        return (exitCode != 0);
    }

    private String agentProps(Properties properties) {
        int port = determinePort(Integer.parseInt(properties.getProperty("jolokia.port", "11006")),1);
        String joloAgent = properties.getProperty("jolokia.agent");
        if (joloAgent != null) {
            writeToStatus("Setting Jolokia Agent:" + joloAgent);
            return  joloAgent;
        }
        writeToStatus("Ignoring: Jolokia Agent not configured in boot.properties");
        return null;
    }
    public static int determinePort(int startingPort, int increment) {

        for (int result = startingPort; result < startingPort + 50000; result+=increment) {
            try {
                ServerSocket serverSocket = new ServerSocket(result);
                serverSocket.setReuseAddress(false);
                serverSocket.close();
                DatagramSocket datagramSocket = new DatagramSocket(result);
                datagramSocket.setReuseAddress(false);
                datagramSocket.close();
                return result;
            } catch (Throwable t) {
            }
        }
        throw new RuntimeException("Failed to determine port in range:" + startingPort + "-" + startingPort + 20000);
    }

    private void modifyMaxHeapForForwarder(List<String> command) {
        int foundMaxIndex = -1;
        int foundMinIndex = -1;
        int index =0 ;
        for (String s : command) {
            if (s.contains("-Xmx")) {
                foundMaxIndex = index;
            }
            if (s.contains("-Xms")) {
                foundMinIndex = index;
            }

            index++;
        }

        if (foundMaxIndex > 0) command.set(foundMaxIndex, "-Xmx" + Integer.getInteger("fwdr.mem", 128) + "M");
        if (foundMinIndex > 0) command.set(foundMinIndex, "-Xms" + Integer.getInteger("fwdr.mem", 128) + "M");
        writeToStatus("Overriding HEAP for ForwarderRole -Xmx:" + Integer.getInteger("fwdr.mem", 128) + "MB");

    }

    public void shutmeDown() {
        // Dont kill the child using process.destroy cause it doesnt call the shutdown hooks
        writeToStatus("BOOTSTRAPPER EXIT");
        System.out.println(new Date().toString() + " Bootstrapper: Destroying child process");
        try {
            outputStream.write("exit\r\n".getBytes());
            outputStream.flush();
            Thread.sleep(5000);
            System.out.println(new Date().toString() + " Bootstrapper: Destroying process");
//			process.destroy();
        } catch (Throwable e) {
        }
    }

    private boolean isWindows() {
        return System.getProperty("os.name").toUpperCase().contains("WINDOW");
    }
    protected void renice(Process process, Hashtable<Object, Object> properties) {
        String priority = (String) properties.get("priority");
        writeToStatus("PRIORITY:" + priority);
        // only apply renice when there is a LOW priority
        if (!"LOW".equalsIgnoreCase(priority)) return;

        writeToStatus("Waiting for PID");
        while (pid == null) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        writeToStatus("Set Priority of pid:" + pid);

        String cmd = winNiceCmd + pid;
        if (!isWindows()) {
            cmd = nixNiceCmd + pid;
        }
        String[] splitArgs = cmd.split(" ");
        try {
            ProcessBuilder builder = new ProcessBuilder(splitArgs);
            builder.redirectErrorStream(true);
            Process p = builder.start();
            InputStream is = p.getInputStream();
            int count = 0;
            boolean finished = false;
            while (!finished && count < 100) {
                try {
                    p.exitValue();
                    finished = true;
                    if (is.available() > 0) {
                        byte[] msg = new byte[is.available()];
                        is.read(msg);
                        writeToStatus("Renice:" + new String(msg));
                    }
                } catch (Throwable t) {
                    // exit value will blowup
                    count++;
                    Thread.currentThread().sleep(100);
                }
            }

            p.getOutputStream().flush();
            p.getOutputStream().close();
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
            writeToStatus("Failed to renice:" + e.toString());
        }
    }
    private void addSystemProperties(List<String> command) {
        Properties properties = System.getProperties();
        Set<Entry<Object,Object>> entrySet = properties.entrySet();
        for (Entry<Object, Object> entry : entrySet) {
            if (((String)entry.getKey()).startsWith("vscape")) {
                command.add(String.format("-D%s=%s",entry.getKey(), entry.getValue()));
            }
        }
    }

    private void addMissingProps(List<String> command) {
        boolean gotAgentProps = false;
        for (String arg : command) {
            if (arg.contains("-Dlog4j.configuration")) gotAgentProps = true;
        }
        if (!gotAgentProps) command.add("-Dlog4j.configuration=./agent-log4j.properties");
    }

    private void addArgs(Properties properties, List<String> command, String property) {
        String property2 = properties.getProperty(property, "");
        if (property.equals(VMARGS)) {
            property2 = removeUnwantedParams(property2);
        }
        String[] args = property2.replaceAll("\\s+", " ").split(" ");
        for (String arg : args) {
            command.add(arg);
        }
    }

    String getJavaCmd() {
        return System.getProperty("java.home") + "/bin/java";
    }

    private void configureClassPath(File sysBundles, StringBuilder cp) throws IOException {

        List<File> directories = loadDirectories(sysBundles);

        for (File directory : directories) {
            if (new File(directory,".excludeFromCP").exists()) continue;
            cp.append(File.pathSeparator).append(directory.getAbsolutePath());
            collectUrls(directory, cp);
        }
        cp.append(File.pathSeparator);
        cp.append("\"");
        System.out.println("CLASSPATH:" + cp);
    }


    private void collectUrls(File directory, StringBuilder cp) throws IOException {
        File[] files = directory.listFiles();
        if (files == null) {
            System.err.println("No files found in:" + directory.getAbsolutePath());
            return;
        }
        for (File file : files) {
            // use java 6 wildcarding so only add dirs
            if (file.isDirectory()) {
                collectUrls(file, cp);
            }
        }
        // add the directory
        if (files != null) {
            cp.append(File.pathSeparator).append(directory.getAbsolutePath());
            cp.append(File.separator).append("*");
        }
    }



    private List<File> loadDirectories(File sysBundles) {
        List<File> results = new ArrayList<File>();
        results.add(new File("lib"));
        File[] listFiles = sysBundles.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.isDirectory();
            }});
        if (listFiles != null) {
            for (File file : listFiles) {
                results.add(file);
            }
        }
        return results;
    }

    static BootStrapper bootStrapper = null;
    public static void main(String[] args) throws Throwable {
        new File("status.txt").delete();
        final File file = new File("agent.lock");
        if (!file.createNewFile()) {
            long systemUptime = getSystemUptime();
            long created = file.lastModified();
            if (created < System.currentTimeMillis() - systemUptime) {
                // ignore the agent.lock because it is old!
                writeToStatus("Found existing agent.lock but it looks created before system boot so ignoring.");
                file.delete();
                file.createNewFile();
            } else {
                System.err.println("It appears that an agent is already running. Please check and remove the agent.lock file if you sure it is not running");
                writeToStatus("It appears that an agent is already running. Please check and remove the agent.lock file if you sure it is not running");
                System.exit(1);
            }
        }
        file.deleteOnExit();
        writeToStatus("BOOTSTRAPPER START");
        writeToStatus("JVM:" + printJvm());
        Random random = new Random();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                writeToStatus("BOOTSTRAPPER ShutdownHookCalled");
                bootStrapper.shutmeDown();
            }
        });

        boolean reboot = false;
        int i = 0;
        do {

            boolean diskMessage = false;
            int deskLeftH = getDiskLeftMB(".");
            int deskLeftWD = getDiskLeftMB("work");

            while (deskLeftH > 0 && deskLeftH < MIN_DISK_REQUIRED || deskLeftWD > 0 && deskLeftWD < MIN_DISK_REQUIRED) {
                if (!diskMessage) {
                    writeToStatus("DISK FULL!, waiting to boot  LS_HOME:" + getDiskLeftMB(".") + "(MB) LS_HOME/work:" + getDiskLeftMB("work") + ("MB)"));
                    diskMessage = true;
                }
                deskLeftH = getDiskLeftMB(".");
                deskLeftWD = getDiskLeftMB("work");

                Thread.sleep(30 * 1000);
            }
            Properties properties = getBootProperties(BOOT_PROPERTIES);
            bootStrapper = new BootStrapper(properties.getProperty(DEPLOY, DOWNLOADS), properties.getProperty("vscape.system.bundles.dir","system-bundles"));
            writeToStatus("BOOTSTRAPPER REBOOT:" + i);
            System.out.println(new Date() + " ############################# REBOOTING COUNT = " + i++ + " ###########################");
            reboot = bootStrapper.boot(args);
            Thread.sleep(random.nextInt(10000));
        } while (reboot);
        System.out.println(new Date() + " ############################# EXIT CODE GIVEN - Not Rebooting ###########################");
    }

    public static long getSystemUptime() throws Exception {

        long uptime = -1;
        try {
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("win")) {

                uptime = doWindows(uptime);
            } else if (os.contains("mac") || os.contains("nix") || os.contains("nux") || os.contains("aix")) {
                // linux
                //  14:25:28 up 184 days, 46 min,  1 user,  load average: 1.05, 0.99, 0.96
                // mac
                // 14:25  up 2 days, 5 mins, 8 users, load averages: 4.52 4.34 3.99
                Process uptimeProc = Runtime.getRuntime().exec("uptime");
                BufferedReader in = new BufferedReader(new InputStreamReader(uptimeProc.getInputStream()));
                String line = in.readLine();
                if (line != null) {
                    //.*?((\d+) days,)?\s+((\d+) mins,)?\s+
                    //((\d+) days,)? (\d+):(\d+)
                    //                Pattern parse = Pattern.compile(".*?((\\d+) days,)?(\\s+(\\d+) hours,)?(\\s+(\\d+) mins,)?.*");
                    String pattern  = ".* up ((\\d+) days,)?( (\\d+) hours,)?( (\\d+) mins,)?.*";//.*?((\\d+) mins,)?";

                    Pattern parse = Pattern.compile(pattern);
                    Matcher matcher = parse.matcher(line);
                    if (matcher.matches()) {

                        /**
                         * GroupCount:6
                         0:)     14:25  up 2 days, 4 mins, 8 users, load averages: 5.82 4.52 4.04
                         1:)     2 days,
                         2:)     2
                         3:)     null
                         4:)     null
                         5:)      4 mins,
                         6:)     4
                         */
                        String _days = null;
                        String _hours = null;
                        String _minutes = null;

                        try {
                            _days = matcher.group(2);
                        } catch (Throwable t) {}
                        try {
                            _hours =  matcher.group(4);
                        } catch (Throwable t) {}
                        try {
                            _minutes = matcher.group(6);
                        } catch (Throwable t) {}

                        int days = _days != null ? Integer.parseInt(_days) : 0;
                        int hours = _hours != null ? Integer.parseInt(_hours) : 0;
                        int minutes = _minutes != null ? Integer.parseInt(_minutes) : 0;
                        uptime = (minutes * 60000) + (hours * 60000 * 60) + (days * 6000 * 60 * 24);
                    }
                }
            }
        } catch (Throwable t) {
            System.err.println("Failed to determine SystemUptime");
            t.printStackTrace();
        }
        return uptime;
    }

    private static long doWindows(long uptime) throws IOException, ParseException {
        Process uptimeProc = Runtime.getRuntime().exec("net stats srv");
        BufferedReader in = new BufferedReader(new InputStreamReader(uptimeProc.getInputStream()));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("Statistics since")) {
                Date boottime = extractDate(line);
                uptime = System.currentTimeMillis() - boottime.getTime();
                break;
            }
        }
        return uptime;
    }

    static Date extractDate(String line) throws ParseException {
        line = line.replace("Statistics since ","").trim();
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy hh:mm");
        return format.parse(line);
    }

    private static String printJvm() {
        String[] props =  { "java.runtime.version", "java.specification.name","java.specification.vendor","java.specification.version" };
        StringBuilder results = new StringBuilder();
        results.append("file.encoding:" + System.getProperty("file.encoding") + "\n");
        for (String prop : props) {
            results.append(System.getProperty(prop,"NOT_SPECIFIED"));
            results.append(", ");
        }
        return results.toString();

    }

    public static Properties getBootProperties(String bootProps) throws IOException {

        ClassLoader cl = new URLClassLoader(new URL[] { new File(".").toURL() }, ClassLoader.getSystemClassLoader() );

        InputStream reader = cl.getSystemResourceAsStream(bootProps);
        if (reader == null) reader = cl.getResourceAsStream(bootProps);
        if (reader == null) {
            String property = System.getProperty("java.class.path");
            String pp = property.replaceAll(";", "\n");
            System.out.println(pp);
            System.out.println("Failed to find boot.properties on classpath:" + property);
            System.err.println("Failed to find boot.properties on classpath:" + System.getProperty("java.class.path"));
            System.exit(-1);
        }
        Properties properties = new Properties();
        properties.load(reader);
        reader.close();

        return overrideWithAgentPropertiesIfExists(properties);
    }

    private static Properties overrideWithAgentPropertiesIfExists(Properties boot) throws IOException{
        final String type = getAgentType(boot);
        final String property = boot.getProperty("sharedConfig", DOWNLOADS);
        File[] files = new File(property).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String filename) {
                return filename.equalsIgnoreCase(type+".boot.properties");
            }
        });

        if (files == null) files = new File[0];

        if(files.length != 1) {
            System.out.printf("Couldn't find %s/%s.boot.properties - using default boot.properties\n", property, type);
            return boot;
        } else if (files.length == 1) {
            System.out.printf("Using %s/%s.boot.properties\n", property, type);
        }

        final Properties agentProperties = new Properties();
        final FileReader fileReader = new FileReader(files[0]);
        agentProperties.load(fileReader);
        fileReader.close();
        return agentProperties;
    }


    private static String getAgentType(Properties properties) {
        String sysProps = properties.getProperty(SYSPROPS);
        String [] props = sysProps.split("\\s+");
        for(String property : props) {
            final String[] prop = property.split("=");
            if (RESOURCE_TYPE.equals(prop[0]) && prop.length == 2) {
                return property.split("=")[1];
            }
            if (RESOURCE_TYPE_1.equals(prop[0]) && prop.length == 2) {
                return property.split("=")[1];
            }
        }
        return "agent";
    }
    private static String removeUnwantedParams(String sysProps) {
        // remove 64bit only params
        if (!isSixtyFourBit) {
            for (String arg : sixtyFourOnlyArgs) {
                sysProps = sysProps.replace(arg, "");
            }
        }
        return sysProps;
    }

    static Set<String> sixtyFourOnlyArgs = new HashSet<String>(Arrays.asList("-XX:+UseCompressedOops"));
    static boolean isSixtyFourBit = System.getProperty("sun.arch.data.model", "64").equals("64");

    public static void writeToStatus(String string) {
        String eoln = System.getProperty("line.separator");
        try {
            FileOutputStream fos = new FileOutputStream("status.txt", true);
            String statusMsg = new Date().toString();
            System.out.println(statusMsg + " " + string);
            fos.write((statusMsg + " " + string + eoln).getBytes());
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void kill() {
        process.destroy();
    }
    String nixNiceCmd = "scripts/renice.sh ";
    String winNiceCmd = String.format("%s\\cscript.exe scripts\\renice.vbs ", getSystem32());
    private OutputStream outputStream;
    public static String getSystem32() {
        return System.getProperty("win.sys32", "C:\\WINDOWS\\SYSTEM32");
    }




}
