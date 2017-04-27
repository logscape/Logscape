/**
 *
 */
package com.liquidlabs.vso.agent.process;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.container.SLAContainer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Runs child processes. Note the child environment will inherit the classpath from the parent, however child cp precedes it
 *
 */
public class ProcessMaker {
    private static final Logger LOGGER = Logger.getLogger(ProcessMaker.class);
    static int forkedHeap = Integer.getInteger("forked.heap.max.mb",64);
    Set<String> sixtyFourOnlyArgs = new HashSet<String>(Arrays.asList("-XX:+UseCompressedOops"));
    boolean isSixtyFourBit = System.getProperty("sun.arch.data.model", "64").equals("64");

    private final String workingDirectory;
    private ProcessBuilder processBuilder;
    private final Map<String, Object> variables;
    String javaHome = System.getProperty("java.home");
    String javaCommand = javaHome + "/bin/java";

    private String runDir;

    private File workingDir;

    public ProcessMaker(String workingDirectory, String runDir, boolean background, Map<String, Object> variables, boolean isRedirecting) {

        if (System.getProperty("os.name").toUpperCase().contains("WINDOWS")){
            javaCommand = javaHome.replace("\\","/") + "/bin/java.exe";
        }

        this.variables = variables;
        processBuilder = new ProcessBuilder();
        processBuilder.redirectErrorStream(isRedirecting);
        this.workingDirectory = getPath(new File(workingDirectory));
        this.runDir = getPath(new File(runDir));
        processBuilder.environment().put("appDir", this.workingDirectory);
        processBuilder.environment().put("workDir", this.workingDirectory);
        processBuilder.environment().put("rundir", this.runDir);


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("CWDDIR=" + new File(".").getAbsolutePath());
            LOGGER.debug("APPDIR=" + this.workingDirectory);
            LOGGER.debug("RUNDIR=" + this.runDir);
        }
        workingDir = null;

        if (!background) workingDir = new File(this.runDir);
        else workingDir = new File(this.workingDirectory);


        LOGGER.info("PROCESS WORKDIR=" + workingDir.getAbsolutePath());

        processBuilder.directory(workingDir);
        processBuilder.environment().put("vslib", FileUtil.cleanupPathAndMakeNative(new File("lib").getAbsolutePath()));

    }

    public void setEnvironmentVariable(String name, String value) {
        processBuilder.environment().put(name, value);
    }

    public void fork(String command, String mainClass, String...args) {
        List<String> arguments = new ArrayList<String>();
        String fullpath = getPath(new File("."));
        if (LOGGER.isDebugEnabled()) LOGGER.debug("ABSPATH:" + fullpath);
        if (LOGGER.isDebugEnabled()) LOGGER.debug("WORKINGDIR:" + this.workingDirectory);

        if (System.getProperty("os.name").toUpperCase().contains("WINDOWS")){
            fullpath = fullpath.replaceAll("\\/", "\\\\");
            if (command.endsWith(".bat") || command.endsWith("cmd")){
                arguments.add("cmd");
                arguments.add("/c");
            }
        }

        // TODO: Not sure about this - the command could be on the PATH
//		if (!command.startsWith("/")) {
//			command = fullpath + command;
//		}
        arguments.add(command);

        if (args !=null && args.length > 0) {
            for(String arg : args){
                if(!arg.trim().isEmpty())
                    arguments.add(arg.trim());
            }
        }

        String argumentsString = "";
        for (String string : arguments) {
            argumentsString += " '" + string + "'";
        }
        if (LOGGER.isDebugEnabled()) {
            System.out.println("From:" + this.workingDirectory + " RUNNING: mainClass:" + mainClass + "\n " + argumentsString);

        }
        if (LOGGER.isDebugEnabled()) {
            String string = arguments.toString().replaceAll(",", "\n\t");
            string = string.replaceAll(File.pathSeparator, "\n\t\t");

            LOGGER.info("RUNNING: mainClass:" + mainClass + "\n" + string);
        }
        processBuilder.command(arguments);
    }

    /**
     * .slaContainer "-cp:lib/*.jar:.:../lib/*.jar", "-Dlog4j.debug=false", "com.liquidlabs.flow.sla.FlowConsumer", "sla.xml", "WorkerTemplate"
     * @param consumerClass
     * @param slaString
     * @param classPath
     * @param arguments
     */
    public void runSLAContainer(String... args) {
        try {
            String classPath = getArg("-cp:", args, null);
            String serviceToRun = getArg("-serviceToRun:", args, null);

//			String lookupSpaceAddress = (String) this.variables.get("LookupSpaceAddress");

//			ArrayList<String> argsList = new ArrayList<String>();
//			argsList.add(SLAContainer.class.getName());
//			argsList.add("-lookup:" + lookupSpaceAddress);
//			argsList.add("-sla:" + getArg("-sla:", args, (String) variables.get("slaFilename")));
//			argsList.add("-consumerName:" + variables.get("consumerName"));
//			argsList.add("-serviceToRun:" + serviceToRun);
//			argsList.add("-workingDirectory:" + workingDirectory);
//			argsList.add("-bundleName:" + variables.get("bundleName"));
//			argsList.add("-agentAddress:" + variables.get("ResourceAgent"));
//			argsList.add("-resourceId:" + variables.get("resourceId"));
//			argsList.add("-workId:" + variables.get("workId"));
//
            String[] argss = getSLAArgs((String) variables.get("lookupSpaceAddress"),
                    getArg("-sla:", args, (String) variables.get("slaFilename")),
                    (String)variables.get("consumerName"), serviceToRun, workingDirectory, (String)variables.get("bundleName"),
                    (String)variables.get("resoureAgent"), (String)variables.get("resourceId"), (String)variables.get("workId"),
                    args);
            java("-cp:" + classPath, SLAContainer.class.getName(), argss);
        } catch (Throwable t){
            throw new RuntimeException("Boom on SLAContainer:" + t.getMessage(), t);
        }

    }

    public static String[] getSLAArgs(String lookup, String sla, String consumer, String serviceToRun, String workingDirectory,
                                      String bundleName, String resourceAgent, String resourceId, String workId, String... args) {
        ArrayList<String> argsList = new ArrayList<String>();
        argsList.add(SLAContainer.class.getName());
        argsList.add("-lookup:" + lookup);
        argsList.add("-sla:" + sla);//getArg("-sla:", args, (String) variables.get("slaFilename")));
        argsList.add("-consumerName:" + consumer);// variables.get("consumerName"));
        argsList.add("-serviceToRun:" + serviceToRun);
        argsList.add("-workingDirectory:" + workingDirectory);
        argsList.add("-bundleName:" + bundleName);
        argsList.add("-agentAddress:" + resourceAgent);
        argsList.add("-resourceId:" + resourceId);
        argsList.add("-workId:" + workId);
        for (String arg : args) {
//				if (arg.trim().startsWith("-D")) argsList.add(arg);
//				if (arg.trim().startsWith("-X"))
            argsList.add(arg);

        }
        return com.liquidlabs.common.collection.Arrays.toStringArray(argsList);
    }

    private String getArg(String key, String[] args, String defaultValue) {
        for (String arg : args) {
            if (arg.startsWith(key)) return arg.replace(key, "");
        }
        if (defaultValue != null) return defaultValue;
        throw new RuntimeException("Argument:" + key + " was not found in:" + Arrays.toString(args));
    }

    public void java(String classPath, String mainClass, String... args) {

        try {
            classPath = classPath.substring(classPath.indexOf(":")+1, classPath.length());
            String processedClasspath = processClassPath(classPath.split(":"));

            processedClasspath = File.pathSeparator + processedClasspath + File.pathSeparator + getFilteredClassPath(System.getProperty("java.class.path"), File.pathSeparator);
            Map<String, String> jvmArgs = new HashMap<String, String>();

            jvmArgs.putAll(loadJVMArgsFromBootProps());


            Properties properties = System.getProperties();
            for (Object key : properties.keySet()) {
                if (key.toString().startsWith("-D")) {
                    jvmArgs.put((String) key, (String)properties.get(key));
                }
            }

            List<String> progArgs = new ArrayList<String>(Arrays.asList(args));
            for (String arg : args) {
                arg = arg.trim();
                if (arg.startsWith("-D") || arg.startsWith("-X") || arg.startsWith("-agentlib") || arg.startsWith("-verbose") || arg.startsWith("-agentpath")) {

                    // only allow 64 bit args on 64 bit jvms
                    if (!isSixtyFourBit && sixtyFourOnlyArgs.contains(arg)) continue;

                    if (!arg.contains("agentlib") && !arg.startsWith("-X") && arg.contains("=")) {
                        String[] split = arg.split("=");
                        jvmArgs.put(split[0], split[1]);
                    } else {
                        jvmArgs.put(arg, "");
                    }
                    progArgs.remove(arg);
                }
            }

            ArrayList<String> argsList = new ArrayList<String>();
            argsList.add("-Xms32M");

            argsList.add("-Xmx" + forkedHeap + "M");
            argsList.add("-cp");
            argsList.add(processedClasspath);
            argsList.add("-DLogscape.MainClass=" + mainClass);
            argsList.addAll(toPropertyString(jvmArgs));
            argsList.add("-Dlookup.url="+ VSOProperties.getLookupAddress());
            argsList.add(mainClass);
            argsList.addAll(progArgs);
            fork(javaCommand, mainClass, com.liquidlabs.common.collection.Arrays.toStringArray(argsList));
        } catch (Throwable t) {
            String msg = String.format("Failed to start[%s] ex[%s]", mainClass, t.getMessage());
            LOGGER.error(msg, t);
            throw new RuntimeException(msg, t);
        }

    }

    String getFilteredClassPath(String classpath, String splitChar) {

        classpath = classpath.replace("\"","");

        String[] split = classpath.split(splitChar);

        List<String> resultsList = new ArrayList<String>();
        StringBuilder results = new StringBuilder();
        for (String pathPart : split) {
            if (pathPart.endsWith(".jar")) {
                pathPart = new File(pathPart).getParent() + "/*";
            }
            // strip off the working directory
//            if (pathPart.startsWith(workingDirectory)) pathPart = "." + pathPart.replace(workingDirectory, "");

            if (!resultsList.contains(pathPart)) {
                resultsList.add(pathPart);
                if (results.length() > 0) results.append(splitChar);
                results.append(pathPart);//.append("\n");
            }

        }

        return results.toString();
    }

    private List<String> toPropertyString(Map<String, String> jvmArgs) {
        List<String> results = new ArrayList<String>();
        for (String key : jvmArgs.keySet()) {
            String value = jvmArgs.get(key);
            if (value == null || value.length() == 0) {
                results.add(key);
            } else {
                results.add(String.format("%s=%s", key, value));
            }
        }
        return results;
    }

    private Map<String, String> loadJVMArgsFromBootProps() throws FileNotFoundException, IOException {
        Map<String, String> results = new HashMap<String,String>();

        try {
            FileInputStream reader = new FileInputStream("boot.properties");
            Properties bootProperties = new Properties();
            bootProperties.load(reader);
            reader.close();
            String sysProps = (String) bootProperties.get("sysprops");
            if (is32BitJvm()) {
                sysProps = sysProps.replace("-XX:+UseCompressedOops", "");
            }
            sysProps += " " + " -Dfile.encoding=" + System.getProperty("file.encoding");
            String bootPropsString[] = new String(sysProps).split("\\s+");
            for (String string : bootPropsString) {
                if (string.contains("-Dlog4j.configuration")) continue;
                if (string.contains("-Dcom.sun.management.jmxremote.port")) continue;
                if (string.contains("jolokia")) continue;
                if (string.contains("-Djava.io.tmpdir")) {
                    String[] split = string.split("=");
                    results.put(split[0], new File(split[1]).getAbsolutePath());
                    continue;
                }
                if (string.contains("=")) {
                    String[] split = string.split("=");
                    results.put(split[0], split[1]);
                } else {
                    results.put(string,"");
                }
            }
            results.put("-Dvscape.home", new File(".").getCanonicalPath());
            // stop child processes from being killed when you log out
            results.put("-Xrs", "");
        } catch (Exception e) {
            LOGGER.warn("Failed to load boot.properties for child process");
        }
        return results;
    }

    private static boolean is32BitJvm() {
        return System.getProperty("sun.arch.data.model","32").equals("32");
    }

    /**
     * Converts *\*.jar into correct classpath notation for java 5 etc
     */
    String processClassPath(String[] classPath) {
        return getJava6Classpath(classPath);
    }

    String getJava6Classpath(String[] classPath) {
        StringBuilder sb = new StringBuilder();
        for (String string : classPath) {
            sb.append(string.replaceAll(":", File.pathSeparator));
            sb.append(File.pathSeparator);
        }
        return sb.toString();
    }


    /**
     * converts single "../../lib/lib/*.jar" to all files"
     * @param cpItem
     * @return
     */
    List<String> lookupFileItems(String cpItem) {
        ArrayList<String> arrayList = new ArrayList<String>();
        int indexOfFileStar = cpItem.indexOf("*.");
        int indexOfDirStar = cpItem.indexOf("*/");
        if (indexOfDirStar > -1) {

            // locate the Dir and recurse
            String dirr = cpItem.substring(0, indexOfDirStar);
            File file = new File(dirr);
            String[] list = file.list();
            if (list == null) {
                LOGGER.warn("Cannot Locate DIR:" + dirr);
            } else {
                for (String string : list) {
                    if (new File(dirr + string).isDirectory()) {
                        String expandedPathItem = dirr+ string + "/" + cpItem.substring(indexOfDirStar+2, cpItem.length());
                        List<String> expandedDirContents = lookupFileItems(expandedPathItem);
                        arrayList.addAll(expandedDirContents);
                    }
                }
            }

        } else if (indexOfFileStar > -1) {
            // locate the Files and return values
            String dirr = cpItem.substring(0, indexOfFileStar);
            String suffix = cpItem.substring(cpItem.lastIndexOf("."), cpItem.length());
            File file = new File(dirr);
            String[] list = file.list();
            if (list == null) list = new String[0];
            for (String string : list) {
                File file2 = new File(dirr + string);
                if (file2.isDirectory()) continue;
                if (file2.getName().endsWith(suffix)) {
                    arrayList.add(getPath(file2));
                } else {
                }
            }

        } else {
            arrayList.add(getPath(new File(cpItem)));
        }
        return arrayList;
    }

    public Process fork() {
        try {
            if (LOGGER.isDebugEnabled()) LOGGER.info("Forking:"  + processBuilder.command());
            if (processBuilder.command().size() == 0) {
                throw new RuntimeException("Cannot fork process, no command specified - script forking using [processMaker.java \"-cp:lib/*.jar:.\", \"-Dlog4j.debug=false\", \"com.liquidlabs.YourMainClassHere\"], \"arg0\"");
            }
            return processBuilder.start();
        } catch (Throwable e) {
            LOGGER.error(String.format("Process did not start, command%s", processBuilder.command()), e);
            throw new RuntimeException("Process did not start:" + processBuilder.command() + " ex:" + e.getMessage(), e);
        }
    }

    public String getPath(File fullPath) {
        return FileUtil.cleanupPathAndMakeNative(fullPath.getAbsolutePath());
    }
}