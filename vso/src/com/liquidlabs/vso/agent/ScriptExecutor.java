package com.liquidlabs.vso.agent;

import com.liquidlabs.common.*;
import com.liquidlabs.common.RedirectingFileOutputStream.FilenameGetter;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.process.ProcessUtils;
import com.liquidlabs.vso.deployment.ScriptForker;
import com.liquidlabs.vso.work.WorkAssignment;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.json.JSONObject;

import javax.management.remote.JMXConnectorFactory;
import javax.naming.Context;
import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class ScriptExecutor implements LifeCycle {
    private static boolean updateAlways = Boolean.parseBoolean(System.getProperty("vs.update.always", "false"));
    private ResourceAgent agent;
    private String deployBundlesDir;
    private ScriptForker runner;
    private static final Logger LOGGER = Logger.getLogger(ScriptExecutor.class);
    private static final String DELIMITER = VSOProperties.getLogScriptDelim();
    private final ScheduledExecutorService scheduler;
    private Future<?> future;
    private WorkAssignment workAssignment;
    private EmbeddedServiceManager esm;
    private ScheduledFuture<?> restartableFuture;

    String lastInfoString = "";
    String lastRunString = "never";
    String lastError = "";
    private Process p;
    private static boolean isDebugMode = System.getProperty("script.debug","false").equals("true");
    private int failureCount;

    public ScriptExecutor(java.util.concurrent.ExecutorService executor, ResourceAgent agent, String deployBundlesDir, ScheduledExecutorService scheduler) {

        this.agent = agent;
        this.deployBundlesDir = deployBundlesDir;
        this.scheduler = scheduler;
        this.runner = new ScriptForker();
    }
    public static boolean isNonGroovyScript(String script) {
        return !script.contains(".groovy") &&  script.contains(".vbs") || script.contains(".sh") || script.contains(".bat");
    }

    public Future<?> execute(final WorkAssignment workAssignment, final HashMap<String, Object> variables, final String workId2) {
        LOGGER.info("Starting Task:" + workAssignment.getId());
        this.workAssignment = workAssignment;

        this.esm = new EmbeddedServiceManager(workId2);
        variables.put("serviceManager", this.esm);

        final String hostname = NetworkUtils.getHostname();
        final boolean isHostname = workAssignment.getServiceName().contains("wHOST");
        final boolean isTimestamp = workAssignment.getServiceName().contains("wTSTAMP");
        final String fullBundleDir = deployBundlesDir + "/" + workAssignment.getBundleId();
        ScriptRunnable task = null;
        try {

            final String script = workAssignment.getScript().trim();
            if (script.contains(".vbs")) {

                if (isDebugMode) LOGGER.info(".vbs script");

                String[] varArgs = convertVarsToCmdLine(variables);

                String bundleDir = workAssignment.isSystemService() ? "system-bundles" : deployBundlesDir;
                LOGGER.info("VBS RUN PWD:" + new File(".").getAbsolutePath());
                String scriptLocal = script;
                if (script.contains(".vbs ")) {
                    scriptLocal = scriptLocal.replace(" ","_SPLITIT_");
                }
                final String cmdSys = String.format("cscript.exe_SPLITIT_//nologo_SPLITIT_%s\\%s\\%s", bundleDir,workAssignment.getBundleId(), scriptLocal);
                final String args[] = Arrays.append(cmdSys.split("_SPLITIT_"), varArgs);


                Runnable command = new Runnable() {
                    public void run() {
                        runScript(args, workAssignment, isTimestamp, isHostname, hostname, variables);
                    }
                };
                // Need to dump the
                task = submit(workAssignment, command, this.esm);

            } else if (script.endsWith(".sh") || script.endsWith(".bat")) {
                task = runProcessForkedScript(workAssignment, variables, hostname, isHostname, isTimestamp, script, convertVarsToCmdLine(variables));

            } else if (script.endsWith(".groovy") || script.contains(".groovy ")) {

                String scriptName;
                if (workAssignment.isSystemService()) {
                    String bundleDir = workAssignment.isSystemService() ? "system-bundles" : deployBundlesDir;
                    scriptName = String.format("%s/%s/%s", bundleDir,workAssignment.getBundleId(), script);
                } else {
                    scriptName = fullBundleDir + "/" + script;
                }
                final String params;
                if (scriptName.contains(".groovy ")) {
                    String[] split = StringUtil.split(".groovy ", script);
                    params = split[1];
                    scriptName = fullBundleDir + "/" + split[0] + ".groovy";
                } else {
                    params = "";
                }
                LOGGER.info("Groovy Schedule:" + new File(scriptName).getAbsolutePath());
                if (FileUtil.readAsString(scriptName) == null) {
                    String msg = "Failed to find Script:" + new File(scriptName).getAbsolutePath();
                    LOGGER.error(msg);
                    throw new RuntimeException(msg);
                } else {
                    final String scriptNameFinal = scriptName;
                    Runnable command = new Runnable() {
                        String script = null;
                        long lastMod = 0;
                        public void run() {
                            File file = new File(scriptNameFinal);
                            if (script == null || file.lastModified() != lastMod) {
                                LOGGER.info("Script:" + scriptNameFinal + " modified - reloading" + " id:" + workAssignment.getId() + " myLastMod:" + lastMod + " actual:" + file.lastModified());
                                lastMod = file.lastModified();
                                script = FileUtil.readAsString(scriptNameFinal);
                            }
                            runGroovyScript(scriptNameFinal + lastMod, workAssignment, variables, script, params, fullBundleDir);
                        }
                    };
                    task = submit(workAssignment, command, this.esm);
                }
            } else if (script.startsWith("jmx:")) {
                Runnable command = new Runnable() {
                    public void run() {
                        runJMXScript(workAssignment, variables, script, fullBundleDir);
                    }
                };
                task = submit(workAssignment, command, this.esm);
            } else if (isEmbeddedGroovyScript(script)) {
                final String scriptId = workAssignment.getServiceName();// + System.currentTimeMillis();
                // Its just a normal GroovyScript...
                variables.put("exceptionHandler", new StatusUpdatingExceptionHandler(agent, workAssignment));
//	                    runner.runString(workAssignment.getScript(), variables, getClassLoader(fullBundleDir, workAssignment.isSystemService()));
                Runnable command = new Runnable() {
                    public void run() {
                        runGroovyScript(scriptId, workAssignment, variables, script, "", fullBundleDir);
                    }
                };
                task = submit(workAssignment, command, this.esm);
            } else {

                // default to running a forked script process
                task = runProcessForkedScript(workAssignment, variables, hostname, isHostname, isTimestamp, script, convertVarsToCmdLine(variables));
            }
            pauseForScript(workAssignment);
            // all is good
            if (task != null) {
                if (!task.isError()) {
                    LOGGER.info("Setting TASK To be running:" + workAssignment.getId());
                    updateWorkAssignmentStatus(workAssignment, workAssignment.getId(),null, LifeCycle.State.RUNNING,"");
                } else {
//                    if (dumpToStdOut) {
//                        System.out.println("Error:" + task.isError());
//                        System.out.println("ErrorMsg:" + task.esm.getErrorMsg());
//                    }
                    LOGGER.warn("Task has an errmsg:" + workAssignment.getId());
                }
            } else {
                // should never 'appen
                LOGGER.warn("No Task found:" + workAssignment.getId());
            }
        } catch (Throwable t) {
            try {
                LOGGER.warn("Work Failed:" + workAssignment,t);
                Thread.sleep(VSOProperties.getProcessStartingWaitMs());
            } catch (InterruptedException e) {
            }
            esm.stop();
            updateWorkAssignmentStatus(workAssignment, workId2, t, LifeCycle.State.ERROR,"");
        }

        return future;
    }

    private boolean isEmbeddedGroovyScript(String script) {
        return script.contains("<![CDATA[") || script.contains("import") || script.contains("SLAContainer") || script.contains("logger");
    }

    private ScriptRunnable runProcessForkedScript(final WorkAssignment workAssignment, final HashMap<String, Object> variables, final String hostname, final boolean fff, final boolean timestamp, String script, String[] varArgs) {
        ScriptRunnable task;
        LOGGER.info("runProcessForkedScript PWD:" + new File(".").getAbsolutePath());

        String bundleDir = workAssignment.isSystemService() ? "system-bundles" : deployBundlesDir;
        final String[] processArgs = getProcessArgs(bundleDir, workAssignment.getBundleId(), script, varArgs);

        Runnable command = new Runnable() {
            public void run() {
                runScript(processArgs, workAssignment, timestamp, fff, hostname, variables);
            }
        };
        task = submit(workAssignment, command, this.esm);
        return task;
    }
    public static String[] getProcessArgs(String bundleDir, String bundleId, String script, String[] varArgs) {
        List<String> results = new ArrayList<String>();
        String[] scriptWithParams = script.split(" ");
        results.add(String.format("%s/%s/%s", bundleDir, bundleId, scriptWithParams[0]));
        results.addAll(Arrays.asList(scriptWithParams).subList(1, scriptWithParams.length));
       // results.addAll(Arrays.asList(varArgs));

        return results.toArray(new String[0]);
    }

    private String[] convertVarsToCmdLine(Map<String, Object> variables) {
        ArrayList<String> argss = new ArrayList<String>();
        ArrayList<String> keys = new ArrayList<String>(variables.keySet());
        Collections.sort(keys);
        for (String key :  keys) {
            try {
                argss.add(String.format("%s=%s", key, variables.get(key).toString()));
            } catch (Exception ex) {
                //LOGGER.warn("Failed to setVar:" + key, ex);
            }
        }
        return argss.toArray(new String[0]);
    }


    private void pauseForScript(WorkAssignment workAssignment) {
        try {
            if ( workAssignment.isSystemService()) {
                Thread.sleep(1000);
            } else {
                Thread.sleep(VSOProperties.getProcessStartingWaitMs());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class ScriptRunnable implements Runnable {
        private Runnable task;
        AtomicBoolean isError = new AtomicBoolean(false);
        private WorkAssignment work;
        private final EmbeddedServiceManager esm;

        public ScriptRunnable(Runnable task, WorkAssignment work, final EmbeddedServiceManager esm) {
            this.task = task;
            this.work = work;
            this.esm = esm;
        }

        public void run() {
            try {
                Thread.sleep((long) (VSOProperties.getTaskNoiseSecs() * 1000l * Math.random()));

                lastRunString = DateUtil.shortDateTimeFormat7.print(System.currentTimeMillis());
                task.run();
                lastRunString = DateUtil.shortDateTimeFormat7.print(System.currentTimeMillis()) + " F:" + task;
                // Flip from ERROR to RUNNING
                if (isError.get() == true || updateAlways) {
                    isError.set(false);
                    updateWorkAssignmentStatus(work, work.getId(),null, LifeCycle.State.RUNNING, isError.get() ? "Recovered" : "");
                }
            } catch (Throwable t) {
                lastError = lastRunString + " @ " + t.toString();
                // do we really want to stop executing?
                // Flip from RUNNING to ERROR
//				if (!isError.get()) {
                isError.set(true);
                updateWorkAssignmentStatus(work, work.getId(), t, LifeCycle.State.ERROR,"");
//				}
//				updateWorkAssignmentStatus(work, work.getId(), t, LifeCycle.State.ERROR);
            }
        }
        public boolean isError() {
            return isError.get();
        }
    }

    private ScriptRunnable submit(final WorkAssignment workAssignment, Runnable command, final EmbeddedServiceManager esm) {
        final ScriptRunnable task = new ScriptRunnable(command, workAssignment, esm);
        if (!workAssignment.isSystemService() && workAssignment.getPauseSeconds() > 0) {
            int secondOfMinute = new DateTime().getSecondOfMinute();
            int secondOfHour = new DateTime().getMinuteOfHour() * 60 + secondOfMinute;
            int startOffSet = getStartOffsetSecs(workAssignment.getPauseSeconds(), secondOfHour);
            LOGGER.info("LS_EVENT:ScriptStart in :" + startOffSet + " " + workAssignment);
            future = scheduler.scheduleAtFixedRate(task, startOffSet, workAssignment.getPauseSeconds(), TimeUnit.SECONDS);
        }
        else {
            future = scheduler.submit(task);
            if (!workAssignment.isSystemService()) {
                Runnable restartable = new Runnable() {
                    public void run() {
                        if (future.isDone() && !future.isCancelled()) {
                            LOGGER.warn("Restarting Service:" + workAssignment);
                            esm.stop();
                            future = scheduler.submit(task);
                        }
                    }
                };
                restartableFuture = scheduler.scheduleAtFixedRate(restartable, 5, 5, TimeUnit.MINUTES);
            }
        }
        return task;
    }

    /**
     * Convert to mod/secs of hour so 5 min interval first on the 5 min period of the hour
     * @param pauseSeconds
     * @return
     */
    int getStartOffsetSecs(int pauseSeconds, int currentSecond) {
        if (pauseSeconds <= 60 * 60) {
            int value = (currentSecond / pauseSeconds) * pauseSeconds;
            return  value + pauseSeconds - currentSecond;

        } else {
            return 60 -  currentSecond;
        }
    }

    private void runScript(final String[] cmdSys, final WorkAssignment work, boolean timeStampIt, boolean insertHostname, String hostname, Map<String, Object> variables) throws RuntimeException {

        if (failureCount > 50) {
            LOGGER.warn("DISABLED_TASK:" + work.getId() + ", Too many fails:" + failureCount);
            return;
        }

        int pid = 0;
        ProcessUtils processUtils = new ProcessUtils();
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(">> Running:" + work.getId());
                LOGGER.debug(work.getId() + " >> Script:" + Arrays.toString(cmdSys));
            }
            if (isDebugMode) {
                dumpErrMsg("Starting Process:" + work.getId());
            }


            // Get the OS first in case it cannot be handled/opened
            OutputStream fos = TryAndOpenOutputFile(work);

            ProcessBuilder builder = new ProcessBuilder(cmdSys);

            if (Boolean.getBoolean("script.environment.vars.enabled")) {
                Map<String, String> environment = builder.environment();
                for (String key : variables.keySet()) {
                    try {
                        environment.put(key, variables.get(key).toString());
                    } catch (Throwable t){
                    }
                }
            }
            String bundleDir = workAssignment.isSystemService() ? "system-bundles" : deployBundlesDir;
            builder.directory(new File(bundleDir + "/" + workAssignment.getBundleId()));

            if (isDebugMode) {
                dumpErrMsg("RunService:" + work.getId());
                dumpErrMsg("WorkingDir:" + builder.directory());
                List<String> strings = Arrays.asList(cmdSys);
                Collections.sort(strings);
                dumpErrMsg("CMD:" + cmdSys[0]);
                dumpErrMsg("Args:" + strings);

            }
            builder.redirectErrorStream(false);

            p = builder.start();
            pid = processUtils.convertProcess(p).pid();
            if (isDebugMode) dumpErrMsg("PID:" + pid);

            processUtils.writePid(pid, makeJson(work, cmdSys));

            p.getOutputStream().flush();
            p.getOutputStream().close();


            String line = "";
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader errreader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            boolean finished = false;
            int exitValue = 999;
            String errs = "";
            try {

                while (!finished) {
                    try {
                        exitValue = p.exitValue();
                        finished = true;
                        if (isDebugMode) dumpErrMsg("Finished ExitCode:"  +exitValue);
                    } catch (Throwable t) {
                        Thread.sleep(1000);
                        dumpStdOut(work.getId(), timeStampIt, insertHostname, hostname, DateUtil.shortDateTimeFormat4.print(DateTimeUtils.currentTimeMillis()), p, fos, reader);
                        if (p.getErrorStream().available() > 0) {
                            if ((line = errreader.readLine()) != null) {
                                errs += line;
                            }
                        }

                    }
                }
            } finally {
                if (!finished) {
                    dumpErrMsg("Killing:"  +work + " PID:" + pid);
                    LOGGER.info("Killing:" + work + " PID:" + pid);
                    p.destroy();
                }
            }
            processUtils.deletePid(pid);
            errreader.close();
            reader.close();
            fos.close();
            if (exitValue != 0) {
                String scriptmsg = "Script:" + Arrays.toString(cmdSys);

                String msg = scriptmsg + "\n\tFailed:" + errs + "\n\tExitValue:" + exitValue;
                dumpErrMsg(msg);
                throw new RuntimeException(msg);
            }

            if (LOGGER.isDebugEnabled()) LOGGER.debug("<< Running:" + work.getId());
        } catch (Throwable t) {
            dumpErrMsg("Process Failed (check boot.log for full StackTrace):" + t);
            if (isDebugMode) {
                t.printStackTrace();
            }

            if (t instanceof NullPointerException) {
                NullPointerException npe = (NullPointerException) t;
                LOGGER.warn("NullPtr:" + workAssignment, npe);
            }

            throw new RuntimeException(t);
        } finally {
            processUtils.deletePid(pid);
            try {
                // always destroy the process if we barf
                p.destroy();
            } catch (Throwable t2) {}

        }
    }

    private String makeJson(WorkAssignment work, String[] cmdSys) {
        try {
            JSONObject js = new JSONObject();
            js.put("bundle", work.getBundleId());
            js.put("service", work.getServiceName());
            js.put("date", DateUtil.shortDateFormat.print(System.currentTimeMillis()));
            js.put("timestamp", DateUtil.shortDateTimeFormat2.print(System.currentTimeMillis()));
            js.put("cmd:", Arrays.asList(cmdSys).toString());
            return js.toString();
        } catch (Throwable t) {
            return "";
        }
    }

    private boolean isDebuggingFlagTrue(Map<String, String> environment) {
        boolean debugging = false;
        if (isDebugMode) return true;
        if (environment.get("debug") != null) {
            if (environment.get("debug").contains("true")) {
                debugging = true;

            }
        }
        return debugging;
    }

    /**
     * Needed because on Window the output file can sometimes be locked - so we try and try again... failing the second time
     * @param work
     * @return
     * @throws FileNotFoundException
     */
    private BufferedOutputStream TryAndOpenOutputFile(final WorkAssignment work) throws FileNotFoundException {
        try {
            return new BufferedOutputStream(new FileOutputStream(getOutFile(work), true));
        } catch (Throwable t) {
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) { }
            return new BufferedOutputStream(new FileOutputStream(getOutFile(work), true));
        }
    }

    public boolean dumpToStdOut = false;
    public OutEvents outputListener = null;
    public static interface OutEvents {
        void print(String service, String output);
        void flush();
    }

    private void dumpErrMsg(String msg) {
        try {
            if (isDebugMode || dumpToStdOut) {
                System.out.println(new Date() + " " + msg);
                //return;
            }
            FileOutputStream ferr = new FileOutputStream(getErrOutFile(workAssignment), true);
            ferr.write((new Date() + " " + msg + " failureCount:" + failureCount++ + "\n").getBytes());
            ferr.close();
        } catch (Throwable t) {}
    }

    private void dumpStdOut(String service, boolean timeStampIt, boolean insertHostname, String hostname, String timestamp, Process p, OutputStream fos, BufferedReader reader) throws IOException {
        String line;
        if (p.getInputStream().available() > 0) {
            while ((line = reader.readLine()) != null) {
                if (insertHostname) line = hostname + DELIMITER + line;
                if (timeStampIt) line = timestamp + DELIMITER + line;
                if (isDebugMode || dumpToStdOut) {
                    System.out.println(line + "\n");
                } else {
                    fos.write((line + "\n").getBytes());
                }
                if (outputListener != null) {
                    outputListener.print(service, line);
                }
            }
            if (outputListener != null) outputListener.flush();
            fos.flush();
        }
    }
    private void runGroovyScript(final String scriptName, final WorkAssignment workAssignment, final HashMap<String, Object> variables, final String script, String params, String fullBundleDir) {

        if (isDebugMode) LOGGER.info("runGroovyScript:" + script);
        if (LOGGER.isDebugEnabled()) LOGGER.debug(">> Running:" + workAssignment.getId());
        OutputStream fout = null;
        OutputStream ferr = null;
        try {
            variables.put("args" , "");
            if (dumpToStdOut) {
                fout = System.out;
            } else {
                fout = new PrintStream(new RedirectingFileOutputStream(new FilenameGetter(){
                    public String getFilename() {
                        return getOutFile(workAssignment);
                    }
                }));
            }
            variables.put("pout", fout);

            if (dumpToStdOut) {
                ferr = System.err;
            } else {
                ferr = new PrintStream(new RedirectingFileOutputStream(new FilenameGetter(){
                    public String getFilename() {
                        return getErrOutFile(workAssignment);
                    }
                }));
            }
            variables.put("perr", ferr);

            Object runString = runner.runString(script, params, variables, getClass().getClassLoader(), workAssignment.getId()+ scriptName);
            // a nasty side affect of the groovy script is that the last value is always printed.
            // so if you def an array - then that is what is printed - unless you have a return value
//				String result = runString != null ? runString.toString() : "";
//				if (LOGGER.isDebugEnabled()) LOGGER.debug("<< Running:" + workAssignment.getId());
//				if (result.length() > 0) {
//					fout.write(result.getBytes());
//					fout.flush();
//				}
        } catch (Throwable e) {
            LOGGER.error("Script Output Error:" + scriptName, e);
            try {
                ferr.write((new Date() + " " + workAssignment + " error:" + e.toString()+ "\n").getBytes());
                String exceptionstring = ExceptionUtil.stringFromStack(e, 4096);
                ferr.write((new Date() + " " + exceptionstring + "\n").getBytes());
                ferr.flush();
                ferr.close();
                fout.close();
            } catch (Throwable t) {
                System.out.println("Error:" + scriptName + " ex:" + e.toString());
                t.printStackTrace();
            }
            throw new RuntimeException("Script Output Error:" + scriptName, e);
        } finally {
            try {
                if(fout != null) fout.close();
                if (ferr != null) ferr.close();
            } catch (IOException e) {}
        }
    }
    protected void runJMXScript(WorkAssignment work, HashMap<String, Object> variables, String script, String fullBundleDir) {

        RedirectingFileOutputStream os = null;
        try {
            LOGGER.info("runJMXScript script:" + script);
            final String outFile = getOutFile(workAssignment);

            if (LOGGER.isDebugEnabled()) LOGGER.debug(">> Running:" + work.getId() + " out:" + outFile);

            os = new RedirectingFileOutputStream(new FilenameGetter(){
                public String getFilename() {
                    return outFile;
                }
            });
            //jmxUrl=service:jmx:rmi://localhost:3000/jndi/rmi://localhost:9000/server
            // script=jmx:
            /**
             * def h = new Hashtable()
             h.put(Context.SECURITY_PRINCIPAL, username)
             h.put(Context.SECURITY_CREDENTIALS, password)
             h.put(JMXConnectorFactory.PROTOCOL_PROVIDER_PACKAGES, "weblogic.management.remote")

             def connector = JMXConnectorFactory.connect(new JMXServiceURL(jmxUrl), h)

             */
            script = script.substring("jmx:".length());
            Map<String, String> properties = new HashMap<String, String>();
            // username
            if (variables.containsKey("SECURITY_PRINCIPAL")) properties.put(Context.SECURITY_PRINCIPAL, (String) variables.get("SECURITY_PRINCIPAL"));
            // pwd
            if (variables.containsKey("SECURITY_CREDENTIALS")) properties.put(Context.SECURITY_CREDENTIALS, (String) variables.get("SECURITY_CREDENTIALS"));

            if (variables.containsKey("PROTOCOL_PROVIDER_PACKAGES")) properties.put(JMXConnectorFactory.PROTOCOL_PROVIDER_PACKAGES, (String) variables.get("PROTOCOL_PROVIDER_PACKAGES"));


            JmxScriptRunner jmxScriptRunner = new JmxScriptRunner(os,false);
            jmxScriptRunner.runJmxQueries(script.split("\n"), (String) variables.get("jmxUrl"), properties);
            lastInfoString = jmxScriptRunner.processed.toString();
            if (jmxScriptRunner.t != null) throw jmxScriptRunner.t;
            if (LOGGER.isDebugEnabled()) LOGGER.debug("<< Running:" + work.getId() + " processed:" + jmxScriptRunner.processed + " " + " b:" + os.toString());

        } catch (Throwable e) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug(">> FailedRunning:" + work.getId());
            lastError = new Date().toString() + " ex:" + e.toString();
            throw new RuntimeException(e);
        } finally {
            try {
                if (os != null) os.close();
            } catch (Throwable t){}
        }
    }

    boolean loggedError = false;
    private void updateWorkAssignmentStatus( final WorkAssignment workAssignment, final String workId2, Throwable t, LifeCycle.State state, String msg) {
        try {

            Thread.sleep(100);
            if (state == LifeCycle.State.ERROR) {
                if (!loggedError) LOGGER.error("1WorkError:" + workId2 + " Error with Script:" + workAssignment.getScript(), t);
                else  LOGGER.error("2WorkError:" + workId2 + " Error with Script:" + workAssignment.getScript() + " ex:" + t.toString());
                loggedError = true;
                if (t.getCause() != null) t = t.getCause();
                agent.updateStatus(workAssignment.getId(), state, new Date() + " " + t.toString() + "\n" + ExceptionUtil.stringFromStack(t, 4096));
            } else {
                agent.updateStatus(workAssignment.getId(), state, msg);
            }
        } catch (Exception e) {
            LOGGER.error(format("3WorkError: %sFailed to Update WorkAssignment to error status. Script %s", workId2, workAssignment.getScript()), e);
        }
    }

    public void start() {
    }
    public void stop() {
        LOGGER.info("Stopping:" + this.workAssignment);
        this.esm.stop();
        if (this.p != null) {
            int pid = new ProcessUtils().convertProcess(p).pid();
            LOGGER.info("Stopping: Process:" + p + " pid:" + pid);
            try {
                p.destroy();
            } catch (Throwable t) {
                LOGGER.info(t);
            }
        }
        if (restartableFuture != null) restartableFuture.cancel(true);
        if (future != null && !future.isDone()) {
            boolean success =  future.cancel(true);
            if (!success) LOGGER.warn("Failed to cancel task:" + future + " work:" + workAssignment);
        }
    }


    private String getOutFile(final WorkAssignment workAssignment) {
        String result = getWorkOut(workAssignment, "out");
        return result;
    }
    private String getErrOutFile(final WorkAssignment workAssignment) {
        String result = getWorkOut(workAssignment, "err");
        return result;
    }

    private String getWorkOut(final WorkAssignment workAssignment, String extension) {
        String dateDir = DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
        String outdir = "work/" + workAssignment.getBundleId() + "/" + dateDir + "/";
        String result = outdir + "/"  + workAssignment.getServiceName() + "." + extension;
        new File(result).getParentFile().mkdirs();
        return result;
    }
    public String toString() {
        return super.toString() + " Work:" + this.workAssignment.getId() + "  LastRun:" + this.lastRunString + " Info:" + lastInfoString + " LastError:" + this.lastError;
    }
}
