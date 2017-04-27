package com.liquidlabs.vso.agent;

import com.liquidlabs.common.*;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.ClientLeaseManager;
import com.liquidlabs.space.lease.LeaseManagerImpl;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.metrics.OSGetter;
import com.liquidlabs.vso.agent.outtage.DetectorsBuilder;
import com.liquidlabs.vso.agent.process.ProcessHandler;
import com.liquidlabs.vso.agent.process.ProcessListener;
import com.liquidlabs.vso.deployment.DeploymentService;
import com.liquidlabs.vso.deployment.ScriptForker;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.BundleSerializer;
import com.liquidlabs.vso.deployment.bundle.BundleUnpacker;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;
import com.liquidlabs.vso.work.WorkListener;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

public class ResourceAgentImpl implements ResourceAgent, ProcessListener {

    public static String TAG = "AGENT";
    final static Logger LOGGER = Logger.getLogger(ResourceAgent.class);
    static final Logger STATS_LOGGER = Logger.getLogger("StatsLogger");
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "Agent");
    Set<String> fileEndsToDelete = new HashSet<String>(Arrays.asList(".ser", ".replay", ".temp", ".tmp"));

    ProcessHandler processHandler = new ProcessHandler();

    ResourceSpace resourceSpace;

    ObjectTranslator query = new ObjectTranslator();

    private WorkAllocator workAllocator;
    private DeploymentService deploymentService;

//	URI endPoint;

    public static long timeDelta = 0;

    private String lookupSpaceAddress;
    private LookupSpace lookupSpace;
    private ProxyFactory proxyFactory;

    boolean sendUpdatedProfile = false;
    private boolean reboot;
    Resource[] resources;

    Random random = new Random();
    private long sysRebootNotifiedAt = -1;
    private long seedTime;
    private String lookupStart;

    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool(VSOProperties.getAgentSchedThreads(), new NamingThreadFactory("agent-sched"));
    private final ScheduledExecutorService leaseScheduler = ExecutorService.newScheduledThreadPool(1, "resource-lease-renewer");
    ClientLeaseManager leaseManager = new ClientLeaseManager(leaseScheduler);
    Map<String, EmbeddedServiceManager> embeddedServices = new ConcurrentHashMap<String, EmbeddedServiceManager>();

    State status = LifeCycle.State.STOPPED;

    ScheduledExecutorService localScheduler = ExecutorService.newScheduledThreadPool(1, new NamingThreadFactory("agent-local-sched"));

    private ResourceAgentAdminImpl jmxHook;
    private ScriptForker scriptForker = new ScriptForker();
    private DetectorsBuilder outtageDetector;

    public ResourceAgentImpl() {
    }

    public ResourceAgentImpl(String lookupSpaceAddress, ProxyFactory proxyFactory, final String jmxURL, final int profiles) {
        auditLogger.emit("Created","LU[]");
        LOGGER.info("Constructing Agent, ManagerAddr:" + lookupSpaceAddress);
        this.lookupSpaceAddress = lookupSpaceAddress;
        VSOProperties.setLookupAddress(lookupSpaceAddress);
        this.proxyFactory = proxyFactory;

        LOGGER.info("Create Profiles");
        createResourceProfiles(profiles, jmxURL, proxyFactory.getAddress());
        LOGGER.info("Init Profile");
        resources[0].initProfile();
        processHandler.addListener(ResourceAgentImpl.this);
        LOGGER.info("Done");
    }

    public ResourceAgentImpl(WorkAllocator workAllocator, ResourceSpace resourceSpace, DeploymentService deploymentService, LookupSpace lookupSpace, ProxyFactory proxyFactory,
                             String lookupSpaceAddress, String jmxHttpURL, int processingSlots) {

        auditLogger.emit("Created", "Services[], Manager:" + lookupSpaceAddress);
        LOGGER.info("Constructing Agent, ManagerAddr:" + lookupSpaceAddress);
        VSOProperties.setLookupAddress(lookupSpaceAddress);
        this.lookupSpaceAddress = lookupSpaceAddress;
        this.workAllocator = workAllocator;
        this.resourceSpace = resourceSpace;
        this.deploymentService = deploymentService;

        this.lookupSpace = lookupSpace;
        this.proxyFactory = proxyFactory;
        createResourceProfiles(processingSlots, jmxHttpURL, proxyFactory.getAddress());
        processHandler.addListener(this);
    }

    private void createResourceProfiles(int count, String jmxHttpURL, URI endPoint2) {
        resources = new Resource[count];
        for (int i = 0; i < resources.length; i++) {
            ResourceProfile resourceProfile = new ResourceProfile(jmxHttpURL, i, scheduler);
            resources[i] = new Resource(leaseManager, resourceProfile, endPoint2, i);
        }
    }

    public String getId() {
        if (resources == null)
            return "Already Stopped";
        // return getClass().getSimpleName() + UID.getSimpleUID("");
        return resources[0].agentId();
    }

    public void start() {
        try {

            if (this.status == State.STARTED) {
                LOGGER.info("AGENT Already Started");
                return;
            }
            auditLogger.emit("Start","");
            LOGGER.info("AGENT Starting " + getId());
            status = State.STARTED;
            try {
                lookupStart = lookupSpace.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
            } catch (Throwable t) {
                LOGGER.warn(t);
            }
            initProfiles();

            setBuildInfo();

            LOGGER.info(String.format("ID[0][%s] CWD[%s] Profiles[%d]", resources[0].agentId(), getCwd(), resources.length));

            resources[0].profile().scheduleOsStatsLogging(scheduler, ResourceAgent.class, LOGGER);
            resources[0].profile().scheduleOsStatsLogging(scheduler, ResourceAgent.class, STATS_LOGGER);

            addBootHashToResources();

            // dont do this unless for testing agent/lu in eclipse ----
            // unpackIfNeeded();

            scanForDeployments();

            int systemResourceId = getSystemResourceId();
            LOGGER.info("Getting System ResourceId:" + getId() + " given:" + systemResourceId);

            setupWorkListener(this, workAllocator, proxyFactory);

            deploymentService.addDeploymentListener(this);

            registerResourceProfiles(systemResourceId);

            DateTime now = new DateTime();

            // keep the resource space up to date with current utilisation etc.
            localScheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    try {
                        if (status.equals(State.STOPPED))
                            return;
                        ResourceProfile source = resources[0].profile();
                        source.updateValues();
                        for (Resource data : resources) {
                            if (data.profile().getId() != source.getId()) {
                                data.updateValues(source);
                            }
                        }
                    } catch (Throwable t) {
                        LOGGER.error("Update error", t);
                    }
                }

            }, 60 - now.getSecondOfMinute(), 60, TimeUnit.SECONDS);

            outtageDetector = new DetectorsBuilder(lookupSpaceAddress, proxyFactory,
                    new Runnable() {
                        public void run() {
                            LOGGER.warn("Bouncing the agent");
                            ResourceAgentImpl.this.bounce(true);
                        }
                    },

                    new Runnable() {
                            int panicCount;
                            public void run() {
                                if (resources[0].isPanic()) {
                                    panicCount++;
                                    if (panicCount > 5) {
                                        auditLogger.emit("Panic", Integer.toString(panicCount));
                                        LOGGER.warn("Detected Agent Panic");
                                        dumpThreads();
                                        throw new RuntimeException("NicPanic");
                                    }
                                } else {
                                    panicCount = 0;
                                }
                            }
                            private void dumpThreads() {
                                try {
                                    System.err.println(new DateTime() + " Agent Dumping threads");
                                    FileOutputStream fos = new FileOutputStream("panic-threads.txt");
                                    String threadDump = ThreadUtil.threadDump("", "");
                                    fos.write((new DateTime().toString() + "\n").getBytes());
                                    fos.write(threadDump.getBytes());
                                    fos.close();
                                } catch (Throwable t) {
                                    t.printStackTrace();
                                }
                            }

                            @Override
                            public String toString() {
                                return "Panic-Detector count:" + panicCount;
                            }
            },
                        new Runnable() {
                            private void exitWhenNoDisk(int availMb) {
                                if (availMb > 0 && availMb <= Integer.getInteger("disk.left.mb.exit", 500)) {
                                    LOGGER.fatal("Out of DiskSpace, process exiting...MB:" + availMb);
                                    System.err.println("Out of DiskSpace, process exiting...");
                                    stop();
                                    System.exit(1);
                                }
                            }

                            public void run() {
                                exitWhenNoDisk(resources[0].profile().getDiskUsableMb());
                                exitWhenNoDisk((int) (new File(VSOProperties.getWorkingDir()).getUsableSpace() / (1024 * 1024)));
                            }

                            @Override
                            public String toString() {
                                return "Low-Disk";
                            }
                        }
            );


            localScheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    if (status.equals(State.STOPPED)) {
                        LOGGER.info("Agent is stopping - suspending updates");
                        return;
                    }

                    try {
                        updateResourceSpaceIfDataChanged(false);
                    } catch (Throwable t) {
                        LOGGER.error("Critical Schedule in ResourceAgent error:" + t.toString(), t);
                    }
                }

            }, 62 - now.getSecondOfMinute(), 60, TimeUnit.SECONDS);

            localScheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    for (Resource resource : resources) {
                        resource.updateDownloads();
                        resource.forcedUpdate();
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);

            localScheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    long before = DateTimeUtils.currentTimeMillis();

                    long lookupUTC = lookupSpace.time();
                    long after = DateTimeUtils.currentTimeMillis();
                    long delta = (after - before) / 2;
                    long myTime = after - delta / 2;
                    // now we have the relative time difference... i.e.
                    // -12second
                    timeDelta = myTime - lookupUTC;

                    resources[0].profile().timeDelta = timeDelta;
                    LeaseManagerImpl.setClockDrift(timeDelta * -1);
                }

            }, 1, VSOProperties.getClockSyncMins(), TimeUnit.MINUTES);

            scheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    try {
                        File workDir = new File(VSOProperties.getWorkingDir());
                        if (workDir.exists()) {
                            List<File> listFilesRecursively = FileUtil.listFilesRecursively(workDir);
                            for (File file : listFilesRecursively) {
                                if (ResourceAgentImpl.this.acceptForDelete(file)) {
                                    if (file.lastModified() <  new DateTime().minusDays(VSOProperties.getWorkDirDaysToClean()).getMillis()) {
                                        LOGGER.info("AutoCleanup Removing:" + file.getAbsolutePath());
                                        file.delete();
                                    }
                                }
                            }
                        }
                    } catch (Throwable t) {
                        LOGGER.warn("WorkDir cleaner failed:" + t.toString(), t);
                    }

                }


            }, 1, 1, TimeUnit.DAYS);


            localScheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    Set<String> workIds = embeddedServices.keySet();
                    for (String workId : workIds) {
                        EmbeddedServiceManager service = embeddedServices.get(workId);
                        if (service.isRunning()) {
                        } else {
                            String errorMsg = service.getErrorMsg();
                            if (!errorMsg.equals("VOID")) {
                                updateStatus(workId, LifeCycle.State.ERROR, errorMsg);
                                resources[0].removeWorkId(workId);
                            }
                        }
                    }
                }
            }, 10, 10, TimeUnit.MINUTES);

            jmxHook = new ResourceAgentAdminImpl(this, resources[0].profile());

            jmxHook.registerServiceWithLookup(lookupSpace, proxyFactory);

        } catch (Throwable t) {
            LOGGER.error(t);
        }
    }
    public boolean acceptForDelete(File file) {
        String filename = file.getName();
        if (file.getAbsolutePath().toUpperCase().contains("_SERVER_")) return false;
        for (int i = 0; i < 20; i++) {
            if (filename.endsWith("." + i)) {
                return true;
            }
        }
        for (String fileEnd : fileEndsToDelete) {
            if (filename.endsWith(fileEnd)) return true;
        }
        return false;
    }

    public void setBuildInfo() {
        String bootBuildId = getBuildId("/boot/boot-1.0.bundle");
        String logBuildId = getBuildId("/vs-log-1.0/vs-log.bundle");
        String buildInfo = "boot:" + bootBuildId + " logscape:" + logBuildId;

        LOGGER.info("Build " + buildInfo + " CWD:" + getCwd());
        System.out.println("Build " + buildInfo + " CWD:" + getCwd());

        LOGGER.info("AddressInfo:" + resources[0].profile().endPoint);
        System.out.println("AddressInfo:" + resources[0].profile().endPoint);
        resources[0].profile().buildInfo = buildInfo;

    }

    /**
     * Return the current time and allow for the delta between this host time
     * and the lookupSpace time
     */
    public static long getCurrentTime() {
        return DateTimeUtils.currentTimeMillis() - timeDelta;
    }

    /**
     * Gets the time and factor in the system time delta (could be a different
     * timezone)
     *
     * @param forTime
     * @return
     */
    public static long getTime(long forTime) {
        return forTime - timeDelta;
    }

    ;

    public void scanForDeployments() {
        // scheduler.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // try {
        scanForDeployments(VSOProperties.getSystemBundleDir());
        scanForDeployments(VSOProperties.getDeployedBundleDir());
        // } catch (Throwable t) {
        // LOGGER.warn("Failed to scan deployments:" + t.toString(), t);
        // }
        // }
        //
        // }, 0, 10, TimeUnit.SECONDS);
    }

    private String getBuildId(String bundle) {
        BufferedReader reader = null;
        try {
            String line = "";
            reader = new BufferedReader(new FileReader(VSOProperties.getSystemBundleDir() + bundle));
            while ((line = reader.readLine()) != null) {
                if (line.contains("buildId")) {
                    return removeReleaseDateTags(line);
                }
            }
            return null;
        } catch (Throwable t) {
            return "unknown";
        } finally {
            try {
                reader.close();
            } catch (Throwable e) {
            }
        }
    }

    public String removeReleaseDateTags(String line) {
        return line.replaceAll("buildId", "").replaceAll("<", "").replaceAll(">", "").replaceAll("/", "").trim();
    }

    private int getSystemResourceId() {
        int waitCount = 0;
        while (waitCount++ < VSOProperties.agentRebootPingTrySeconds()) {
            try {
                return resourceSpace.getSystemResourceId();
            } catch (Throwable t) {
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        // for some reason didn't make contact with the resource space - make
        // reboot
        System.err.println("Failed resourceSpace.getSystemResourceId();, rebooting agent");
        System.exit(10);
        return 0;
    }

    // dont do this - it needs to be handled by the bootstrapper

    private void unpackIfNeeded() {
        BundleUnpacker unpacker = new BundleUnpacker(new File(VSOProperties.getSystemBundleDir()), new File(VSOProperties.getDeployedBundleDir()));
        File downloads = new File("downloads");
        File[] listFiles = downloads.listFiles();
        for (File download : listFiles) {
            unpacker.unpack(download, false);
        }
    }

    private void registerResourceProfiles(int systemResourceId) {
        int pos = 0;
        for (Resource stuff : resources) {
            try {
                stuff.profile().setSystemId(systemResourceId + pos++);
                stuff.init(resourceSpace, VSOProperties.getLUSpaceServiceLeaseInterval());
            } catch (Exception e) {
                LOGGER.error("Problem when registering Resource:" + stuff.agentId(), e);
            }
        }
    }

    private void initProfiles() {
        for (Resource stuff : resources) {
            stuff.initProfile();
        }
    }

    public void stop() {

        if (status == State.STOPPED)
            return;

        auditLogger.emit("Stop","");
        LOGGER.info("AGENT Stopping " + getId());
        System.out.println("Stopping:" + getId());

        status = State.STOPPED;
        leaseScheduler.shutdownNow();
        for (Resource res : resources) {
            res.stop();
        }

        if (!localScheduler.isShutdown()) {
            localScheduler.execute(new Runnable() {
                public void run() {
                    LOGGER.info("AGENT: " + getId() + " unregister Resources");
                    unregisterResources();
                }
            });

        }

        try {
            LOGGER.info("AGENT: " + getId() + " stopping processes");
            processHandler.shutdown();
        } catch (Throwable t) {
        }

        LOGGER.info("AGENT: " + getId() + " Stopping ESMs");
        java.util.concurrent.ExecutorService shutdownPool = Executors.newCachedThreadPool(new NamingThreadFactory("SHUTDOWN"));
        Set<String> keySet = embeddedServices.keySet();
        for (final String esmKey : keySet) {
            final EmbeddedServiceManager esm = embeddedServices.get(esmKey);

            shutdownPool.execute(new Runnable() {
                public void run() {
                    try {
                        LOGGER.info("Stopping:" + esmKey + " / " + esm.toString());
                        esm.stop();
                    } catch (Throwable t) {
                        LOGGER.warn(t.toString(), t);
                    }
                }
            });
        }
        shutdownPool.shutdown();
        scheduler.shutdown();
        LOGGER.info("AGENT: Shutting down Scheduler");
        try {
            shutdownPool.awaitTermination(VSOProperties.getShutdownPoolTimeout(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        }
        LOGGER.info("AGENT: Stopped ESMs");

        try {
            LOGGER.info("AGENT: " + getId() + " Stopping ProxyFactory");
            if (!localScheduler.isShutdown())
                localScheduler.shutdownNow();
            proxyFactory.stop();
        } catch (Throwable t) {
            LOGGER.info(t.toString(), t);
        }

        try {
            pause();
        } catch (Throwable t) {
        }

        LOGGER.info("AGENT: " + getId() + " Stopped	");
        resourceSpace = null;
        lookupSpace = null;


        this.resources = null;
    }

    private void pause() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }
    }

    private void unregisterResources() {
        try {
            for (Resource resource : resources) {
                resource.unregister();
            }
        } catch (Throwable t) {
        }
    }

    void updateResourceSpaceIfDataChanged(boolean force) {

        for (Resource resource : resources) {
            try {
                resource.updateResourceSpace(force);
            } catch (Throwable t) {
                LOGGER.error("Failed to updateResourceSpace", t);
            }
        }
    }


    public void start(final WorkAssignment workAssignment) {
        try {

            //
            // PRE-CHECK
            //
            if (isNotRunning()) {
                LOGGER.warn("Agent is:" + this.status + " Ignoring:" + workAssignment);
                return;
            }

            if (workAssignment.getProfileId() >= resources.length) {
                LOGGER.warn(String.format("WorkId %s not started as profileId %d does not exist on agent %s", workAssignment.getId(), workAssignment.getProfileId(), getId()));
                return;
            }
            Resource resource = resources[workAssignment.getProfileId()];

            if (resource.isRunning(workAssignment.getId())) {
                LOGGER.info(String.format("WorkId %s has already started on %s", workAssignment.getId(), workAssignment.getResourceId()));
                if (workAssignment.getStatus().equals(State.ASSIGNED)) {
                    setWorkStatusToRunning(workAssignment, processHandler.getPid(workAssignment.getId()));
                }
                return;
            }

            auditLogger.emit("StartWork",workAssignment.getBundleId());

            //
            // PREPARE
            //

            LOGGER.info(String.format("%s %s Starting: %s %s bg:%s workDir:%s", TAG, WorkAllocator.NAME, workAssignment.getId(), workAssignment.getStatus().name(), workAssignment.isBackground(),
                    workAssignment.getWorkingDirectory()));

            ResourceProfile resourceProfile = resource.profile();
            if (resourceProfile.setCustomProperties(workAssignment.getProperies())) {
                resource.updated();
            }
            String override = workAssignment.getOverridesService();
            if (override.length() > 0 && resourceProfile.getWorkIds().contains(override)) {
                LOGGER.warn("Starting Service where override clash exists:" + override + " work:" + resourceProfile.getWorkIds());
            }

            if (!new File(workAssignment.getWorkingDirectory()).exists()) {
                LOGGER.warn("Starting WorkAssignment:" + workAssignment.getId() + " and WorkingDir does not exist:" + workAssignment.getWorkingDirectory());
            }

            final HashMap<String, Object> variables = new HashMap<String, Object>();

            Map<String, String> customProperties = resourceProfile.getCustomProperties();
            variables.putAll(customProperties);
            if (customProperties != null && customProperties.size() > 0)
                LOGGER.debug("putting customProperties:" + customProperties);

            final String deployedBundlesDir = putScriptVariables(workAssignment, resourceProfile, variables);
            addServiceProperties(workAssignment, variables, "default");
            addServiceProperties(workAssignment, variables, workAssignment.getServiceName());
            addOverrideProperties(workAssignment, variables);

            String oldWorkId = resourceProfile.getWorkId();
            LOGGER.info("WORK ASSIGNMENT ID = " + workAssignment.getId());


            // set the variables used so they can be seen in VScapeUI
            workAssignment.setVariables(variables);

            //
            // START WORK
            //
            boolean started = false;
            final String script = workAssignment.getScript();
            // in-process or .vbs/etc which is always forked by scriptexecutor
            if (ScriptExecutor.isNonGroovyScript(script) || !workAssignment.isFork()) {
                started = startScriptExecutorTask(workAssignment, variables, deployedBundlesDir, workAssignment.getId());
            } else {
                started = startForkedGroovyTask(workAssignment, variables, script);
            }

            // the above already wait for a configured time
            // wait in case there is start wait state on services
            Thread.sleep(500);

            //
            // TELL EVERYONE WE DID IT
            //
            if (started) {
                resource.addStarted(workAssignment.getId());
                if (!workAssignment.isBackground()) {
                    resourceProfile.setActiveServiceName(workAssignment.getServiceName());
                    resourceProfile.setActiveBundle(workAssignment.getBundleId());
                    resource.setForegroundAssignment(workAssignment.getId());
                }
            }
            resource.updated();
            final String newWorkId = resourceProfile.getWorkId();
            LOGGER.info(String.format("WORK_ID was[%s] new[%s]", oldWorkId, newWorkId));



        } catch (Throwable t) {
            LOGGER.error(String.format("Failed to startWork:%s \nSCRIPT:%s", workAssignment, workAssignment.getScript()), t);
            processExited(workAssignment, true, 0, t, t.toString());
        }
    }

    private boolean startScriptExecutorTask(final WorkAssignment workAssignment, final HashMap<String, Object> variables, final String deployedBundlesDir, final String workId2) {
        if (embeddedServices.containsKey(workAssignment.getId())) {
            LOGGER.info("Already Executing WorkId:" + workAssignment.getId());
            return false;
        }
        LOGGER.info("Starting script Executor workID:" + workAssignment.getId() + " workID2:" + workId2 + " containsWork?:" + embeddedServices.containsKey(workAssignment.getId()) + " containsWork2?:" + embeddedServices.containsKey(workId2));
        EmbeddedServiceManager esm = new EmbeddedServiceManager(workAssignment.getId());
        variables.put("serviceManager", esm);
        embeddedServices.put(workAssignment.getId(), esm);

        ScriptExecutor scriptExecutor = new ScriptExecutor(scheduler, this, deployedBundlesDir, scheduler);
        esm.registerLifeCycleObject(scriptExecutor);
        Future<?> futureTask = scriptExecutor.execute(workAssignment, variables, workId2);
        if (futureTask != null) {
            esm.registerFuture(futureTask);
            return true;
        }
        return false;
    }

    private boolean startForkedGroovyTask(final WorkAssignment workAssignment, final HashMap<String, Object> variables, final String script) throws InterruptedException, Exception {
        final String workId = workAssignment.getId();
        LOGGER.info("Starting Forked:" + workId);
        if (workAssignment.isSchedulingUserScript()) {
            if (embeddedServices.containsKey(workId)) {
                LOGGER.warn("Already Executing WorkId:" + workId + " Killing old task");
                try {
                    EmbeddedServiceManager esm = embeddedServices.get(workId);
                    esm.stop();
                } catch (Throwable t) {
                    LOGGER.error("Failed to stop ESM:" + workId);
                }
                // now try and run it again
                Thread.sleep(5000);

            }

            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        if (LOGGER.isDebugEnabled()) LOGGER.info("START PROCESS:" + workAssignment.getId());
                        ScriptForker scriptForker = new ScriptForker();
                        int pid = processHandler.manage(workAssignment, scriptForker.runForked(script, workAssignment.isBackground(), variables, workAssignment));
                        if (LOGGER.isDebugEnabled()) LOGGER.info("START PROCESS PID:" + workAssignment.getId() + " PID:" + pid);
                        // wait long enough to read err-stream failure
                        Thread.sleep(VSOProperties.getProcessStartingWaitMs());
                        if (processHandler.isRunning(pid)) {
                            LOGGER.info("Process Exists:" + workId + " Set Status To Running");
                            setWorkStatusToRunning(workAssignment, processHandler.getLastPid());
                        }
                    } catch (InterruptedException t) {
                    } catch (Throwable t) {
                        LOGGER.warn("Process Failed:" + workId, t);
                        updateStatus(workId, State.ERROR, t.toString() + " Stack:" + ExceptionUtil.stringFromStack(t, 4096));
                    } finally {

                    }
                }
            };
            int minuteOffset = 60 - new DateTime().getSecondOfMinute();
            ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(runnable, minuteOffset, workAssignment.getPauseSeconds(), TimeUnit.SECONDS);
            EmbeddedServiceManager esm = new EmbeddedServiceManager(workId);
            esm.registerFuture(future);
            embeddedServices.put(workId, esm);
        } else {
            processHandler.manage(workAssignment, scriptForker.runForked(script, workAssignment.isBackground(), variables, workAssignment));
        }
        return true;
    }

    private boolean isNotRunning() {
        return this.status != LifeCycle.State.STARTED && this.status != LifeCycle.State.RUNNING;
    }

    private void addOverrideProperties(WorkAssignment workAssignment, HashMap<String, Object> variables) {
        String fileName = String.format("downloads/%s-override.properties", workAssignment.getBundleId());
        if (!new File(fileName).exists())
            return;
        Properties properties = loadProperties(fileName);
        int putCount = 0;
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String name = (String) entry.getKey();
            if (name.startsWith(workAssignment.getServiceName() + ".") || name.startsWith("default.") || name.startsWith("ALL.") || name.startsWith("*.")) {
                variables.put(name.substring(name.indexOf(".")+1), entry.getValue());
                putCount++;
            } else if (name.indexOf(".") == -1) {
                variables.put(name, entry.getValue());
            }

        }
        LOGGER.info("Adding Overrides from:" + fileName + " propertyCount:" + properties.size() + " propertiesSet:" + putCount);

    }

    private void addServiceProperties(WorkAssignment workAssignment, HashMap<String, Object> variables, String serviceName) {
        String bundleDir = workAssignment.isSystemService() ? "system-bundles" : "deployed-bundles";
        final String propertyFile = String.format("%s/%s/%s", bundleDir, workAssignment.getBundleId(), serviceName + ".properties");

        if (!new File(propertyFile).exists())
            return;
        int count = loadPropertiesFromFile(variables, propertyFile);
        LOGGER.info("Added Service Properties, count:" + count);
    }

    private Properties loadProperties(String fileName) {
        FileInputStream inStream = null;
        try {
            Properties properties = new Properties();
            inStream = new FileInputStream(fileName);
            properties.load(inStream);
            return properties;
        } catch (Exception e) {

        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    LOGGER.warn(e);
                }
            }
        }
        return new Properties();

    }

    private int loadPropertiesFromFile(HashMap<String, Object> variables, final String propertyFile) {
        Properties properties = loadProperties(propertyFile);
        for (Entry<Object, Object> entry : properties.entrySet()) {
            variables.put((String) entry.getKey(), entry.getValue());
        }
        return properties.size();

    }

    private void setWorkStatusToRunning(final WorkAssignment workAssignment, int pid) throws Exception {
        if (workAllocator == null) {
            if (workAllocator == null)
                LOGGER.info("WorkAllocator is not started YET");
            return;
        }
        // it wont be in the list until after this call goes out.
//		if (resources[workAssignment.getProfileId()].hasStarted(workAssignment.getId())) {
        LOGGER.info(" ** Work Started - setting Work to RUNNING:" + workAssignment.getId());
        workAssignment.setStatus(State.RUNNING);
        workAllocator.update(workAssignment.getId(), String.format("status replaceWith %s AND pid replaceWith %d AND variables replaceWith %s", LifeCycle.State.RUNNING, pid, workAssignment
                .getVarString()));
//		} else {
//			LOGGER.info(" ** Work NOT Started - NOT Work to RUNNING:" + workAssignment.getId());
//		}
    }

    public static HashMap<String, Object> runtimeVariables = new HashMap<String, Object>();
    private String putScriptVariables(final WorkAssignment workAssignment, ResourceProfile resourceProfile, final HashMap<String, Object> variables) {
        Map<String, String> variableMap = workAssignment.getVariables();
        variables.putAll(variableMap);
        if (variableMap != null && variableMap.size() > 0)
            LOGGER.debug("putting variables :" + variableMap.toString());

        // add required runtime properties from ResourceProfile
        variables.put("bundleName", workAssignment.getBundleId());
        variables.put("bundleId", workAssignment.getBundleId());
        variables.put("osArch", resourceProfile.getOsArchitecture());
        variables.put("resourceId", resourceProfile.getResourceId());
        variables.put("osName", resourceProfile.getOsName());
        variables.put("LookupSpaceAddress", this.lookupSpaceAddress);
        variables.put("lookupSpaceAddress", this.lookupSpaceAddress);
        variables.put("lookupUrl", this.lookupSpaceAddress);
        variables.put("lookupSpace", lookupSpace);
        variables.put("thisAgent", this);
        variables.put("executor", proxyFactory.getExecutor());
        variables.put("scheduler", proxyFactory.getScheduler());
        variables.put("proxyFactory", proxyFactory);
        variables.put("zone", getProfiles().get(0).getZone());
        variables.put("WorkingDirectory", workAssignment.getWorkingDirectory());
        variables.put("cwd", getCwd());
        String deployedBundlesDir = "";
        deployedBundlesDir = FileUtil.getPath(new File(VSOProperties.getDeployedBundleDir()));
        variables.put("bundleDir", deployedBundlesDir);
        variables.put("ResourceAgent", proxyFactory.getAddress().toString());
        variables.put("resourceAgent", proxyFactory.getAddress().toString());
        variables.put("ResourceAgentPort", proxyFactory.getAddress());
        variables.put("agentPort", proxyFactory.getAddress().getPort());
        variables.put("consumerName", workAssignment.getServiceName());
        variables.put("serviceName", workAssignment.getServiceName());
        variables.put("slaFilename", workAssignment.getSlaFilename());
        variables.put("hostname", NetworkUtils.getHostname());
        variables.put("workId", workAssignment.getId());
        variables.put("workAllocation", workAssignment.getAllocationsOutstanding());
        variables.put("profileId", resourceProfile.getId());
        variables.put("logger", LOGGER);
        variables.put("log", LOGGER);
        runtimeVariables = variables;
        return deployedBundlesDir;
    }

    public void update(WorkAssignment workAssignment) {
        LOGGER.info(String.format("%s %s Updated WorkAssignment:%s", TAG, WorkAllocator.NAME, workAssignment.getId()));
        if (workAssignment.getStatus().equals(State.SUSPENDED))
            suspend(workAssignment);
        else if (workAssignment.getStatus().equals(State.ASSIGNED))
            start(workAssignment);
        else if (workAssignment.getStatus().equals(State.STOPPED))
            stop(workAssignment);
    }

    public synchronized void suspend(WorkAssignment workAssignment) {
        LOGGER.info(String.format("Suspending %s", workAssignment.getId()));

        auditLogger.emit("SuspendWork", workAssignment.getId());
        int profileId = workAssignment.getProfileId();
        if (profileId >= resources.length) {
            LOGGER.warn("Bad Profile:" + profileId);
            return;
        }
        Resource resource = resources[profileId];
        try {
            // clean up the assignment first, then stop
            if (processHandler.stop(workAssignment)) {
                LOGGER.info("Stopping Process:" + workAssignment.getId());
            }
            if (embeddedServices.containsKey(workAssignment.getId())) {
                EmbeddedServiceManager esm = embeddedServices.remove(workAssignment.getId());
                LOGGER.info("Stopping ESM:" + workAssignment.getId());
                esm.stop();
            } else {
                LOGGER.warn("Could not find:" + workAssignment.getId() + " to stop, removing from Assignment list");
                resource.cleanUpAssignment(workAssignment);
            }
        } catch (Throwable t) {
            LOGGER.error("Failed to stopWork properly:" + workAssignment, t);
        }

    }

    public void stop(WorkAssignment workAssignment) throws RuntimeException {
        if (isNotRunning()) {
            LOGGER.warn("Agent is:" + this.status + " Ignoring:" + workAssignment);
            return;
        }
        auditLogger.emit("StopWork",workAssignment.getId());

        LOGGER.info(String.format("Stopping %s", workAssignment.getId()));
        int profileId = workAssignment.getProfileId();
        if (profileId >= resources.length) {
            LOGGER.warn("Bad Profile");
            return;
        }
        Resource resource = resources[profileId];
        try {
            boolean found = false;
            // clean up the assignment first, then stop
            if (processHandler.stop(workAssignment)) {
                LOGGER.info("Stopping Process:" + workAssignment.getId());
                resource.cleanUpAssignment(workAssignment);
                found = true;
            }
            if (embeddedServices.containsKey(workAssignment.getId())) {
                EmbeddedServiceManager removed = embeddedServices.remove(workAssignment.getId());
                LOGGER.info("Stopping ESM:" + workAssignment.getId());
                removed.stop();
                resource.cleanUpAssignment(workAssignment);
                found = true;
            }
            if (!found) {
                LOGGER.warn("Could not find:" + workAssignment.getId() + " to stop, removing from Assignment list");
                resource.cleanUpAssignment(workAssignment);
            }
        } catch (Throwable t) {
            LOGGER.error("Failed to stopWork properly:" + workAssignment, t);
        }
    }

    public void updateStatus(String workId, LifeCycle.State status, String errorMsg) {
        try {
            errorMsg = fixErrorMsg(errorMsg);
            LOGGER.info(String.format("00%s] status:%s msg:%s", workId, status.name(), errorMsg));
            auditLogger.emit("UpdateWork",workId + " status: " +status.name() + "  msg:" + errorMsg);
            if (errorMsg.length() == 0) errorMsg = "-";
            if (workAllocator != null) workAllocator.update(workId, "status replaceWith " + status.name() + " and errorMsg replaceWith '" + errorMsg + "'");
        } catch (Exception e) {
            LOGGER.error(String.format("%s cannot update Status of WorkId[%s] to Status[%s] message[%s]", getId(), workId, status, errorMsg), e);
        }
    }

    private String fixErrorMsg(String errorMsg) {
        if (errorMsg.length() > 512)
            errorMsg = errorMsg.substring(0, 511);
        errorMsg = errorMsg.replaceAll("\"", "&q;");
        errorMsg = errorMsg.replaceAll("\'", "&q;");
        errorMsg = errorMsg.replaceAll(" OR ", "_OR_");
        errorMsg = errorMsg.replaceAll(" or ", "_OR_");
        errorMsg = errorMsg.replaceAll(" AND ", "_AND_");
        errorMsg = errorMsg.replaceAll(" and ", "_AND_");
        return errorMsg;
    }

    private String getCwd() {
        return FileUtil.getPath(new File("."));
    }

    private void setupWorkListener(final WorkListener workListener, final WorkAllocator workAllocator, ProxyFactory proxyFactory) {

        final long purgeAllocsBefore = System.currentTimeMillis() - 30 * 1000;
        workAllocator.removeWorkAssignmentsForResourceId(getId(), purgeAllocsBefore);
        localScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                workAllocator.removeWorkAssignmentsForResourceId(getId(), purgeAllocsBefore);

            }
        }, 1, TimeUnit.MINUTES);

        try {
            LOGGER.info("Registering WorkListener:" + getId() + " :" + workAllocator);
            workAllocator.registerWorkListener(workListener, getId(), getId(), true);
        } catch (Exception e) {
            LOGGER.error("Failed to register WorkListener:" + e.toString(), e);
        }
        localScheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    workAllocator.registerWorkListener(workListener, getId(), getId(), false);
                } catch (Throwable t) {
                    LOGGER.error("Failed to RegisterWorkListener against:" + workAllocator.toString(), t);
                }
            }
        }, VSOProperties.getLUSpaceServiceRenewInterval(), VSOProperties.getLUSpaceServiceRenewInterval(), TimeUnit.SECONDS);

        localScheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                for (Resource virtual : resources) {
                    try {
                        workAllocator.renewWorkLeases(virtual.profile().getResourceId(), VSOProperties.getLUSpaceServiceLeaseInterval());
                    } catch (Throwable t) {
                        LOGGER.error("Failed to RenewLease against:" + workAllocator.toString(), t);
                    }
                }
            }
        }, VSOProperties.getLUSpaceServiceRenewInterval(), VSOProperties.getLUSpaceServiceRenewInterval(), TimeUnit.SECONDS);

    }

    public ResourceProfile getResourceProfile() {
        return resources[0].profile();// wrong
    }

    public void addDeployedBundle(String bundleName, String releaseDate) {
        LOGGER.info(String.format("AGENT %s %s [%s] has been deployed", getEndPoint(), bundleName, releaseDate));
        auditLogger.emit("addDeployedBundle", bundleName);
        for (Resource resource : resources) {
            resource.addDeployedBundle(bundleName);
            resource.forcedUpdate();
        }
    }

    public void removeBundle(String bundleId) {
        LOGGER.info(String.format("AGENT %s Removing bundle[%s] from resource agent", getEndPoint(), bundleId));
        for (Resource resource : resources) {
            resource.removeBundle(bundleId);
        }
    }

    public String getEndPoint() {
        return proxyFactory.getEndPoint();
    }

    private void addBootHashToResources() {
        try {
            FileInputStream hashStream = new FileInputStream("system-bundles/boot/vs.hash");
            byte[] buf = new byte[hashStream.available()];
            hashStream.read(buf, 0, buf.length);
            String hash = new String(buf);
            for (Resource resource : resources) {
                resource.setBootHash(hash);
            }
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }

    }

    public void scanForDeployments(String directoryName) {
        File directory = new File(directoryName);
        File[] dirItems = directory.listFiles();
        if (dirItems == null) {
            try {
                new File(directoryName).mkdir();
            } catch (Throwable t) {
                LOGGER.warn("Failed to create:" + directoryName + " " + t.toString(), t);
            }
            LOGGER.warn("Directory did not exist and has been created:" + directoryName);
            return;
        }
        for (File dirItem : dirItems) {
            if (dirItem.isDirectory() && !dirItem.getName().startsWith(".")) {
                File[] files = dirItem.listFiles(new FileFilter() {
                    public boolean accept(File arg0) {
                        return arg0.getName().endsWith(".bundle");
                    }
                });
                if (files.length == 1) {
                    Bundle bundle = new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle(files[0].getAbsolutePath());
                    addDeployedBundle(bundle.getId(), bundle.getReleaseDate());
                } else {
                    LOGGER.warn(String.format("Couldn't determine bundle file from directory [%s] Found [%d] *.bundle files", dirItem.getName(), files.length));
                }
            } else if (dirItem.getName().contains(".bundle")) {
                addDeployedBundle(dirItem.getName(), "unknown");
            }
        }

    }

    public void editResourceProperties(String type, String location, String maxHeap) {
        String confFile = rewriteSetupConf(type, location);
        if (confFile == null)
            return;

        com.liquidlabs.common.file.FileFilter fileFilter = new com.liquidlabs.common.file.FileFilter(".", confFile,
                "boot.properties:etc/logscape-service-x86.ini:etc/logscape-service-x64.ini:conf/agent.conf:conf/lookup.conf:logscape.sh:logscape.bat");
        Map<String, String> extractVars = fileFilter.extractVars();
        LOGGER.info("Vars:" + extractVars);
        fileFilter.process();

        // now sort out the replacement for heap
        rewriteBOOTPropertiesHeapValue(maxHeap);

        scheduler.execute(new Runnable() {
            public void run() {
                bounce(true);
            }
        });
    }

    private void rewriteBOOTPropertiesHeapValue(String maxHeap) {
        String bootContents = FileUtil.readAsString("boot.properties");
        String[] split = bootContents.split("-Xmx");
        StringBuilder newContents = new StringBuilder();
        newContents.append(split[0]);
        newContents.append("-Xmx");
        newContents.append(maxHeap);
        newContents.append(split[1].substring(split[1].indexOf(" ")));

        try {
            FileOutputStream fos = new FileOutputStream("boot.properties");
            fos.write(newContents.toString().getBytes());
            fos.close();
        } catch (Exception e) {
            LOGGER.warn(e);
        }
    }

    private String rewriteSetupConf(String type, String newLocation) {
        // edit system properties in boot.properties
        String confFile = "etc/setup.conf";
        LOGGER.info("GIVEN VALUES:" + type + " LOC:" + newLocation);

        try {
            List<String> setupConf = FileUtil.readLines(confFile, 1000);
            if (setupConf == null) {
                LOGGER.warn("Failed to read config file");
                return null;
            }
            FileOutputStream fos = new FileOutputStream(confFile);
            for (String line : setupConf) {
                line = line.replaceFirst(".*agent.role.*", "\t\t<add key=\"agent.role\" value=\"" + type + "\"/>")
                        + "\n";
                fos.write(line.getBytes());
            }
            fos.close();

        } catch (Exception e) {
            LOGGER.warn("Failed to re-write config file:" + e, e);
            return null;
        }
        return confFile;
    }

    public void bounce(boolean shouldSleep) {
        try {
            if (status == State.STOPPED) {
                LOGGER.info(String.format("AGENT %s  - cannot bounce again - already stopped", getId()));
                return;
            }
            auditLogger.emit("Bounce","");
            System.out.println(new Date() + " bouncing");
            LOGGER.info(String.format("bounce(%b)", shouldSleep));
            stop();
            Thread.sleep(5000);

        } catch (Throwable t) {
            LOGGER.warn(t.toString(), t);
        }

        LOGGER.info(String.format("AGENT %s is shutting down - sleep:%b sysNotifiedMs:%d", getId(), shouldSleep, sysRebootNotifiedAt));
        restALittleMore(shouldSleep);

        reboot = true;
        synchronized (this) {
            LOGGER.info(String.format("AGENT %s  - notify", getId()));
            notify();
        }
        LOGGER.info(String.format("AGENT %s  - EXITED", getId()));
        System.out.println("The AGENT HAS EXITED");
        System.exit(10);
    }

    private void restALittleMore(boolean shouldSleep) {
        if (sysRebootNotifiedAt != -1 && shouldSleep) {

            LOGGER.info("Waiting for LUSpace[ping] to bounce - or timeout");

            ProxyFactoryImpl proxyFactory = null;
            try {
                proxyFactory = new ProxyFactoryImpl(this.proxyFactory.getAddress().getPort() + 2, Executors.newCachedThreadPool(), "ResourceAgent");
                proxyFactory.start();
            } catch (URISyntaxException e1) {
            }
            String ping = "";
            int waitCount = 0;
            int sleepInterval = 5000;
            while ((ping.equals(lookupStart) || ping.equals("")) && (waitCount++ * sleepInterval) < VSOProperties.agentRebootPingTrySeconds() * 1000) {
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                }
                try {
                    ping = proxyFactory.getRemoteService(LookupSpace.NAME, LookupSpace.class, lookupSpaceAddress).ping(VSOProperties.getResourceType(), System.currentTimeMillis());
                } catch (Throwable t) {
                }
            }
            System.err.println(String.format("AGENT: LookupSpace has rebooted ping[%s] waitCount[%d]secs", ping, (waitCount * sleepInterval) / 1000));
            LOGGER.info(String.format("AGENT: LookupSpace has rebooted ping[%s] waitCount:[%d]secs", ping, (waitCount * sleepInterval) / 1000));
            if (resources[0].profile().getWorkIds().contains("UploadService")) {
                try {
                    long sinceRebootMessage = System.currentTimeMillis() - sysRebootNotifiedAt;
                    long minSleep = 30000 + seedTime - sinceRebootMessage;
                    int nextInt = random.nextInt(30000);
                    long sleep = minSleep + nextInt;
                    LOGGER.info(String.format("Sleeping for %d seconds before bouncing", sleep));
                    if (sleep > 0) {
                        Thread.sleep(sleep);
                    }
                } catch (InterruptedException e) {
                }
            }
        } else {
            if (shouldSleep) {
                try {
                    Thread.sleep(VSOProperties.agentRebootPingTrySeconds() * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void systemReboot(long seedTime) {

        auditLogger.emit("SystemReboot","");
        LOGGER.info(String.format("systemReboot(%d)", seedTime));
        sysRebootNotifiedAt = System.currentTimeMillis();
        this.seedTime = seedTime;
    }

    public void booted(LookupSpace lookupSpace, WorkAllocator workAllocator, ResourceSpace resourceSpace, DeploymentService bundleDeploymentService) {
        LOGGER.info("Booted with boot-1.0 services");
        this.lookupSpace = lookupSpace;
        this.workAllocator = workAllocator;
        this.resourceSpace = resourceSpace;
        this.deploymentService = bundleDeploymentService;

        // revert status so we make sure start actually starts
        this.status = State.STOPPED;

        start();
    }

    public void bootNoWait(LookupSpace lookupSpace, WorkAllocator workAllocator, ResourceSpace resourceSpace, DeploymentService deploymentService) {
        this.lookupSpace = lookupSpace;
        this.workAllocator = workAllocator;
        this.resourceSpace = resourceSpace;
        this.deploymentService = deploymentService;
        start();
    }

    public List<ResourceProfile> getProfiles() {
        List<ResourceProfile> profiles = new ArrayList<ResourceProfile>();
        for (Resource data : resources) {
            profiles.add(data.profile());
        }
        return profiles;
    }

    public void go() {
        synchronized (this) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
        if (reboot) {
            LOGGER.warn("Rebooting: rebootFlag:" + reboot);
            System.exit(10);
        }
        LOGGER.warn("Exiting: rebootFlag:" + reboot);
        System.exit(0);

    }

    public void processExited(WorkAssignment workAssignment, boolean isFault, int exitCode, Throwable throwable, String stderr) {
        try {
// allow it to update the workStatus as Stopped - this may not scale so well!
//            // the process is a scheduled task so there is nothing to do....
//            if (workAssignment.isSchedulingUserScript() && !isFault) {
//                if (LOGGER.isDebugEnabled()) LOGGER.debug("PROCESS EXITED NORMALLY:" + workAssignment.getId() + " ExitCode:" + exitCode + " Fault:" + isFault);
//                return;
//            }
            if (this.isNotRunning()){
                return;
            }

            stderr = fixErrorMsg(stderr);
            if (isFault) {

                auditLogger.emit("ProcessExitError",workAssignment.getId() + " msg:" + stderr);

                LOGGER.warn(String.format("Process %s exited Msg:%s, updating workStatus", workAssignment.getId(), stderr));

                Resource resource = resources[workAssignment.getProfileId()];
                if (!workAssignment.isBackground()) {
                    resource.cleanUpAssignment(workAssignment);
                }


                if (throwable == null) {
                    if (workAllocator != null)
                        workAllocator.update(workAssignment.getId(), String.format("status replaceWith %s AND errorMsg replaceWith 'process terminated exitValue:%s msg:%s'",	LifeCycle.State.ERROR.name(), exitCode, stderr));
                } else {
                    if (workAllocator != null) workAllocator.update(workAssignment.getId(),
                            String.format("status replaceWith %s AND errorMsg replaceWith 'process exception[%s] msg[%s]'", LifeCycle.State.ERROR.name(), ExceptionUtil.stringFromStack(throwable, 2024), stderr));
                }
                if (exitCode == 10) {
                    LOGGER.warn(String.format("Process %s exited with 10 (bounce due to OOMem?) - attempting to restart WorkAssignment", workAssignment.getId()));
                    this.start(workAssignment);
                }
                if (resources != null) resources[workAssignment.getProfileId()].terminated(workAssignment, isFault);
            } else {
                if (stderr == null || stderr.length() == 0) stderr = "-";
                workAllocator.update(workAssignment.getId(), String.format("status replaceWith %s AND errorMsg replaceWith '%s'", LifeCycle.State.STOPPED.name(), stderr));
            }
        } catch (Exception e) {
            LOGGER.warn(e.toString(), e);
        }	}



    public static ResourceAgent getRemoteService(ProxyFactory proxyFactory, String address) {
        return proxyFactory.getRemoteService(ResourceAgent.NAME, ResourceAgent.class, address);
    }

    public void errorDeploying(String bundleName, String hash, String errorMessage) {
        LOGGER.error("DEPLOY_LISTENER Deploying bundle:" + bundleName + " error:" + errorMessage);
    }

    public void successfullyDeployed(String bundleId, String hash) {
        try {
            // unzip the file and add the deployment entry to the profiles.
            BundleUnpacker unpacker = new BundleUnpacker(new File(VSOProperties.getSystemBundleDir()), new File(VSOProperties.getDeployedBundleDir()));
            File bundleZip = new File("downloads", bundleId + ".zip");
            if (!bundleZip.exists()) {
                LOGGER.error("DEPLOY_LISTENER Failed to find:" + bundleZip.getAbsolutePath());
                return;
            }
            LOGGER.info("DEPLOY_LISTENER Deploying bundle:" + bundleId + " " + bundleZip.getAbsolutePath());
            if (unpacker.isBundle(bundleZip)) {
                Bundle theBundle = unpacker.unpack(bundleZip, false);
                LOGGER.info(String.format("DEPLOY_LISTENER - %s has been installed", theBundle.getId()));
                addDeployedBundle(theBundle.getId(), theBundle.getReleaseDate());
                if (!theBundle.isSystem()) {
                    new File(VSOProperties.getDeployedBundleDir() + "/" + bundleId, "DEPLOYED").createNewFile();
                }
                auditLogger.emit("Deployed",bundleId);
            } else {
                LOGGER.info("DEPLOY_LISTENER Not a bundle:" + bundleId);
            }

        } catch (Throwable t) {
            LOGGER.error("DEPLOY_LISTENER Deployment failed:" + bundleId + " ex:" + t.toString(), t);
        }
    }

    public void unDeployed(String bundleId, String hash) {

        try {

            auditLogger.emit("UnDeployed",bundleId);
            // i.e. vs-log-1.0
            LOGGER.info("DEPLOY_LISTENER UnDeploy - Deleting bundle:" + bundleId);
            // remove the bundle.zip file
            BundleUnpacker unpacker = new BundleUnpacker(new File(VSOProperties.getSystemBundleDir()), new File(VSOProperties.getDeployedBundleDir()));
            LOGGER.info("DEPLOY_LISTENER UnDeploy Deleting bundle:" + bundleId);
            this.removeBundle(bundleId);
            boolean wasDeleted = unpacker.deleteExpandedBundleDir(bundleId);
            LOGGER.info("DEPLOY_LISTENER Deleting bundleContents, success:" + wasDeleted);
        } catch (Throwable t) {
            LOGGER.error("undeploy action failed:" + bundleId, t);
        }
    }



    public void setStatus(State started) {
        LOGGER.info("SetStatus:" + started);
        this.status = started;
    }

    // add some shit so we can test without having to test the whole fucking world!
    void setScriptForker(ScriptForker scriptForker) {
        this.scriptForker = scriptForker;
    }

    void setProcessHandler(ProcessHandler processHandler) {
        this.processHandler = processHandler;
    }
}
