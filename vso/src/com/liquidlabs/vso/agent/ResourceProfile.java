package com.liquidlabs.vso.agent;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.Time;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.collection.PropertyMap;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.Id;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.metrics.*;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ResourceProfile implements ResourceProfileMBean {
    transient static Logger LOGGER = Logger.getLogger(ResourceProfile.class);
    transient static DateTimeFormatter dateTimeFormatter = Time.formatter();

    @Id
    String resourceId = "notSet-0";
    int systemId;
    String agentId = "notSet";
    int instanceId = 0;
    private int id;
    private String bootHash;
    String hostName;
    public String ipAddress;
    public String networkInterface = "";
    int port;
    String endPoint = "tcp://xxxxx/xxxx";
    public String gateway = "0.0.0.0";

    //Nathen added
    String username = (System.getProperty("user.name") != "") ? System.getProperty("user.name") : "Unknown";

    // WINDOWS NT || Windows 2000 || MAC OS X || MAC OS || LINUX || Solaris || SUNOS
    String osName = System.getProperty("os.name").toUpperCase();
    // X86
    String osArchitecture = System.getProperty("os.arch");
    String jvmBits = System.getProperty("sun.arch.data.model","32");
    String jvmVersion = System.getProperty("java.version");
    String jvmName = System.getProperty("java.vm.name");

    // need to default to having the boot-1.0 bundle
    public String deployedBundles = "boot-1.0";

    // 4.0
    String osVersion = System.getProperty("os.version");

    long startTimeSecs = System.currentTimeMillis() / 1000;
    String startTime = Time.nowAsString();
    String lastUsed = dateTimeFormatter.print(DateTimeUtils.currentTimeMillis());

    String cpuModel = "unknown";
    String cpuSpeed = "unknown";
    int cpuUtilisation = 0;
    int currentTemp = 20;
    String country = System.getProperty("user.country");
    String timeZone = TimeZone.getDefault().getID();
    int maxTemp = 90;
    int currentPowerDraw = 10;
    int maxPowerDraw = 150;

    int cpuCount = 2;
    int coreCount = 2;
    int mflops = 1;

    String domain = null;
    String subnetMask = "255.255.254.0";

    String zone = VSOProperties.getZone();
    int locationX = VSOProperties.getLocationX();
    int locationY = VSOProperties.getLocationY();
    int locationZ = VSOProperties.getLocationZ();

    String customProperties;

    public String workId = "";

    private String cwd;

    String lastUpdated = new DateTime().toString();
    long lastUpdatedMs = DateTimeUtils.currentTimeMillis();

    // information related to the purpose of the machine
    // dedicated, loaned
    private String ownership = System.getProperty("vscape.resource.ownership", "DEDICATED");

    private String activeServiceName = "";

    private String activeBundleId = "";

    transient MBeanGetter mBeanGetter = new MBeanGetter();

    private long memoryUsed = 1;

    private long memoryAvailable = 1;

    private long memoryMax = 1;

    private long memoryCommitted = 1;

    private String jmxHttpURL;

    public String type = VSOProperties.getResourceType(); // VM, compute, data, etc

    private transient int updateCount;

    private int pid;

    int physMemTotalMb = 1;

    public int physMemFreeMb = 1;

    int swapTotalMb = 1;

    int swapFreeMb = 1;

    private int vmCommitedMb;

    int diskTotalSpaceMb = 1;

    private int diskUsableSpaceMb = 1000;

    int diskFreeSpaceMb = 1;

    String downloads = "";
    String downloadHashes = "";

    private String downloadLast = "";

    transient private ScheduledExecutorService scheduler;

    public String buildInfo;

    public long timeDelta;
    private double cpuTime;

    public ResourceProfile() {
    }

    public ResourceProfile(String jmxHttpURL, int id, ScheduledExecutorService scheduler) {
        this.jmxHttpURL = jmxHttpURL;
        this.id = id;
        this.instanceId = id;
        this.scheduler = scheduler;
    }

    public void scheduleOsStatsLogging(ScheduledExecutorService scheduler, final Class clazz, final Logger logger) {
        this.oneOffUpdate();
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logStats(clazz.getSimpleName(), logger);
            }
        }, 60 - new DateTime().getSecondOfMinute(), 60, TimeUnit.SECONDS);
    }

    public void logStats(String clazz, Logger logger) {
        try {

            updateValues();
            logger.info(String.format("COMP:%s %s ROLE:%s CPU:%d ", clazz, getHostName(), type, getCpuUtilisation()));
            logger.info(getMemLogMsg(clazz));

            if (getMemoryAvailable() < 2) {
                logger.info(String.format("COMP:%s %s ROLE:%s MEM MB MAX:%d COMMITED:%d USED:%d WARN LOW MEM ***AVAIL:%d", clazz, getHostName(), type, getMemoryMax(), getMemoryCommitted(), getMemoryUsed(), getMemoryAvailable()));
            }
        } catch (OutOfMemoryError oom) {
            logger.info(String.format("COMP:%s %s ROLE:%s MEM MB MAX:%d COMMITED:%d USED:%d AVAIL:%d", clazz, getHostName(), type, getMemoryMax(), getMemoryCommitted(), getMemoryUsed(), getMemoryAvailable()));
        } catch (Throwable t) {
            logger.warn("LogStatsWarn:" + t,t);
        }
    }    public boolean updateValues() {

        long nowMs = DateTimeUtils.currentTimeMillis() + timeDelta;
        long oldLastUpdate = this.lastUpdatedMs;

        // at most every 15 minutes
        if (this.mflops == 0 ||  nowMs - oldLastUpdate > (10 * 60 * 1000) ) {
            this.mflops = new Linpack().actionPerformed(500, mflops, updateCount++);
            if (cpuUtilisation > 30) {
                this.mflops = (int) (this.mflops + cpuUtilisation);
            }
        }
        touch();

        this.memoryUsed = mBeanGetter.getHeapMemoryUsage();
        this.memoryAvailable = mBeanGetter.getHeapMemoryAvailable();

        this.memoryCommitted = mBeanGetter.getHeapMemoryCommitted();

        this.physMemFreeMb = mBeanGetter.getPhysicalMemFreeMb();
        this.swapFreeMb = mBeanGetter.getSwapFreeMb();
        this.vmCommitedMb = mBeanGetter.getVMCommitedMb();
        this.diskUsableSpaceMb = mBeanGetter.getDiskUsableSpaceMb();
        this.diskFreeSpaceMb = mBeanGetter.getDiskFreeSpaceMb();


        String hh = getBundleHashValues("downloads");
        if (hh != null) {
            this.downloadHashes = hh;
            this.downloads = getBundleHash("downloads");
        }
        this.cpuTime =  mBeanGetter.getCpuTime();


        OSGetter osGetter = OSGetters.get(scheduler);

        if (osGetter == null) return true;

        this.cpuUtilisation = osGetter.getCPULoadPercentage();
        this.physMemFreeMb = OSGetters.get(scheduler).getAvailMemMb();

        // TODO: WMIC doesn't hook into temp or power modules so stuff it - derive from CPUModel, CPUItil and power algo 4 cpu
        this.currentTemp = 20 + (int) (this.maxTemp * this.cpuUtilisation/100.0);
        this.currentPowerDraw = 20 + (int) ((this.maxPowerDraw * this.cpuUtilisation)/100.0);
        return true;
    }



    private String getBundleHash(String directory) {
        File file = new File(directory);
        if (!file.exists() || !file.isDirectory()) return "None";
        File[] listFiles = file.listFiles();
        if (listFiles == null) return "Zero Files";
        List<File> allFiles = new ArrayList<File>(java.util.Arrays.asList(listFiles));
        Collections.sort(allFiles, new Comparator<File>(){
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        StringBuilder results = new StringBuilder();
        long lastMod = 0;
        String lastModName = "";
        for (File file2 : allFiles) {
            // note - we minus timeDelta - i.e. if we are 12 seconds in front - need to remove it - 12 seconds behind we need to add it
            String when = DateUtil.shortDateTimeFormat3.print(file2.lastModified() - timeDelta);

            results.append(file2.getName()).append(" m:").append(when).append(" s:").append(file2.length()/1024);
            results.append("\n");
            if (file2.lastModified() > lastMod && !file2.getName().startsWith(".")) {
                lastMod = file2.lastModified();
                lastModName = file2.getName() + " m:" + when;
            }
        }
        this.downloadLast  = lastModName;

        return results.toString();
    }
    transient Map<String, String> lastFilesHash = new HashMap<String, String>();
    transient boolean isCollectingHash = false;

    private String getBundleHashValues(String directory) {
        if (isCollectingHash) return null;
        try {
            isCollectingHash = true;

            File file = new File(directory);
            if (!file.exists() || !file.isDirectory()) return "None";
            File[] listFiles = file.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return !file.isDirectory();
                }
            });
            if (listFiles == null) return "Zero Files";
            List<File> allFiles = new ArrayList<File>(java.util.Arrays.asList(listFiles));
            Collections.sort(allFiles, new Comparator<File>(){
                @Override
                public int compare(File o1, File o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });

            StringBuilder results = new StringBuilder();
            for (File file2 : allFiles) {
                try {
                    results.append(getHashForFile(file2));
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
            return results.toString();
        } finally {
            isCollectingHash = false;
        }
    }
    String getHashForFile(File file2) throws NoSuchAlgorithmException, IOException {
        String key = file2.getName() + file2.lastModified();
        String fileHash = lastFilesHash.get(key);
        if (fileHash == null) fileHash = new HashGenerator().createHash(file2.getName(), file2);
        lastFilesHash.put(key, fileHash);
        String format = String.format("%s #:%s\n",file2.getName(), fileHash);
        return format;
    }

    public void setDownloads(String downloads) {
        this.downloads = downloads;
    }

    public void updateValues(ResourceProfile source){
        this.mflops = source.mflops;

        this.memoryUsed = source.memoryUsed;
        this.memoryAvailable = source.memoryAvailable;
        this.memoryCommitted = source.memoryCommitted;

        this.cpuUtilisation = source.cpuUtilisation;
        this.currentPowerDraw = source.currentPowerDraw;
        this.currentTemp = source.currentTemp;
        touch();
    }

    private void touch() {
        this.lastUpdatedMs = DateTimeUtils.currentTimeMillis() + timeDelta;
        this.lastUpdated = Time.formatter().print(this.lastUpdatedMs);
    }

    public long getMemoryCommitted() {
        return memoryCommitted;
    }

    public void oneOffUpdate() {

        if (this.cwd == null) this.cwd = FileUtil.getPath(new File("."));

        this.hostName = NetworkUtils.getHostname();
        this.domain = getDomain();
        this.ipAddress = NetworkUtils.getIPAddress();
        this.networkInterface = NetworkUtils.networkInterface;
        this.endPoint = "tcp://" + this.ipAddress + ":" + port;

        this.memoryMax = mBeanGetter.getHeapMemoryMax();

        this.pid = mBeanGetter.getPid();
        this.cpuCount = mBeanGetter.getAvailableProcessors();
        this.swapTotalMb = mBeanGetter.getSwapTotalMb();
        this.swapFreeMb = mBeanGetter.getSwapFreeMb();
        this.vmCommitedMb = mBeanGetter.getVMCommitedMb();
        this.diskTotalSpaceMb = mBeanGetter.getDiskTotalSpaceMb();
        this.diskUsableSpaceMb = mBeanGetter.getDiskUsableSpaceMb();
        this.diskFreeSpaceMb = mBeanGetter.getDiskFreeSpaceMb();

        if (scheduler == null) return;

        if (!Boolean.getBoolean("test.mode")) {
            Runnable run = new Runnable() {
                public void run() {
                    OSGetter osGetter = OSGetters.get(scheduler);
                    physMemTotalMb = osGetter.getTotalMemoryMb();
                    physMemFreeMb = osGetter.getAvailMemMb();
                    coreCount = osGetter.getTotalCPUCoreCount();
                    mflops = new Linpack().actionPerformed(500);
                    cpuModel = osGetter.getCPUModel();
                    cpuSpeed = osGetter.getCPUSpeed();
                    subnetMask = osGetter.getSubnetMask();
                    ResourceProfile.this.gateway = osGetter.getGateway();
                }
            };
            scheduler.submit(run);

        }
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public long getMemoryAvailable() {
        return memoryAvailable;
    }

    public long getMemoryMax() {
        return memoryMax;
    }

    public String getHostName() {
        if (hostName != null) {
            return hostName;
        }
        return NetworkUtils.getHostname();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
        this.agentId = getHostName() + "-" + port;
        this.resourceId = getHostName() + "-" + port + "-" + id;
        this.endPoint = "tcp://" + NetworkUtils.getIPAddress()  + ":" + port;
    }

    public String getAgentId() {
        return agentId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public long getStartTimeSecs() {
        return startTimeSecs;
    }

    public String getLastUsed() {
        return lastUsed;
    }

    public int getCurrentTemp() {
        return currentTemp;
    }

    public int getCoreCount() {
        return coreCount;
    }

    public int getMflops() {
        return mflops;
    }

    public String getZone() {
        return zone;
    }

    public String getDomain() {

        if (domain == null) {
            String name = "";
            String fullName = "";
            try {
                InetAddress localHost = InetAddress.getLocalHost();
                fullName = localHost.getCanonicalHostName();
                name = localHost.getHostName();
                domain = fullName.replace(name, "");
                if (domain.startsWith(".")) domain = domain.substring(1);
            } catch (Throwable t) {
            }
        }

        return domain;
    }

    public String getSubnet() {
        return subnetMask;
    }

    public Map<String, String> getCustomProperties() {
        return new PropertyMap(customProperties);
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(URI endPoint) {
        try {
            this.endPoint = new URI(endPoint.getScheme(), endPoint.getUserInfo(), this.ipAddress, endPoint.getPort(), endPoint.getPath(), endPoint.getQuery(), endPoint.getFragment()).toString();
        } catch (URISyntaxException e) {
            LOGGER.error(e);
        }
    }

    public String getOsName() {
        return osName;
    }

    public String getOsArchitecture() {
        return osArchitecture;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public String getCpuModel() {
        return cpuModel;
    }

    public int getCpuUtilisation() {
        return cpuUtilisation;
    }
    public double getCpuTime() {
        return cpuTime;
    }
    public double getSysMemUtil() {
        return ((double)physMemFreeMb / (double)physMemTotalMb) * 100.0;
    }
    public double getDiskUtil() {
        return  100.0 - getPercent(diskFreeSpaceMb, diskTotalSpaceMb);
    }
    public double getSwapUtil() {
        return  ((double)this.swapFreeMb / (double) this.swapTotalMb) * 100.0;
    }

    public int getMaxTemp() {
        return maxTemp;
    }

    public int getCurrentPowerDraw() {
        return currentPowerDraw;
    }

    public int getMaxPowerDraw() {
        return maxPowerDraw;
    }

    public int getCpuCount() {
        return cpuCount;
    }

    public String getSubnetMask() {
        return subnetMask;
    }

    public int getLocationX() {
        return locationX;
    }

    public int getLocationY() {
        return locationY;
    }

    public int getLocationZ() {
        return locationZ;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName.toLowerCase();
    }

    public void setOsName(String osName) {
        this.osName = osName.toLowerCase();
    }

    public void setOsArchitecture(String osArchitecture) {
        this.osArchitecture = osArchitecture;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public void setStartTimeSecs(long startTimeSecs) {
        this.startTimeSecs = startTimeSecs;
    }

    public void setLastUsed(String lastUsed) {
        this.lastUsed = lastUsed;
    }

    public void setCpuModel(String cpuModel) {
        this.cpuModel = cpuModel;
    }

    public void setCpuUtilisation(int cpuUtilisation) {
        this.cpuUtilisation = cpuUtilisation;
    }

    public void setCurrentTemp(int currentTemp) {
        this.currentTemp = currentTemp;
    }

    public void setMaxTemp(int maxTemp) {
        this.maxTemp = maxTemp;
    }

    public void setCurrentPowerDraw(int currentPowerDraw) {
        this.currentPowerDraw = currentPowerDraw;
    }

    public void setMaxPowerDraw(int maxPowerDraw) {
        this.maxPowerDraw = maxPowerDraw;
    }

    public void setLocation(String location) {
        this.zone = location;
    }

    public void setLocationX(int locationX) {
        this.locationX = locationX;
    }

    public void setLocationY(int locationY) {
        this.locationY = locationY;
    }

    public void setLocationZ(int locationZ) {
        this.locationZ = locationZ;
    }

    public void setCustomProperty(String key, String value) {
        PropertyMap propertyMap = new PropertyMap(this.customProperties);
        propertyMap.put(key, value);
        this.customProperties = propertyMap.toString();
    }
    public boolean setCustomProperties(Map<String, String> properies) {
        PropertyMap propertyMap = new PropertyMap(this.customProperties);
        boolean newValueWasAdded = propertyMap.putAllWithNewValueAddedResult(properies);
        this.customProperties = propertyMap.toString();
        return newValueWasAdded;
    }

    public String getDeployedBundles() {
        return deployedBundles;
    }

    public synchronized void setDeployedBundles(String deployedBundle) {
        if (!this.deployedBundles.contains(deployedBundle))	this.deployedBundles += "," +deployedBundle;
    }

    public synchronized void removeBundle(String bundleName) {
        String[] bundles = deployedBundles.split(",");
        HashSet<String> bundleSet = new HashSet<String>(Arrays.asList(bundles));
        bundleSet.remove(bundleName.trim());
        deployedBundles = Arrays.toString(bundleSet);
        touch();
    }

    synchronized public void addWorkAssignmentId(String workId) {
        String[] workIds = this.workId.split(",\n");
        List<String> asList = new ArrayList<String>(Arrays.asList(workIds));
        if (!asList.contains(workId)) {
            asList.remove("");
            asList.add(workId);
            Collections.sort(asList);
            this.workId = Arrays.toString(asList).replaceAll(",", ",\n");
            touch();
        }
    }
    synchronized public void removeWorkAssignmentId(String workId){
        String[] workIds = this.workId.split(",\n");
        List<String> asList = new ArrayList<String>(Arrays.asList(workIds));
        if (asList.contains(workId)) {
            asList.remove("");
            asList.remove(workId);
            Collections.sort(asList);
            this.workId = Arrays.toString(asList).replaceAll(",", ",\n");
            touch();
            setLastUsed();
        }
    }
    public String getWorkIds() {
        return this.workId;
    }

    @Override
    public String toString() {
        String workId = this.workId;
        if (workId.length() == 0) workId = "\t";
        return "\tid:" + getResourceId() +  "\tsysId:" + systemId + "\thost:" + hostName + "\twork:" + workId + "\tcpu:" + cpuUtilisation + "\tmflops:" + mflops + "\tos:" + getOsName() + "\tcProps:" + customProperties + " cwd:" + cwd + " last:" + lastUsed + " deployed:" + deployedBundles;
    }

    public String getOwnership() {
        return ownership;
    }

    public void setOwnership(String ownership) {
        this.ownership = ownership;
    }

    public String getCwd() {
        return cwd;
    }

    public void setCwd(String cwd) {
        this.cwd = cwd;
    }

    public String getWorkId() {
        return workId;
    }

    public void setWorkId(String workId) {
        this.workId = workId;
    }

    public void setActiveServiceName(String serviceName) {
        this.activeServiceName = serviceName;
    }

    public void setActiveBundle(String bundleId) {
        this.activeBundleId = bundleId;
        setLastUsed();
    }

    public String getActiveServiceName() {
        return activeServiceName;
    }

    public String getActiveBundleId() {
        return activeBundleId;
    }

    public void setLastUsed() {
        this.lastUsed = Time.nowAsString();
    }

    public void setJmxHttpURL(String jmxHttpURL) {
        this.jmxHttpURL = jmxHttpURL;
    }
    public String getJmxHttpURL() {
        return jmxHttpURL;
    }

    public int getId() {
        return id;
    }

    public void setBootHash(String hash) {
        this.bootHash = hash;
    }

    public String getBootHash() {
        return bootHash;
    }

    public void setAgentId(String id) {
        this.agentId = id;
    }

    public void setType(String type) {
        this.type = type;
    }
    public String getType() {
        return type;
    }

    public void setMflops(int mflops) {
        this.mflops = mflops;
    }

    public void setSystemId(int systemId) {
        this.systemId = systemId;
    }

    public int getSystemId() {
        return systemId;
    }
    public void logStats(Class<?> target, Logger LOGGER){
        try {
            updateValues();
            LOGGER.info(getSystemStats(target.getSimpleName(), getHostName()));
            LOGGER.info(String.format("%s %s ROLE:%s MEM MB MAX:%d COMMITED:%d USED:%d AVAIL:%d", target.getSimpleName(), getHostName(), type, getMemoryMax(), getMemoryCommitted(), getMemoryUsed(), getMemoryAvailable()));

            if (getMemoryAvailable() < 2) {
                LOGGER.warn(String.format("%s %s ROLE:%s MEM MB MAX:%d COMMITED:%d USED:%d LOW MEM ***AVAIL:%d", target.getSimpleName(), getHostName(), type, getMemoryMax(), getMemoryCommitted(), getMemoryUsed(), getMemoryAvailable()));
            }
            if (this.diskFreeSpaceMb < 10) {
                LOGGER.info(String.format("%s %s ROLE:%s CPU:%d MemFree:%d WARN LOW ****DiskFree:%d SwapFree:%d", target.getSimpleName(), getHostName(), type, getCpuUtilisation(), this.physMemFreeMb, this.diskFreeSpaceMb, this.swapFreeMb));
            }
            if (getMemoryAvailable() == 0) {
                LOGGER.fatal(String.format("%s %s ROLE:%s MEM MB MAX:%d COMMITED:%d USED:%d LOW MEM ***AVAIL:%d", target.getSimpleName(), getHostName(), type, getMemoryMax(), getMemoryCommitted(), getMemoryUsed(), getMemoryAvailable()));
            }


        } catch (Throwable t) {
            LOGGER.warn(t);
        }
    }
    public String getSystemStats(String tag, String id) {
        return String.format("%s %s ROLE:%s CPU:%d MemFree:%d MemUsePC:%2.2f DiskFree:%d DiskUsePC:%2.2f SwapFree:%d SwapUsePC:%2.2f",tag, id, type, getCpuUtilisation(), physMemFreeMb, getSysMemUtil(),  diskFreeSpaceMb, getDiskUtil(), swapFreeMb, 100.0 - getPercent(swapFreeMb, swapTotalMb));
    }


    double getPercent(int value, int tt) {
        if (value > tt) tt = value;
        return (((double) value)/ ((double) tt)) * 100.0;
    }

    public String getIpAddress() {
        return this.ipAddress;
    }

    public String getGateway() {
        return gateway;
    }

    public int getPid() {
        return pid;
    }
    public String getLastUpdated() {
        return lastUpdated;
    }

    public List<String> getWorkIdsAsList() {
        String[] split = this.workId.split(",");
        return new ArrayList<String>(Arrays.asList(split));
    }

    public boolean isFileDownloaded(String filename, long modTime, long lengthKb, String fileHash) {
        if(downloadHashes == null) {
            return false;
        }
        String hashEntry = String.format("%s #:%s",filename, fileHash);
        return downloadHashes.contains(hashEntry);
    }

    public String getFileEntry(String filename) {
        String[] results = downloads.split("\n");
        for (String file : results) {
            if (file.startsWith(filename + " ")) {
                return file;
            }
        }
        return "";
    }

    /**
     * update if files we modded in the last X seconds
     * @return
     */
    public boolean updateDownloads(int seconds) {
        File[] listFiles = new File("downloads").listFiles();
        if (listFiles == null) return false;
        boolean update = false;
        long timeFrom = DateTimeUtils.currentTimeMillis() -  (seconds * 1000);

        for (File file : listFiles) {
            if (file.lastModified() >= timeFrom) update = true;
        }
        if (update) {
            this.downloads = getBundleHash("downloads");
            this.downloadHashes = getBundleHashValues("downloads");
            return true;
        }
        return false;


    }

    public String getMemLogMsg(String target) {
        return String.format("COMP:%s %s ROLE:%s MEM MB MAX:%d COMMITED:%d USED:%d AVAIL:%d USED_PC:%2.2f", target,
                getHostName(), type, getMemoryMax(), getMemoryCommitted(), getMemoryUsed(),
                getMemoryAvailable(),
                ((double)getMemoryUsed()/(double)getMemoryMax()) * 100.0);
    }

    public String agentId() {
        return agentId;
    }

    public String getStartTime() {
        return startTime;
    }

    public boolean isManagement() {
        return type.equals("Management") || type.equals("Failover");
    }

    public int getDiskFreeSpaceMb() {
        return diskFreeSpaceMb;
    }
    public int getDiskUsableMb() {
        return diskUsableSpaceMb;
    }
    public int getPhysicalMemTotalMB() {
        return physMemTotalMb;
    }
    public String getFieldValue(String userField) {
        if (userField != null && userField.trim().length() > 0 ) {
            try {
                Field field = ResourceProfile.class.getDeclaredField(userField);
                field.setAccessible(true);
                return field.get(this).toString();
            } catch (Throwable t) {
            }
        }
        return "";
    }
    public List<String> getFields() {
        Field[] fields = Arrays.sortFields(getClass().getDeclaredFields());
        List<String> results = new ArrayList<String>(fields.length);

        for (Field field : fields) {
            results.add(field.getName());
        }
        return results;

    }

    public boolean isIndexer() {
        return !isManagement() && type.endsWith("Indexer");
    }

    public boolean isIndexStore() {
        return !isManagement() && type.endsWith("IndexStore");
    }
}
