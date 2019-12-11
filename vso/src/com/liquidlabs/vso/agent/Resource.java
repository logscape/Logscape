/**
 *
 */
package com.liquidlabs.vso.agent;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.ClientLeaseManager;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.space.lease.Renewer;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.work.WorkAssignment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.zip.CRC32;

public class Resource implements Registrator {
    public static final String MEM_FORMAT = "%s %s MEM MB MAX:%d COMMITED:%d USED:%d AVAIL:%d SysMemFree:%d TimeDelta:%d Cores:%d";
    private ResourceProfile profile;
    private int id;
    private String resourceLease;
    private long lastCrc;
    private Set<String> startedWork = new CopyOnWriteArraySet<>();
    private final ClientLeaseManager leaseManager;
    private final URI endPoint;
    private final ObjectTranslator query = new ObjectTranslator();
    private boolean sendUpdatedProfile;
    private ResourceSpace resourceSpace;
    private Renewer resourceRegistrationRenewer;
    private String currentForegroundAssignment;
    private static final Logger LOGGER = Logger.getLogger(Resource.class);
    private boolean panic;

    public Resource(ClientLeaseManager leaseManager, ResourceProfile resourceProfile, URI endPoint, int id) {
        this.leaseManager = leaseManager;
        profile = resourceProfile;
        this.endPoint = endPoint;
        this.id = id;
    }

    private void log(Level level, String message) {
        LOGGER.log(level, String.format("RESOURCE %s: %s", profile.getResourceId(), message));
    }

    private void log(Level level, String message, Throwable t) {
        LOGGER.log(level, String.format("RESOURCE %s: %s", profile.getResourceId(), message), t);
    }

    public void init(final ResourceSpace resourceSpace, int expires) throws Exception {
        this.resourceSpace = resourceSpace;
        log(Level.INFO, "Registering ResourceId:" + profile.getResourceId() + " " + profile.getSystemId());


        resourceSpace.releaseAllocsForOwner(profile.getResourceId());

        // allow extra minute before renewer kicks in
        register(expires+1);
        resourceRegistrationRenewer = new Renewer(resourceSpace, this, resourceLease, expires, profile.getResourceId(), LOGGER);
        leaseManager.manage(resourceRegistrationRenewer, VSOProperties.getLUSpaceServiceRenewInterval());
    }

    public void updateValues() {
        profile.updateValues();
    }
    public void updateValues(ResourceProfile source) {
        profile.updateValues(source);
    }


    public void initProfile() {
        profile.oneOffUpdate();
        profile.setPort(endPoint.getPort());
        profile.setEndPoint(endPoint);

    }

    public boolean updateResourceSpace(boolean force) throws Exception {
        if (resourceRegistrationRenewer == null) return false;
        long newCrc = getCRC(profile);

        if (lastCrc == newCrc && !force) {
            return false;
        } else {
            lastCrc = newCrc;
            // will register/update the resourceSpace with the new details
            resourceRegistrationRenewer.register();
            sendUpdatedProfile = false;
            return true;
        }
    }

    private synchronized void register(int expires) {
        try {
            if (LOGGER.isDebugEnabled()) LOGGER.debug(" >>> Register:" + this.id);
            resourceLease = resourceSpace.registerResource(profile, expires);
            panic = false;
        } catch (Throwable e) {
            log(Level.WARN,">>> Register (RETRY):" + this.id,e);

            try {
                log(Level.WARN,String.format("Failed to register resource Attempt:1 %s e:%d", profile.getResourceId(), expires),e);
                resourceLease = resourceSpace.registerResource(profile, expires);
                panic = false;
            } catch (Throwable e2) {
                log(Level.WARN,String.format("Failed to register resource Attempt:2 %s e:%d", profile.getResourceId(), expires),e2);
            }

            panic = true;
        } finally {
            if (LOGGER.isDebugEnabled()) LOGGER.debug(" <<< Register:" + this.id);
        }
    }

    public boolean isRunning(String id) {
        return startedWork.contains(id);
    }

    synchronized public void addStarted(String id) {
        profile.setLastUsed();
        profile.addWorkAssignmentId(id);
        startedWork.add(id);

    }

    public long getCRC(ResourceProfile string){
        long updateMs = profile.lastUpdatedMs;
        String updateString = profile.lastUpdated;
        String lastUsed = profile.lastUsed;

        profile.lastUpdatedMs = 0;
        profile.lastUpdated = "";
        profile.lastUsed = "";

        String currentResourceProfile = query.getStringFromObject(profile);

        CRC32 crc32 = new CRC32();
        crc32.update(currentResourceProfile.getBytes());

        profile.lastUpdatedMs = updateMs;
        profile.lastUpdated = updateString;
        profile.lastUsed = lastUsed;
        long value = crc32.getValue();
//		dumpProfileDelta(currentResourceProfile, value);
        this.lastRPString = currentResourceProfile;

        return value;
    }
    private String lastRPString;

    /**
     * Used to figure out what has made the CRC value change
     * @param currentResourceProfile
     * @param value
     * @return
     */
    private long dumpProfileDelta(String currentResourceProfile, long value) {
        if (this.lastRPString != null && lastCrc != value){
            System.out.println(query.getAsCSV(profile));

            StringBuilder sb1 = new StringBuilder();
            sb1.append(profile.getResourceId()).append("\n");
            sb1.append(lastRPString).append("\n");
            sb1.append(currentResourceProfile).append("\n");
            for (int i = 0; i < lastRPString.length(); i++) {
                if (i >= currentResourceProfile.length()) continue;
                if (lastRPString.charAt(i) != currentResourceProfile.charAt(i)) {
                    sb1.append(lastRPString.charAt(i));
                } else {
                    sb1.append("-");
                }
            }
            sb1.append("\n");
            sb1.append("\n");
            sb1.append("\n");
            System.out.println(sb1.toString());
        }
        return value;
    }

    public boolean hasStarted(String workId) {
        return startedWork.contains(workId);
    }

    synchronized public void cleanUpAssignment(WorkAssignment workAssignment) {
        boolean removed = startedWork.remove(workAssignment.getId());
        if (removed) {
            removeWorkId(workAssignment.getId());
            if (profile.getActiveServiceName().equals(workAssignment.getServiceName())) {
                profile.setActiveServiceName("");
                profile.setActiveBundle("");
            }
            if (workAssignment.getId().equals(currentForegroundAssignment)) {
                currentForegroundAssignment = null;
            }
            // let the BG update happen on the timer - this affects BG task allocation
            // so should not be forced (in case another BG task is being switched in place of this one)
            if (!workAssignment.isBackground()) sendUpdatedProfile = true;
        }
    }

    void removeWorkId(String workId) {
        profile.removeWorkAssignmentId(workId);
    }

    public void terminated(WorkAssignment workAssignment, boolean isFault) {
        if (resourceSpace != null) {
            if (!workAssignment.isBackground()) resourceSpace.forceFreeResourceAllocation("", "ResourceTerminated-" + workAssignment.getId(), profile.getResourceId());
            cleanUpAssignment(workAssignment);
        }
    }

    public void updated() {
        sendUpdatedProfile = true;
    }

    public void forcedUpdate() {
        if (sendUpdatedProfile) {
            try {
                updateResourceSpace(true);
            } catch (Exception e) {
                LOGGER.warn("Update failed", e);
            }
        }

    }

    public void addDeployedBundle(String bundleName) {
        profile.setDeployedBundles(bundleName);
        sendUpdatedProfile = true;
    }

    public void removeBundle(String bundleName) {
        profile.removeBundle(bundleName);
        sendUpdatedProfile = true;
    }

    public String agentId() {
        return profile.agentId;
    }
    public int id(){
        return id;
    }


    transient ThreadMXBean threadFactory = ManagementFactory.getThreadMXBean();

    public ResourceProfile profile() {
        return profile;
    }

    public void unregister() {
        resourceSpace.unregisterResource("", profile.getResourceId());
    }

    public void setBootHash(String hash) {
        profile.setBootHash(hash);

    }

    public String register() {
        register(180);
        return resourceLease;
    }
    public String info() {
        return resourceSpace.toString();
    }

    public void setForegroundAssignment(String workAssignmentId) {
        currentForegroundAssignment = workAssignmentId;
    }

    public boolean isPanic() {
        return panic;
    }

    public void stop() {
        resourceRegistrationRenewer = null;
        resourceSpace = null;

    }

    public void updateDownloads() {
        sendUpdatedProfile = this.profile.updateDownloads(10) || sendUpdatedProfile;
    }

    public void registrationFailed(int failedCount) {
    }
}