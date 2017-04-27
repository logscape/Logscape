package com.liquidlabs.vso.resource;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.liquidlabs.common.Logging;
import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.Pair;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.orm.Id;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.Notifier;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.work.InvokableImpl;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class ResourceSpaceImpl implements ResourceSpace {

    public static String TAG = "RESOURCE";
    private static String CONFIG_START = "<!-- RESOURCE Config Start -->";
    private static String CONFIG_END = "<!-- RESOURCE Config End -->";


    private final static Logger LOGGER = Logger.getLogger(ResourceSpace.class);
    private final static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "ResourceSpace");

    private final SpaceService resourceSpace;
    private final SpaceService allocSpace;
    private final SpaceService allResourcesEver;
    private Map<String, RequestEventListener> requestEventListeners = new ConcurrentHashMap<String, RequestEventListener>();

    Boolean isFairshare = VSOProperties.getResourceFairShare();

    Map<String, String> allocationLeases = new ConcurrentHashMap<String, String>();
    Map<String, AllocListener> allocOwners = new ConcurrentHashMap<String, AllocListener>();

    protected ResourceProfile resourceProfile;

    Pair<Integer, Integer> pl;
    private long llc = -1;

    public ResourceSpaceImpl(SpaceService resourceSpace,
                             SpaceService allocSpace, SpaceService allResourcesEver) {
        this.resourceSpace = resourceSpace;
        this.allocSpace = allocSpace;
        this.allResourcesEver = allResourcesEver;
        LOGGER.info("FairShare is:" + isFairshare);
    }

    public String exportConfig(String filter) {
        return allResourcesEver.exportObjectAsXML(filter, CONFIG_START, CONFIG_END);
    }


    public void importConfig(String xmlConfig) {
        allResourcesEver.importFromXML(xmlConfig, true, true, CONFIG_START, CONFIG_END);
    }

    public void start() {
        allResourcesEver.start(this, "boot-1.0");
        allocSpace.start(this, "boot-1.0");
        resourceSpace.start(this, "boot-1.0");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.warn("Shutdown:" + ResourceSpaceImpl.this.toString());
                ResourceSpaceImpl.this.stop();
            }
        });

        registerResourceGroup(new ResourceGroup("Dedicated", "mflops > 99 AND maxPowerDraw < 200", "Dedicated Servers", new Date().toString()));
        registerResourceGroup(new ResourceGroup("Harvested", "mflops > 99 AND maxPowerDraw < 200", "Borrowed Idle Servers", new Date().toString()));
        registerResourceGroup(new ResourceGroup("Low End Desktops", "mflops < 99", "Generally used for low-risk work", new Date().toString()));
        registerResourceGroup(new ResourceGroup("Scavenged Servers Win32", "osName contains 'WINDOWS' AND mflops < 300 AND coreCount <= 4", "Generally used for low-risk work", new Date().toString()));
        registerResourceGroup(new ResourceGroup("Scavenged Servers Win64", "osName contains 'X64' AND mflops < 300 AND coreCount <= 4", "Generally used for low-risk work", new Date().toString()));
        registerResourceGroup(new ResourceGroup("Scavenged Servers Linux", "osName contains LINUX AND mflops < 300 AND coreCount <= 4", "Generally used for low-risk work", new Date().toString()));
        registerResourceGroup(new ResourceGroup("Linux", "osName contains LINUX", "Dedicated", new Date().toString()));

        resourceSpace.getScheduler().scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (resourceProfile == null) {
                    resourceProfile = new ResourceProfile();
                    resourceProfile.oneOffUpdate();
                }
                resourceProfile.logStats(ResourceSpace.class, LOGGER);
            };
        }, 60 - new DateTime().getSecondOfMinute(), 5 * 60, TimeUnit.SECONDS);

        resourceSpace.getScheduler().scheduleAtFixedRate(new Runnable() {
            public void run() {
                String[] ids = resourceSpace.findIds(ResourceProfile.class, "id = 0");
                auditLogger.emit("AgentCount", Integer.toString(ids.length));
            };
        }, 5, VSOProperties.getAgentCountAuditSchedule(), TimeUnit.MINUTES);




        // Leases Space is not persisted - so reinstate the leases for 7 days
        String[] resourceIds = allResourcesEver.findIds(ResourceId.class, "");
        for (String resourceIdKey : resourceIds) {
            ResourceId resourceId = allResourcesEver.findById(ResourceId.class, resourceIdKey);
            allResourcesEver.store(resourceId, VSOProperties.getLostResourceTimeout());
        }

        resourceSpace.registerListener(ResourceProfile.class, "id == 0", new Notifier<ResourceProfile>(){
            public void notify(Type event, ResourceProfile result) {
                LOGGER.info(String.format("%s ResourceExpired:%s Host:%s Type:%s", TAG, result.getResourceId(), result.getHostName(), result.getType()));
                String leftJoined = event == Type.WRITE ? "Joined" : "Left";
                auditLogger.emit("Agent:" + leftJoined, " Host:" + result.getHostName() + " Role:" + result.getType());
                // An agent left - print out hte new agent count
                String[] ids = resourceSpace.findIds(ResourceProfile.class, "id = 0");
                auditLogger.emit("AgentCount", Integer.toString(ids.length));

            };
        }, "ExpireListener", -1, new Type[] { Type.WRITE, Type.TAKE });

    }

    public void stop() {
        try {
            LOGGER.info("LS_EVENT:Stopping " + ResourceSpaceImpl.class.getSimpleName());
            resourceSpace.stop();
            allocSpace.stop();
            allResourcesEver.stop();
        } catch (Throwable t) {
        }
    }

    public String registerResource(ResourceProfile resourceProfile, int expires) throws Exception {

        // only allows Fwdrs to register
        if (LOGGER.isDebugEnabled()) LOGGER.info("RegisterResource:" + resourceProfile.getAgentId());


        String store = resourceSpace.store(resourceProfile, expires);
        allResourcesEver.store(new ResourceId(resourceProfile.getResourceId(), resourceProfile.getIpAddress(), resourceProfile.getLastUpdated(), resourceProfile.getType()), VSOProperties.getLostResourceTimeout());
        return store;
    }

    public void unregisterResource(String owner, String resourceId) {
        LOGGER.info("UnregisterResource:" + resourceId);

        // if owner is specified then make sure we validate they can remove it
        if (owner != null && owner.length() > 0) {
            Allocation toRemove = allocSpace.findById(Allocation.class, Allocation.getId(resourceId, AllocType.ALLOCATED));
            if (!toRemove.getOwnerId().equals(owner))
                return;
        }

        ResourceProfile resource = resourceSpace.findById(
                ResourceProfile.class, resourceId);
        if (resource != null)
            LOGGER.info(String.format("%s freeResource id:%s service:%s:%s",
                    TAG, resource.getResourceId(),
                    resource.getActiveBundleId(), resource
                    .getActiveServiceName()));
        resourceSpace.remove(ResourceProfile.class, resourceId);
        allocSpace.purge(Allocation.class, "resourceId equals " + resourceId);
    }

    /**
     * Used when an owner has released an Allocation - this method translates
     * the release onto the demand-based/pending Allocations
     */
    public void releasedResources(List<String> resourceIds) {
        LOGGER.info(String.format("%s ReleasedResources%s", TAG, resourceIds));
        for (String resourceId : resourceIds) {

            // remove pending alloc
            Allocation pendingAllocation = allocSpace.remove(Allocation.class,
                    Allocation.getId(resourceId, AllocType.PENDING));

            if (pendingAllocation == null) {
                int retry = 0;
                while (pendingAllocation == null
                        && retry++ < VSOProperties
                        .getResourceReleasedPendWaitCount()) {
                    LOGGER
                            .info(String
                                    .format(
                                            "Release PEND-NULL Alloc Resource[%s], releasing id[%s]",
                                            resourceId, Allocation.getId(
                                            resourceId,
                                            AllocType.PENDING)));
                    pendingAllocation = allocSpace.remove(Allocation.class,
                            Allocation.getId(resourceId, AllocType.PENDING));
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug(String.format(
                                "%s waiting for allocation object..%d", TAG,
                                retry));
                    pause();
                }
                if (pendingAllocation == null) {
                    LOGGER
                            .info(String
                                    .format(
                                            "Release Did not find PEND Alloc Resource[%s], releasing id[%s]",
                                            resourceId, Allocation.getId(
                                            resourceId,
                                            AllocType.PENDING)));
                    allocSpace.remove(Allocation.class, Allocation.getId(
                            resourceId, AllocType.ALLOCATED));
                    continue;
                }
            }

            // remove existing allocations - [take]
            Allocation existingAlloc = allocSpace.remove(Allocation.class,
                    Allocation.getId(resourceId, AllocType.ALLOCATED));
            if (existingAlloc != null) {
                LOGGER.info(String.format("%s Released[%s] owner[%s] work[%s]",
                        TAG, resourceId, existingAlloc.owner,
                        existingAlloc.workIdIntent));
                this.allocationLeases.remove(existingAlloc.getId());
            }

            String existingOwner = existingAlloc != null ? existingAlloc.owner
                    : "none";
            String pendOwner = pendingAllocation != null ? pendingAllocation.owner
                    : "none";
            int allocTimeout = pendingAllocation != null ? pendingAllocation.timeoutSeconds
                    : 180;

            // convert allocation from PENDING to ALLOCATED and put into space
            LOGGER.info(String.format(
                    "releasedResources Assign from(%s) %s => %s",
                    existingOwner, resourceId, pendOwner));
            pendingAllocation.update(AllocType.ALLOCATED);
            String leaseKey = allocSpace.store(pendingAllocation, allocTimeout);
            allocSpace.assignLeaseOwner(leaseKey, pendingAllocation.owner);
        }
    }

    private void pause() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Used on free-resources
     *
     * @return
     */
    public void assignResources(String requestId, List<String> availableResources, String owner, int priority, String intent, int leaseTimeout) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("  %s %s assignResources count[%d] resources%s work[%s]", TAG, requestId, availableResources.size(), availableResources, intent));

        for (String availableResource : availableResources) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("  %s %s ALLOC == %s => %s p[%d][%s]", TAG, requestId, availableResource, owner, priority, intent));
            Allocation allocation = new Allocation(requestId, availableResource, AllocType.ALLOCATED, owner, priority, intent, DateTimeUtils.currentTimeMillis(),
                    DateTimeFormat.shortDateTime().print(DateTimeUtils.currentTimeMillis()), leaseTimeout);
            String leaseKey = allocSpace.store(allocation, allocation.timeoutSeconds);
            if (leaseKey != null) {
                if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("  %s %s ALLOC Lease Resource[%s] Owner[%s] => Lease[%s] timeout[%d]", TAG, requestId, availableResource, owner, leaseKey, allocation.timeoutSeconds));
                allocationLeases.put(allocation.id, leaseKey);
                allocSpace.assignLeaseOwner(leaseKey, allocation.owner);
            }
        }
    }

    public void forceFreeResourceAllocation(String owner, String requestId, String resourceId) {
        LOGGER.info(String.format("%s %s forceFreeing Allocation for resource:%s", TAG, requestId, resourceId));
        if (!allocSpace.containsKey(Allocation.class, Allocation.getId(resourceId, AllocType.ALLOCATED))) {
            LOGGER.info("Cannot release non-existent ALLOC:" + Allocation.getId(resourceId, AllocType.ALLOCATED));
            // System.err.println(com.liquidlabs.common.collection.Arrays.toString(allocSpace.findIds(Allocation.class,
            // "")));
            return;
        }

        if (owner != null && owner.length() > 0) {
            Allocation toRemove = allocSpace.findById(Allocation.class, Allocation.getId(resourceId, AllocType.ALLOCATED));
            if (!toRemove.getOwnerId().equals(owner))
                return;
        }

        ResourceProfile resource = resourceSpace.findById(ResourceProfile.class, resourceId);
        if (resource != null)
            LOGGER.info(String.format("%s freeResourceAlloc id:%s service:%s:%s", TAG, resource.getResourceId(), resource.getActiveBundleId(), resource.getActiveServiceName()));

        allocSpace.updateMultiple(Allocation.class, "id equals " + Allocation.getId(resourceId, AllocType.ALLOCATED), "requestId replaceWith " + requestId, 1, -1, null);
        Allocation remove = allocSpace.remove(Allocation.class, Allocation.getId(resourceId, AllocType.ALLOCATED));
        if (remove == null) {
            LOGGER.warn("Failed to release Allocation for resource:" + resourceId);
        } else {
            this.allocationLeases.remove(remove.getId());
        }
    }

    public String getLeaseForAllocation(String allocId) {
        return allocationLeases.get(Allocation.getId(allocId,
                AllocType.ALLOCATED));
    }

    /**
     * Return number of BG resources satisfied - different in that we track and
     * rotate the list on resources given to a consumer
     */
    Map<String, Set<String>> bgListForOwner = new ConcurrentHashMap<String, Set<String>>();

    public void releasedBGResources(String TAG, List<String> resourceIds, String owner) {
        Set<String> list = getBGOwnerList(owner);
        list.removeAll(resourceIds);
    }

    public int requestBGResources(String requestId, int count, int priority,
                                  String template, String workIntent, int timeoutSeconds,
                                  String owner, String ownerLabel) {
        Set<String> trackingSet = getBGOwnerList(owner);

        int result = count;
        // if the pending count for this resource is excessive - do nothing;
        // return count
        List<String> suitableResourceIds = findResourceIdsBy(template);
        if (trackingSet.size() == suitableResourceIds.size()) {
            trackingSet.clear();
        }
        suitableResourceIds.removeAll(trackingSet);
        if (suitableResourceIds.size() == 0)
            return 0;

        ArrayList<String> newAllocs = new ArrayList<String>(suitableResourceIds
                .subList(0, count));
        trackingSet.addAll(newAllocs);
        AllocListener allocListener = allocOwners.get(owner);
        if (allocListener == null) {
            LOGGER.warn("Failed to find AllocListener for Owner[" + owner
                    + "] listeners:" + allocOwners.keySet());
            return 0;
        }
        allocListener.add(requestId, newAllocs, ownerLabel, priority);
        return result;

    }

    private Set<String> getBGOwnerList(String owner) {
        if (!bgListForOwner.containsKey(owner)) {
            bgListForOwner.put(owner, new HashSet<String>());
        }
        return bgListForOwner.get(owner);
    }

    /**
     * Return number of resources satisfied - used to allocate foreground tasks
     */
    synchronized public int requestResources(String requestId, int count, int priority, String template, String workIntent, int timeoutSeconds, String owner, String ownerLabel) {

        if (count == -1)
            count = Integer.MAX_VALUE;
        int result = count;

        // if the pending count for this resource is excessive - do nothing;
        // return count
        List<String> suitableResourceIds = findResourceIdsBy(template);

        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s %s RequestResources >>>> Template[%s] Owner[%s] Work[%s] P[%d]  Match[%d]==%s", TAG, requestId, template, owner, workIntent, priority,
                suitableResourceIds.size(), suitableResourceIds));
        if (suitableResourceIds.size() == 0) {
            LOGGER.info(String.format("  No suitables resources for template:%s resourceCount:%d", template, resourceSpace.size()));
            return 0;
        }

        Set<String> pending = new HashSet<String>(getPendingAllocs().keySet());

        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s %s RequestResources --- PENDING%s", TAG, requestId, pending));

        int avail = assignAvailableResources(requestId, suitableResourceIds, pending, priority, count, owner, workIntent, timeoutSeconds);
        count -= avail;

        int lower = assignLowerPriorityResources(requestId, suitableResourceIds, pending, priority, count, owner, workIntent, timeoutSeconds);
        count -= lower;

        int equal = assignEqualPriorityResources(requestId, suitableResourceIds, pending, priority, count, owner, workIntent, timeoutSeconds);
        count -= equal;
        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s %s RequestResources <<<< giving:[%d %d %d ] delta:%d", TAG, requestId, avail, lower, equal, result - count));

        fireRequestEvent(requestId, template, priority, result - count, owner, ownerLabel);
        return result - count;
    }

    public void releaseAllocsForOwner(String ownerId) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("ReleaseALLOCForOwner:" + ownerId);
        List<Allocation> remove = allocSpace.remove(Allocation.class, "owner equals " + ownerId, -1);
        for (Allocation allocation : remove) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Removed:" + allocation);
        }
        if (remove.size() == 0) {
            List<Allocation> remove2 = allocSpace.remove(Allocation.class, "resourceId equals " + ownerId + " AND owner contains BH_DPLY", -1);
            if (remove2.size() > 0)
                if (LOGGER.isDebugEnabled()) LOGGER.debug("ReleasedALLOC: resourceId:" + ownerId + "/" + remove2);
        }
    }

    public boolean isStarted() {
        return resourceSpace.isStarted();
    }

    private void fireRequestEvent(final String requestId,
                                  final String template, final int priority, final int count,
                                  final String owner, final String ownerLabel) {
        for (final String listenerKey : requestEventListeners.keySet()) {
            allocSpace.getScheduler().submit(new Runnable() {
                public void run() {
                    RequestEventListener listener = requestEventListeners
                            .get(listenerKey);
                    try {
                        listener.requested(requestId, owner, ownerLabel,
                                template, priority, count);
                    } catch (Throwable t) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                        try {
                            listener.requested(requestId, owner, ownerLabel,
                                    template, priority, count);
                        } catch (Exception e) {
                            LOGGER.warn("Removing Event Listener, e:"
                                    + e.toString());
                            requestEventListeners.remove(listenerKey);
                        }

                    }
                }
            });

        }

    }

    Map<String, Allocation> getPendingAllocs() {
        HashMap<String, Allocation> results = new HashMap<String, Allocation>();
        List<Allocation> pending = allocSpace.findObjects(Allocation.class,
                "type equals 'PENDING'", false, -1);
        for (Allocation allocation : pending) {
            results.put(allocation.resourceId, allocation);
        }
        return results;
    }

    /**
     * Assign free resources and the unsatisfied count
     *
     * @param requestId
     * @param pending
     */
    int assignAvailableResources(String requestId, List<String> resourceIds, Set<String> pending, int priority, int count, String owner, String workIntent, int leaseTimeoutSeconds) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s %s AssignAvailableResources", TAG, requestId));

        ArrayList<String> available = new ArrayList<String>(resourceIds);

        List<Allocation> existingAllocs = allocSpace.findObjects(Allocation.class, "resourceId containsAny " + available.toString().replaceAll("\\[", "'").replaceAll("\\]", "'"), false, -1);
        for (Allocation alloc : existingAllocs) {
            try {
                available.remove(alloc.resourceId);
            } catch (Throwable t) {
                LOGGER.error("******* Bad ALLOC:" + alloc, t);
            }
        }
        available.removeAll(pending);

        if (available.size() > 0) {
            available = new ArrayList<String>(available.subList(0, Math.min(count, available.size())));
        }

        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("  %s %s count[%d] AssignAvailableResources:%s ExistingAllocs:%d Pending:%s", TAG, requestId, count, available, existingAllocs.size(), pending));
        if (available.size() == 0 && existingAllocs.size() > 0) {
            int cc = 0;
            for (Allocation allocation : existingAllocs) {
                if (cc++ < 4)
                    LOGGER.info("\t" + allocation);
            }
        }
        assignResources(requestId, available, owner, priority, workIntent, leaseTimeoutSeconds);

        return available.size();
    }

    /**
     * For those owners with lower allocIds, request a split count from each one
     * and provide the list from which it needs to choose
     *
     * @param pending
     *            TODO
     */
    int assignLowerPriorityResources(String requestId, List<String> suitableResourceIds, Set<String> pending, int priority, int count, String owner, String workIntent, int leaseTimeoutSeconds) {
        if (count == 0)
            return 0;
        if (LOGGER.isDebugEnabled())
            LOGGER.debug(String.format("%s %s AssignLowerPriorityResources P[%d] C[%d]", TAG, requestId, priority, count));

        List<Allocation> existingAllocs = allocSpace.findObjects(Allocation.class, "owner notEquals " + owner + " AND priority < " + priority + " AND type equals " + AllocType.ALLOCATED, false, -1);

        // Cannot sort as it may upset precendence ordering of the
        // suitableResourceIds i.e. A OR B OR C - the requester must be able
        // torequest A before they want B or C
        // Collections.sort(existingAllocs, new Comparator<Allocation>(){
        // public int compare(Allocation o1, Allocation o2) {
        // return
        // Integer.valueOf(o2.priority).compareTo(Integer.valueOf(o1.priority));
        // }
        // });

        // group them into lists by owner
        HashMap<String, List<String>> releaseMap = new HashMap<String, List<String>>();
        for (Allocation allocation : existingAllocs) {

            // exclude those resourceIds that dont fit our template
            if (!suitableResourceIds.contains(allocation.resourceId))
                continue;
            if (!releaseMap.containsKey(allocation.owner)) {
                releaseMap.put(allocation.owner, new ArrayList<String>());
            }

            releaseMap.get(allocation.owner).add(allocation.resourceId);
        }
        if (LOGGER.isDebugEnabled())
            LOGGER.debug(String.format("  %s %s LOWER Priorty Allocs%s", TAG, requestId, existingAllocs));

        // ask for each owner to release a proportion of the given set - maybe
        // all of them
        int totalReleaseCount = 0;
        for (String ownerKey : releaseMap.keySet()) {
            List<String> thisResourceIds = releaseMap.get(ownerKey);
            thisResourceIds.removeAll(pending);
            int releaseCount = Double.valueOf(existingAllocs.size() * (double) ((double) thisResourceIds.size() / (double) existingAllocs.size())).intValue();

            if (releaseCount == 0) {
                LOGGER.warn(String.format("     %s %s >> Got Zero Release Count for calc: eSize:%s tResIds:%s", TAG, requestId, existingAllocs.size(), thisResourceIds.size()));
                releaseCount = count;
            }
            if (totalReleaseCount > count)
                continue;

            // HACK - need to sort out remainders
            releaseCount = Math.min(releaseCount, count);
            totalReleaseCount += releaseCount;

            if (LOGGER.isDebugEnabled())
                LOGGER.debug(String.format("  %s %s count:%d LOWER Priority <p[%d] - ReleaseResource:%s from:%s ReleaseCount:%d", TAG, requestId, count, priority, ownerKey, thisResourceIds,
                        releaseCount));
            List<String> goingToRelease = getOwner(ownerKey).release(requestId, thisResourceIds, releaseCount);
            LOGGER.info(String.format("  %s %s count:%d LOWER Priority <p[%d] - Owner[%s] ReleasingResource:%s", TAG, requestId, count, priority, ownerKey, goingToRelease));
            setupPendingAllocsFor(requestId, priority, owner, workIntent, goingToRelease, leaseTimeoutSeconds);
            pending.addAll(goingToRelease);
        }
        return totalReleaseCount;
    }

    private void setupPendingAllocsFor(String requestId, int priority,
                                       String owner, String workIntent, List<String> goingToRelease,
                                       int leaseTimeoutSeconds) {
        for (String resourceGoingToRelease : goingToRelease) {
            Allocation allocation = new Allocation(requestId,
                    resourceGoingToRelease, AllocType.PENDING, owner, priority,
                    workIntent, DateTimeUtils.currentTimeMillis(),
                    DateTimeFormat.shortDateTime().print(
                            DateTimeUtils.currentTimeMillis()),
                    leaseTimeoutSeconds);
            String lease = allocSpace.store(allocation, leaseTimeoutSeconds);
            if (lease != null) {
                allocSpace.assignLeaseOwner(lease, owner);
            }
        }
    }

    int assignEqualPriorityResources(String requestId, List<String> suitableResourceIds, Set<String> pending, int priority, int count, String owner, String workIntent, int leaseTimeoutSeconds) {
        if (count == 0)
            return 0;

        LOGGER.info(String.format("%s %s AssignEqualsPriorityResources P[%d] Count[%d]", TAG, requestId, priority, count));

        // find items with equal priority - include PENDING AND ALLOCATED items
        List<Allocation> samePriorityAllocs = allocSpace.findObjects(Allocation.class, "priority == " + priority + " AND type equals 'ALLOCATED'", false, -1);

        if (samePriorityAllocs.size() == 0) {
            LOGGER.info(String.format("     %s %s SamePriorityAlloc == 0", TAG, requestId));
            return 0;
        }

        HashMap<String, Allocation> actualAllocations = new HashMap<String, Allocation>();
        for (Allocation allocation : samePriorityAllocs) {
            // Ignore ALL Allocs that have a PENDING
            if (allocation == null || allocation.getResourceId() == null) {
                LOGGER.warn("Got Bad ALLOCATION:" + allocation);
                continue;
            }

            // only count resources in the suitableResourceIds list
            if (suitableResourceIds.contains(allocation.getResourceId())) {
                actualAllocations.put(allocation.getResourceId(), allocation);
            }
        }

        // this value is key in breaking down allocs/owner to valid sizes
        int totalInterestSize = actualAllocations.size();

        Map<String, List<Allocation>> ownerAllocs = getOwnerAllocations(actualAllocations.values(), owner);

        int releaseCount = 0;

        // figure out the correct balance
        int validSizePerOwner = actualAllocations.values().size() / ownerAllocs.keySet().size();

        // When fairshare if OFF - owners have to release count/ownerCount each
        if (!isFairshare) {
            int thisOwnerTotal = ownerAllocs.get(owner).size() + count;
            int otherOwnerCount = ownerAllocs.keySet().size() == 1 ? 1 : ownerAllocs.keySet().size() - 1;
            int validSizePerOwner2 = totalInterestSize / otherOwnerCount;
            int validCountToRelease = thisOwnerTotal / otherOwnerCount;

            validSizePerOwner = (validSizePerOwner2 - validCountToRelease);
            LOGGER.info(String.format("%s %s NoFairShare == thisOwnerTotal[%d] thisOwner[%d] otherOwner[%d] vSizePerOwner2[%d]   vCToRelease[%d] final[%d] totalAllocs[%d] suitableRiDs[%d]", TAG,
                    requestId, thisOwnerTotal, ownerAllocs.get(owner).size(), otherOwnerCount, validSizePerOwner2, validCountToRelease, validSizePerOwner, totalInterestSize, suitableResourceIds.size()));
        }

        // need to allow for non-exact match, i.e. 21 divided between 2
        // resources
        int remainder = isFairshare ? totalInterestSize % ownerAllocs.keySet().size() : 0;

        if (ownerAllocs.get(owner).size() > validSizePerOwner && isFairshare) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug(String.format(" %s %s << Owner[%s] share is:%d maxPerOwneris[%d]", TAG, requestId, owner, ownerAllocs.get(owner).size(), validSizePerOwner));
            return 0;
        }
        LOGGER.info(String.format("  %s %s EQUALS based rebalance, owners[%d] validSizePerOwner[%d] total[%d] allocs[%d] remainder[%d]", TAG, requestId,
                ownerAllocs.keySet().size(), validSizePerOwner, totalInterestSize, actualAllocations.size(), remainder));

        int ownerPos = 0;
        for (String cOwner : ownerAllocs.keySet()) {
            if (cOwner.equals(owner))
                continue;
            int reclaimCount = ownerAllocs.get(cOwner).size() - validSizePerOwner;
            // if there is an inexact match, allow reclaims from the first set
            // of owners,
            // i.e. 200 allocs between 11 resources == 18.1 - correct value is
            // 18 or 19 depending on ownerPos, we want to reclaim least amount
            if (ownerPos++ < remainder)
                reclaimCount--;
            if (reclaimCount < 1) {
                LOGGER.info(String.format("  %s %s reclaim:", TAG, requestId, reclaimCount));
                continue;
            }

            // TODO: fix me properly
            if (reclaimCount > count)
                reclaimCount = count;

            LOGGER.info(String.format(" %s %s >> EQUALS Owner %s PendingRELEASE <<== reclaimCount[%d] ownerAllocs[%d] forNewOwner:%s\n\t\t%s\n\t%s ", TAG, requestId, cOwner,
                    reclaimCount, ownerAllocs.get(cOwner).size(), getOwner(cOwner).toString(), owner,
                    getResourceIds(ownerAllocs.get(cOwner))));
            List<String> goingToRelease = new ArrayList<String>();
            try {
                List<Allocation> ownerAllocss = new CopyOnWriteArrayList<Allocation>(ownerAllocs.get(cOwner));

                // bit of a HA hack - we seem to end up with duplicate requests coming in so only release when something different is asked for...
                for (Allocation allocation : ownerAllocss) {
                    if (allocation.getWorkIdIntent().equals(workIntent)) ownerAllocss.remove(allocation);
                }
                if (ownerAllocss.size() == 0) {
                    LOGGER.warn("Not releasing anything to owner:" + owner + " because same work is running already");
                } else {
                    List<String> resourceIdsToRelease = getResourceIds(ownerAllocss);
                    resourceIdsToRelease.removeAll(pending);
                    goingToRelease = getOwner(cOwner).release(requestId, resourceIdsToRelease, reclaimCount);
                }
            } catch (Throwable t) {
                LOGGER.error("Consumer is misbehaving:" + t.toString(), t);
            }
            LOGGER.info(String.format(" %s %s << EQUALS cOwner %s PendingRELEASE <<== %s allocSize[%d] ", TAG, requestId, cOwner, goingToRelease, ownerAllocs.get(cOwner).size()));
            setupPendingAllocsFor(requestId, priority, owner, workIntent, goingToRelease, leaseTimeoutSeconds);
            pending.addAll(goingToRelease);
            releaseCount += goingToRelease.size();
        }

        return releaseCount;
    }

    private List<String> getResourceIds(List<Allocation> list) {
        ArrayList<String> result = new ArrayList<String>();
        for (Allocation alloc : list) {
            result.add(alloc.resourceId);
        }
        return result;
    }

    private Map<String, List<Allocation>> getOwnerAllocations(
            Collection<Allocation> actualAllocations, String owner) {
        HashMap<String, List<Allocation>> result = new HashMap<String, List<Allocation>>();
        result.put(owner, new ArrayList<Allocation>());
        for (Allocation allocation : actualAllocations) {
            if (!result.containsKey(allocation.owner))
                result.put(allocation.owner, new ArrayList<Allocation>());
            result.get(allocation.owner).add(allocation);
        }
        return result;
    }

    private AllocListener getOwner(String owner) {
        if (!this.allocOwners.containsKey(owner))
            throw new RuntimeException("AllocOwner not found:" + owner);
        return this.allocOwners.get(owner);
    }

    public int getResourceCount(String template) {
        return resourceSpace.count(ResourceProfile.class, template);
    }

    public List<String> findResourceIdsBy(String template) {
        try {
            if (template == null)
                template = "";
            // expand template with resource groups being broken out
            template = expandResourceGroups(template);

            // use expanded resource template to retrieve resourceIds
            return com.liquidlabs.common.collection.Arrays.asList(resourceSpace
                    .findIds(ResourceProfile.class, template));
        } catch (Throwable t) {
            LOGGER.warn(t.toString(), t);
            return new ArrayList<String>();
        }
    }

    String expandResourceGroups(String preferredResourcesTemplates) {
        StringBuilder result = new StringBuilder();
        String[] split = preferredResourcesTemplates.split(" ");
        for (String part : split) {
            if (part.startsWith("group(")) {
                part = removeGroupBrackets(part);
                ResourceGroup findResourceGroup = findResourceGroup(part);
                if (findResourceGroup == null) {
                    LOGGER.warn(String.format(
                            "Failed to find Zone:%s From query[%s]",
                            part, preferredResourcesTemplates));
                } else {
                    part = findResourceGroup.getResourceSelection();
                }
            }
            result.append(part).append(" ");
        }

        return result.toString();
    }

    private String removeGroupBrackets(String part) {
        part = part.replaceAll("group\\('", "");
        part = part.replaceAll("group\\(", "");
        part = part.replaceAll("'\\)", "");
        part = part.replaceAll("\\)", "");
        return part;
    }

    public void unregisterRequestEventListener(String listenerId) {
        this.requestEventListeners.remove(listenerId);
    }

    public void registerRequestEventListener(RequestEventListener listener,
                                             String listenerId) {
        this.requestEventListeners.put(listenerId, listener);
    }

    public String registerAllocListener(final AllocListener owner, String listenerId, String ownerId) {
        LOGGER.info("Registering AllocOwner:" + listenerId + " ownerId:" + ownerId + " address:" + owner.toString());

        // someone may want to listen to everything
        String query = "owner equals " + ownerId;
        if (ownerId != null && ownerId.length() == 0) {
            query = "";
        }

        String leaseId = allocSpace.registerListener(Allocation.class, query, new Notifier<Allocation>() {
            public void notify(Type event, Allocation allocation) {
                if (event.equals(Type.WRITE) && allocation.type.equals(AllocType.ALLOCATED)) {
                    owner.add(allocation.requestId, com.liquidlabs.common.collection.Arrays.asList(allocation.resourceId), allocation.owner, allocation.priority);
                }
                if (event.equals(Type.WRITE) && allocation.type.equals(AllocType.PENDING)) {
                    owner.pending(allocation.requestId, com.liquidlabs.common.collection.Arrays.asList(allocation.resourceId), allocation.owner, allocation.priority);
                }
                if (event.equals(Type.TAKE) && allocation.type.equals(AllocType.ALLOCATED)) {
                    LOGGER.info("TAKE ALLOC:" + allocation.owner);
                    owner.take(allocation.requestId, allocation.owner, com.liquidlabs.common.collection.Arrays.asList(allocation.resourceId));
                }
                if (event.equals(Type.TAKE) && allocation.type.equals(AllocType.PENDING)) {
                    LOGGER.info("TAKE PEND:" + allocation.owner);
                    owner.satisfied(allocation.requestId, allocation.owner, com.liquidlabs.common.collection.Arrays.asList(allocation.resourceId));
                }
            }
        }, listenerId, VSOProperties.getResourceListenerRegInterval() + 5 * 60, new Event.Type[] { Type.WRITE, Type.TAKE });

        allocOwners.put(ownerId, owner);
        return leaseId;
    }

    public void unregisterAllocListener(String listenerId) {
        allocSpace.unregisterListener(listenerId);
    }

    public void cancelLease(String leaseKey) {
        resourceSpace.cancelLease(leaseKey);
    }

    public void renewLease(String leaseKey, int expires) {
        try {
            // LOGGER.info(">RenewLease:" + leaseKey);
            // if (LOGGER.isDebugEnabled()) LOGGER.debug("RenewLease:" +
            // leaseKey);
            resourceSpace.renewLease(leaseKey, expires);
        } catch (Exception e) {
            LOGGER.error(" RenewLease failed:" + leaseKey + " ALL_LEASES:"
                    + this.allocationLeases.keySet(), e);
        }
    }

    public void renewAllocLeasesForOwner(String owner, int timeSeconds) {
        int count = allocSpace.renewLeaseForOwner(owner, timeSeconds);
        LOGGER.info("RenewAllocLeasesFor:" + owner + " count:" + count);
    }

    public List<Allocation> getAllocsFor(String owner) {
        return allocSpace.findObjects(Allocation.class,
                "owner equals " + owner, false, -1);
    }

    public int removeResources(String requestId, String query) {
        List<Allocation> remove = allocSpace
                .remove(Allocation.class, query, -1);
        for (Allocation allocation : remove) {
            resourceSpace.remove(ResourceProfile.class, allocation.resourceId);
        }
        return remove.size();
    }

    public void registerResourceRegisterListener(final ResourceRegisterListener rrListener, final String listenerId, String resourceSelection, int timeout) throws Exception {
        LOGGER.info("Registering ResourceRegListener:" + rrListener);

        resourceSpace.registerListener(ResourceProfile.class, resourceSelection, new Notifier<ResourceProfile>() {
            public void notify(Type event, ResourceProfile resourceProfile) {
                if (event.equals(Type.WRITE) || event.equals(Type.UPDATE)) {
                    if (LOGGER.isDebugEnabled()) LOGGER.info("ResourceRegListener NOTIFY:" + resourceProfile.getResourceId() + " Listener:" + listenerId);
                    rrListener.register(resourceProfile.getResourceId(), resourceProfile);
                }
                if (event.equals(Type.TAKE)) {
                    rrListener.unregister(resourceProfile.getResourceId(), resourceProfile);
                }
            };
        }, listenerId, timeout, new Event.Type[] { Type.WRITE, Type.UPDATE, Type.TAKE });

        LOGGER.debug("Registered: ResourceRegEventListener, id:" + listenerId + " resourceSelection:" + resourceSelection);

    }

    public void resourceGroupListener(final ResourceGroupListener listener,	final String listenerId) {
        LOGGER.info("Registering ResourceGroupListener:" + listenerId);
        final ResourceRegisterListener rrListener = new ResourceRegisterListener(){

            public Map<String, String> resourceGroupEval = new ConcurrentHashMap<String, String>();

            public String getId() {
                return PIDGetter.getPID() + listenerId;
            }
            public void register(String resourceId, ResourceProfile resourceProfile) {

                // dont do this on the failover node.....it will create work each time a Resource is registered
                if (!VSOProperties.isManagerOnly()) return;

                // re-evaluate resource-groups - if they have changed then notify the listener
                String[] resourceGroup = allResourcesEver.findIds(ResourceGroup.class, "");
                for (String resourceG : resourceGroup) {
                    ResourceGroup group = findResourceGroup(resourceG);
                    if (group.hasChanged(ResourceSpaceImpl.this, resourceGroupEval)) {
                        LOGGER.info("NotifyResourceGroupChanged:" + group);
                        listener.resourceGroupUpdated(Type.WRITE, group);
                    }
                }
            }
            public void unregister(String resourceGroup, ResourceProfile resourceProfile) {
                resourceGroupEval.remove(resourceGroup);
            }
        };
        try {
            registerResourceRegisterListener(rrListener, rrListener.getId(), "", -1);
        } catch (Exception e) {
            LOGGER.info("Register Failed:", e);
        }

        allResourcesEver.registerListener(ResourceGroup.class, "",
                new Notifier<ResourceGroup>() {

                    public void notify(Type event, ResourceGroup result) {
                        try {
                            listener.resourceGroupUpdated(event, result);
                            if (event == Type.TAKE) rrListener.unregister(result.getName(),null);
                        } catch (Throwable t) {
                            LOGGER.warn("Notify Failed:" + listenerId, t);
                        }

                    }
                }, listenerId, -1, new Event.Type[] { Type.WRITE, Type.UPDATE,
                Type.TAKE });
    }

    public void unregisterResourceRegisterListener(String listenerId) {
        resourceSpace.unregisterListener(listenerId);
    }

    public List<ResourceProfile> findResourceProfilesBy(String query) {
        List<ResourceProfile> results = resourceSpace.findObjects(
                ResourceProfile.class, query, false, Integer.MAX_VALUE);
        Collections.sort(results, new Comparator<ResourceProfile>() {
            public int compare(ResourceProfile o1, ResourceProfile o2) {
                return o1.getResourceId().compareTo(o2.getResourceId());
            }
        });
        return results;
    }

    public ResourceProfile getResourceDetails(String key) {
        return resourceSpace.findById(ResourceProfile.class, key);
    }

    public String[] getAllocIdsForQuery(String query) {
        return allocSpace.findIds(Allocation.class, query);
    }

    public List<Allocation> getAllocsForQuery(String query) {
        return allocSpace.findObjects(Allocation.class, query, false,
                Integer.MAX_VALUE);
    }

    public URI getEndPoint() {
        return resourceSpace.getClientAddress();
    }

    public void registerResourceGroup(ResourceGroup resourceGroup) {
        allResourcesEver.store(resourceGroup, -1);
    }

    public void unRegisterResourceGroup(String name) {
        allResourcesEver.remove(ResourceGroup.class, name);
    }

    public List<ResourceGroup> getResourceGroups() {
        List<ResourceGroup> results = allResourcesEver.findObjects(
                ResourceGroup.class, null, true, Integer.MAX_VALUE);
        Collections.sort(results, new Comparator<ResourceGroup>() {
            public int compare(ResourceGroup o1, ResourceGroup o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return results;
    }

    public ResourceGroup findResourceGroup(String name) {
        return allResourcesEver.findById(ResourceGroup.class, name);
    }

    /**
     * Hosts Filter can be a partial name, i.e. contains, a wildcard or a
     * group:XXXX
     */
    public Set<String> expandIntoResourceIds(Set<String> hostsFilter) {
        Set<String> results = new HashSet<String>();
        List<String> allResourceIds = findResourceIdsBy("id equals 0");
        for (String hostFilterItem : hostsFilter) {
            if (hostFilterItem.startsWith("group:")) {
                hostFilterItem = hostFilterItem.replace("group:", "");
                ResourceGroup resourceGroup = findResourceGroup(hostFilterItem);
                if (resourceGroup != null) {
                    List<String> resourceIds = findResourceIdsBy(resourceGroup
                            .getResourceSelection()
                            + " AND id equals 0");
                    results.addAll(resourceIds);
                }
            } else {

                if (hostFilterItem.contains("*")) {
                    String hostFilterExpression = SimpleQueryConvertor
                            .convertSimpleToRegExp(hostFilterItem);
                    for (String resourceId : allResourceIds) {
                        if (resourceId.matches(hostFilterExpression))
                            results.add(resourceId);
                    }
                } else {
                    for (String resourceId : allResourceIds) {
                        if (resourceId.contains(hostFilterItem))
                            results.add(resourceId);
                    }
                }
            }
        }
        return results;
    }

    public Set<String> expandGroupIntoHostnames(String groupName) {
        HashSet<String> results = new HashSet<String>();
        ResourceGroup resourceGroup = findResourceGroup(groupName);
        if (resourceGroup == null)
            return results;
        List<ResourceProfile> profiles = findResourceProfilesBy(resourceGroup
                .getResourceSelection());
        for (ResourceProfile profile : profiles) {
            results.add(profile.getHostName());
        }
        return results;
    }
    @Override
    public BloomMatcher expandGroupIntoBloomFilter(String givenSelection, String... resourceGroups) {
        BloomFilter<CharSequence> stringBloomFilter = null;
        Set<String> hostParts = new HashSet<String>();

        ResourceGroup resourceGroup = null;

        Set<String> allGroups = getAllGroupsOrHosts(givenSelection, resourceGroups);
        for (String groupName : allGroups) {

            if (groupName.startsWith("group:")) {
                resourceGroup = findResourceGroup(groupName.substring("group:".length()));
            } else if (groupName.startsWith("hosts:")){
                String[] split = groupName.substring("hosts:".length()).split(",");
                for (String s : split) {
                    if (s.length() > 0) hostParts.add(s);
                }
            }  else {
                resourceGroup = findResourceGroup(groupName);
            }
            // didnt find anything where a group was set - dont let anything through
            if (resourceGroup == null)  continue;

            stringBloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), resourceSpace.size(), Double.valueOf(System.getProperty("res.grp.prob","0.001")));

            List<ResourceProfile> profiles = findResourceProfilesBy(resourceGroup.getResourceSelection());
            for (ResourceProfile profile : profiles) {
                String hostName = profile.getHostName();
                stringBloomFilter.put(hostName);
            }
        }
        if (stringBloomFilter == null) return null;

        return new BloomMatcher(stringBloomFilter,hostParts);
    }

    @Override
    public void clearLostAgents() {
        List<ResourceId> allResourcesEver = this.allResourcesEver.findObjects(ResourceId.class, "", false, -1);
        for (ResourceId resourceId : allResourcesEver) {
            auditLogger.emit("ClearLostAgent", resourceId.id);
            this.allResourcesEver.remove(ResourceId.class, resourceId.id);
        }
    }

    @Override
    public void setLLC(long llc) {

        this.llc = llc;
    }

    private Set<String> getAllGroupsOrHosts(String selection, String[]resourceGroups) {
        Set<String> results = new HashSet<String>();
        String[] hosts = selection.split(",hosts:");
        for (String host : hosts) {
            if (!host.startsWith("hosts:")) host = "hosts:" + host;
            String[] groupss = host.split("group:");
            for (String groups : groupss) {
                if (!groups.startsWith("group:") && !groups.startsWith("hosts:")) groups = "group:" + groups;
                results.add(groups.trim());
            }
        }
        for (String group : resourceGroups) {
                  results.add("group:" + group);
        }
        return results;

    }

    public Set<String> expandSubstringIntoHostnames(String substring) {
        HashSet<String> results = new HashSet<String>();
        List<ResourceProfile> profiles = findResourceProfilesBy("hostname contains " + substring);
        for (ResourceProfile profile : profiles) {
            results.add(profile.getHostName());
        }
        return results;
    }

    public String purge() {
        int purge = resourceSpace.purge(ResourceProfile.class, "");
        int purge2 = resourceSpace.purge(Allocation.class, "");
        return "Purged:" + purge + " allocs:" + purge2;
    }

    public String[] getResourceIdsForAssigned(String ownerId) {
        List<Allocation> assignments = allocSpace.findObjects(Allocation.class,
                "owner equals " + ownerId, false, Integer.MAX_VALUE);
        String[] results = new String[assignments.size()];
        int pos = 0;
        for (Allocation assignment : assignments) {
            results[pos++] = assignment.resourceId;
        }

        return results;
    }

    public static void main(String[] args) {
        try {
            ResourceSpaceImpl.boot(args[0]);
        } catch (Throwable t) {
            LOGGER.error(t.toString(), t);
            throw new RuntimeException("Failed to start ResourceSpace:"
                    + t.toString(), t);
        }
    }

    public static ResourceSpace boot(String lookupAddress) throws URISyntaxException, UnknownHostException {
        LOGGER.info("Starting");

        ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();

        LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(), "RSBootLU");
        LOGGER.info("Using LUSpace:" + lookupSpace);

        SpaceServiceImpl resourceSpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, NAME, mapperFactory.getProxyFactory().getScheduler(), true, false, false);
        SpaceServiceImpl allocSpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, NAME + "_ALLOC", mapperFactory.getScheduler(), true, false, false);
        SpaceServiceImpl allResourcesEver = new SpaceServiceImpl(lookupSpace, mapperFactory, NAME + "_EVER", mapperFactory.getScheduler(), true, true, true);

        ResourceSpace resourceSpace = new ResourceSpaceImpl(resourceSpaceService, allocSpaceService, allResourcesEver);

        mapperFactory.getProxyFactory().registerMethodReceiver(ResourceSpace.NAME, resourceSpace);

        // hook up the UI
        InvokableImpl invokable = new InvokableImpl(resourceSpaceService.getSpaceServiceAdmin());
        mapperFactory.getProxyFactory().registerMethodReceiver("INVOKABLE", invokable);

        resourceSpace.start();
        LOGGER.info("Started");
        return resourceSpace;
    }

    public static ResourceSpace getRemoteService(String whoAmI,
                                                 LookupSpace lookupSpace, ProxyFactory proxyFactory) {
        return SpaceServiceImpl.getRemoteService(whoAmI, ResourceSpace.class,
                lookupSpace, proxyFactory, ResourceSpace.NAME, true, false);
    }

    public Collection<String> getLostResources() {
        List<ResourceProfile> knownResources = resourceSpace.findObjects(ResourceProfile.class, "", false, -1);
        List<ResourceId> allResourcesEver = this.allResourcesEver.findObjects(ResourceId.class, "", false, -1);
        Set<String> missingIds = new HashSet<String>();

        for (ResourceId resourceProfile : allResourcesEver) {
            missingIds.add(resourceProfile.id);
        }
        for (ResourceProfile resourceProfile : knownResources) {
            missingIds.remove(resourceProfile.getResourceId());
        }
        List<String> results = new ArrayList<String>();
        for (String id : missingIds) {
            ResourceId resourceId = this.allResourcesEver.findById(ResourceId.class, id);
            results.add(resourceId.toString());
        }

        Collections.sort(results);
        return results;
    }

    synchronized public int getSystemResourceId() {
        SystemResourceCounter counter = resourceSpace.findById(SystemResourceCounter.class, SystemResourceCounter.class.getSimpleName());
        if (counter == null)
            counter = new SystemResourceCounter();
        counter.value += 10;
        resourceSpace.store(counter, -1);
        return counter.value;
    }

    public static class SystemResourceCounter {
        @Id
        String id = SystemResourceCounter.class.getSimpleName();
        public int value;
    }

}
