package com.liquidlabs.vso.work;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.liquidlabs.common.Logging;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.Time;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.liquidlabs.vso.Notifier;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;

public class WorkAllocatorImpl implements WorkAllocator {
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "WorkAlloc");
	private final static Logger LOGGER = Logger.getLogger(WorkAllocator.class);
	public static String TAG = "WORK_ALLOC";

	LifeCycle.State state = LifeCycle.State.STOPPED;

	ObjectTranslator query = new ObjectTranslator();

	private final SpaceService spaceService;

	public WorkAllocatorImpl(SpaceService spaceService) {
		this.spaceService = spaceService;
	}

	public void start() {
		if (this.state == LifeCycle.State.STARTED)
			return;
		this.state = LifeCycle.State.STARTED;

		spaceService.start(this, "boot-1.0");

	}

	public void stop() {
		try {
			if (this.state == LifeCycle.State.STOPPED)
				return;
			this.state = LifeCycle.State.STOPPED;
			spaceService.stop();
		} catch (Throwable t) {
		}
		;
	}

	public URI getEndPoint() {
		return spaceService.getClientAddress();
	}

	public void assignWork(String requestId, String resourceId, WorkAssignment workInfo) {
		assignWork(requestId, resourceId, workInfo, State.ASSIGNED);
	}

	public void assignWork(String requestId, String workingDirectory, List<String> resourceIds, int priority, WorkAssignment workAssignment, State state) {
		for (String resourceId : resourceIds) {
			workAssignment.setPriority(priority);
			workAssignment.setWorkingDirectory(workingDirectory);
			assignWork(requestId, resourceId, workAssignment, state);
		}
	}

	boolean isIgnoringDuplicateWork = Boolean.getBoolean("ignore.duplicate.work");
	long pauseDuplicate = Long.getLong("pause.realloc.ms",1);
	private void assignWork(String requestId, String resourceId, WorkAssignment workAssignment, State state) {

		try {
			if (workAssignment.getOverridesService().length() > 0) {
				handleServiceOverride(requestId, resourceId, workAssignment);
			}
			workAssignment.setResourceId(resourceId);

			String[] split = resourceId.split("-");
			workAssignment.setProfileId(Integer.valueOf(split[split.length - 1]));
			String agentId = resourceId.substring(0, resourceId.lastIndexOf('-'));
			workAssignment.setStatus(state);
			workAssignment.setAgentId(agentId);
			if (state.equals(LifeCycle.State.RUNNING)) {
				workAssignment.setStartTime(DateTimeUtils.currentTimeMillis());
			}
			long timestamp = DateTimeUtils.currentTimeMillis();
			workAssignment.timestamp = Time.nowAsString();
			workAssignment.timestampMs = timestamp;

			WorkAssignment existingId = spaceService.findById(WorkAssignment.class, workAssignment.getId());
			if (existingId != null) {
				// now check the workId status - if it is running then we will
				// ignore theassignment request OR do it again.
				if (existingId.getStatus().equals(LifeCycle.State.RUNNING)) {
					Thread.sleep(pauseDuplicate);
					if (isIgnoringDuplicateWork) {
						LOGGER.warn(("Going to ignore:" + workAssignment.getId() + " Existing Work Found and it is already RUNNING"));
						return;
					} else {
						LOGGER.warn(("Existing Work Found and it is already RUNNING = going to run anyway"));
					}
				}
			}

            String msg = String.format("%s AssignWork reqId:%s resource:%s => %s %s", TAG, requestId, resourceId, workAssignment.getId(), state.name());
            LOGGER.info(msg);
            audit("Assign", workAssignment.getId(), "");

			int leasePeriod = VSOProperties.getLUSpaceServiceLeaseInterval();// state
			// ==
			// State.ASSIGNED
			// ?
			// -1
			// :
			// VSOProperties.getLUSpaceServiceLeaseInterval();
			String storeLease = spaceService.store(workAssignment, leasePeriod);
			spaceService.assignLeaseOwner(storeLease, resourceId);

		} catch (Throwable t) {
            String msg = String.format("%s Failed to assignWork[%s] to resource[%s]", requestId, workAssignment.getId(), resourceId);
            LOGGER.error(msg, t);
            audit("AssignERROR",workAssignment.getId(), "msg:" +msg);
		}
	}
    private void audit(String key, String id, String msg) {
        String value = " workId:" + id.replace(":","_");
        if (msg.length() > 0) value += " " + msg;
        auditLogger.emit(key, "Work" +  value);
    }

	private void handleServiceOverride(String requestId, String resourceId, WorkAssignment workAssignment) {
		String[] workIdsToKill = getWorkIdsForQuery(String.format("resourceId equals %s AND bundleId equals %s and serviceName equals %s", resourceId, workAssignment.getBundleId(), workAssignment
				.getOverridesService()));
		for (String workId : workIdsToKill) {
			unassignWork(requestId, workId);
		}
	}

	public void saveWorkAssignment(WorkAssignment workAssignment, String resourceId) {
		long timestamp = DateTimeUtils.currentTimeMillis();
		workAssignment.timestamp = Time.nowAsString();
		workAssignment.timestampMs = timestamp;
		String storeLease = spaceService.store(workAssignment, VSOProperties.getLUSpaceServiceLeaseInterval());
		spaceService.assignLeaseOwner(storeLease, resourceId);
	}

	/**
	 * Should only be called from the Owner of the workAssignment
	 */
	public void update(String id, String updateStatement) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("%s UpdateWorkAssignment id:%s stat:%s", TAG, id, updateStatement));
		}
		int retryCount = 0;
		while (!spaceService.containsKey(WorkAssignment.class, id) && retryCount++ < 2) {
			pause();
		}


		// bit of a hack for event race conditions...
		if (updateStatement.contains("RUNNING"))
			Thread.yield();

		long timestampMs = DateTimeUtils.currentTimeMillis();
		String timestamp = Time.nowAsString();
        WorkAssignment existing = spaceService.findById(WorkAssignment.class, id);
        if (existing == null) {
            audit("UpdateFailed", id, "Status:Missing");
            LOGGER.error("Update Failed to find WorkId:" + id);
            return;
        }
        State beforeStatus = existing.getStatus();
		spaceService.update(WorkAssignment.class, id, updateStatement + " AND timestamp replaceWith '" + timestamp + "' AND timestampMs replaceWith " + timestampMs, -1);

		WorkAssignment wid = spaceService.findById(WorkAssignment.class, id);
		State status = wid.getStatus();
		if (status.equals(LifeCycle.State.ERROR) || status.equals(LifeCycle.State.STOPPED)) {
			// tell the ResourceAgent the Alloc is finished
		}
        if (beforeStatus != status) {

            audit("StatusChanged", id, " BeforeStatus:" + beforeStatus.name() + " AfterStatus:" + status.name() + " Host:" + wid.agentId + " Bundle:" + wid.getBundleId() +  " HostBundle:" + wid.agentId + "_" + wid.getBundleId() + " errMsg:" + wid.getErrorMsg());
        }
	}

	private void pause() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
	}

	synchronized public String bounce(String serviceId) {
		LOGGER.info(String.format("%s Bouncing[%s]", TAG, serviceId));
		List<WorkAssignment> workAssignments = getWorkAssignmentsForQuery("Id equals " + serviceId);
		for (WorkAssignment workAssignment : workAssignments) {
			try {
				unassignWork("bounce", workAssignment.getId());
				pause();
				pause();
				pause();
				pause();
				assignWork("bounce", workAssignment.getResourceId(), workAssignment);
			} catch (Throwable t) {
				LOGGER.warn(t.getMessage(), t);
			}
		}
		return "bounce complete";
	}

	/**
	 * @param serviceName
	 *            is used when the existing allocation is known and expected to
	 *            be removed. If there is a timing issue (race condition between
	 *            ResourceSpace.reclaim - i.e. during deployment) and a
	 *            SLAContainer.release its possible the work will be unassigned
	 *            twice - the check before removal will verify the correct
	 *            serviceName is unassigned.
	 */
	synchronized public void unassignWorkFromResource(String requestId, String resourceId, String bundleId, String serviceName) {
		List<WorkAssignment> workAssignments = spaceService.findObjects(WorkAssignment.class, "resourceId equals " + resourceId, false, Integer.MAX_VALUE);
		LOGGER.debug(String.format("%s %s UnassignWork From Resource[%s] ServiceName[%s]", TAG, requestId, resourceId, serviceName));
		if (workAssignments.size() == 0) {
			LOGGER.warn(requestId + " Failed to UnAssignWork:" + resourceId + " WorkAssignment NOT FOUND");
			return;
		}
		for (WorkAssignment workAssignment : workAssignments) {

			if (serviceName == null || workAssignment.getServiceName().equals(serviceName)) {
				LOGGER.info(String.format("%s UnAssignWork reqId:%s resource:%s =>X %s Svc[%s]", TAG, requestId, resourceId, workAssignment.getId(), workAssignment.getServiceName()));
				spaceService.remove(WorkAssignment.class, workAssignment.id);
			} else {
				LOGGER.warn(String.format("%s ServiceName[%s] bundleId[%s]", TAG, serviceName, bundleId));
				LOGGER.warn(String.format("%s UnAssignWork Service[%s:%s] FAILED, Resource already taken %s =>X %s", TAG, serviceName, bundleId, resourceId, workAssignment.getId()));
			}
		}
	}

	synchronized public void unassignWork(String requestId, String workAssignmentId) {
		List<WorkAssignment> workAssignments = spaceService.findObjects(WorkAssignment.class, "id equals " + workAssignmentId, false, Integer.MAX_VALUE);
		LOGGER.info(String.format("%s UnAssignWorkId reqId:%s count:%s", TAG, requestId, workAssignments.size()));
		for (WorkAssignment workAssignment : workAssignments) {
			LOGGER.info(String.format("%s UnAssignWorkId reqId:%s resource:%s =>X. %s", TAG, requestId, workAssignment.getResourceId(), workAssignment.getId()));
			spaceService.remove(WorkAssignment.class, workAssignment.id);
		}
	}

	synchronized public void suspendWork(String requestId, String workAssignmentId) {
		List<WorkAssignment> workAssignments = spaceService.findObjects(WorkAssignment.class, "id equals " + workAssignmentId, false, Integer.MAX_VALUE);
		LOGGER.debug(String.format("%s Suspend ResourceCount: %s", TAG, workAssignments));
		for (WorkAssignment workAssignment : workAssignments) {
			workAssignment.setStatus(State.SUSPENDED);
			LOGGER.info(String.format("%s SuspendWork reqId:%s resource:%s =>X. %s", TAG, requestId, workAssignment.getResourceId(), workAssignment.getId()));
			spaceService.store(workAssignment, -1);
		}
	}

	public int unassignWorkForQuery(String requestId, String query) {
		return spaceService.purge(WorkAssignment.class, query);
	}

	public String[] getWorkIdsAssignedToResource(String resourceId) {
		return spaceService.findIds(WorkAssignment.class, "resourceId equals " + resourceId);
	}

	public List<WorkAssignment> getWorkAssignmentsForQuery(String query) {
		return spaceService.findObjects(WorkAssignment.class, query, false, Integer.MAX_VALUE);
	}

	public String[] getWorkIdsForQuery(String query) {
		return spaceService.findIds(WorkAssignment.class, query);
	}

	public int getWorkIdCountForQuery(String query) {
		return spaceService.findIds(WorkAssignment.class, query).length;
	}

	public List<Integer> getWorkIdCountForQuery(String... workIdQueries) {
		List<Integer> results = new ArrayList<Integer>();
		for (String query : workIdQueries) {
			results.add(getWorkIdCountForQuery(query));
		}
		return results;
	}

	public List<WorkAssignment> getWorkAssignmentsForBundle(String bundleId) {
		return spaceService.findObjects(WorkAssignment.class, "bundleId equals " + bundleId, false, Integer.MAX_VALUE);
	}

	public int unassignWorkFromBundle(String bundleId) {
		List<WorkAssignment> removed = spaceService.remove(WorkAssignment.class, "bundleId equals " + bundleId, -1);
		LOGGER.info(String.format("%s Removed %d WorkAssignments for bundle:%s", TAG, removed.size(), bundleId));
		return removed.size();
	}

	/**
	 * Register a worListener - it must be reregistered every X seconds or it
	 * will fail to fire
	 */
	public void registerWorkListener(final WorkListener workListener, final String agentId, final String listenerId, boolean replayMissedEvents) throws Exception {
		// if (LOGGER.isDebugEnabled())
		// LOGGER.debug(String.format("Registering WorkListener[%s] listenerId[%s]",
		// agentId, listenerId));

		// first registration will generally want this to happen so we log it
		if (replayMissedEvents) {
			LOGGER.info(String.format("%s Registering WorkListener[%s] listenerId[%s]", TAG, agentId, listenerId));
		}

		try {

			String query = "agentId equals " + agentId;
			if (agentId.length() == 0)
				query = "";

			spaceService.registerListener(WorkAssignment.class, query, new Notifier<WorkAssignment>() {
				public void notify(Type event, WorkAssignment work) {
					try {
						if (event.equals(Type.WRITE)) {
							if (work.getStatus() == State.ASSIGNED) {
								LOGGER.info(String.format("%s W Notifying WorkListener:%s work:%s %s", TAG, listenerId, work.getId(), work.getStatus().name()));
								workListener.start(work);
							}
						}
						if (event.equals(Type.UPDATE)) {
                            if (work.getStatus() == State.STOPPED) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("Ignoring Stopped msg:" + work.getId());
                                }
                                return;
                            }
							if (LOGGER.isDebugEnabled())
								LOGGER.debug(String.format("%s U Notifying WorkListener:%s work:%s %s", TAG, listenerId, work.getId(), work.getStatus().name()));
							workListener.update(work);
						}
						if (event.equals(Type.TAKE)) {
                            audit("Stopping", work.getId()," Host:" + work.getAgentId());
							LOGGER.info(String.format("%s S Stopping WorkListener:%s work%s", TAG, listenerId, work.getId()));
							workListener.stop(work);
						}
					} catch (Throwable e) {
						LOGGER.warn("***** Failed to notify:" + e);
                        audit("NotifyFailed", work.getId(), "Host:" + work.getAgentId() +  " msg:" +e.toString());
						throw new RuntimeException(e);
					}
				}
			}, listenerId, VSOProperties.getLUSpaceServiceLeaseInterval(), new Event.Type[] { Type.WRITE, Type.TAKE, Type.UPDATE });

		} catch (Throwable t) {
			LOGGER.error(String.format("Failed to register listener agent:%s listenerId:%s", agentId, listenerId), t);
			throw new RuntimeException(t);
		}
	}

	public void unregisterWorkListener(String agentId) {
		LOGGER.info(String.format("%s Unregistering WorkListener:", TAG, agentId));
		if (agentId == null || agentId.length() == 0) {
			LOGGER.warn("Unexpected called to unregisterWorkListener, not agentId specified:" + agentId);
			return;
		}
		boolean unregisterListener = spaceService.unregisterListener(agentId);

		if (!unregisterListener)
			LOGGER.warn(String.format("%s ListenerId[%s] not found", TAG, agentId));
		removeWorkAssignmentsForResourceId(agentId, System.currentTimeMillis());
		LOGGER.info(String.format("%s Unregistering WorkListener:%s removed", TAG, agentId));
	}

	public void removeWorkAssignmentsForResourceId(String agentId, long removeBefore) {
		List<WorkAssignment> workAssignments = getWorkAssignmentsForQuery("resourceId contains " + agentId + " AND timeStampMs < " + removeBefore);
		spaceService.purge(workAssignments);
		LOGGER.info(String.format("%s LS_EVENT:Purged %d workAssignments agentId %s", TAG, workAssignments.size(), agentId));
	}

	public boolean renewLease(String workListenerLease, int expires) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Renew LEASE:" + workListenerLease);
		spaceService.renewLease(workListenerLease, expires);
		return true;
	}

	public boolean renewWorkLeases(String ownerId, int timeoutSeconds) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Renew LEASES:" + ownerId);
		this.spaceService.renewLeaseForOwner(ownerId, timeoutSeconds);
		return true;
	}

	public void renewWorkLease(String leaseId, int timeSeconds) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Renew WorkLEASE:" + leaseId);
		spaceService.renewLease(leaseId, timeSeconds);
	}

	public static void main(String[] args) {
		try {
			WorkAllocatorImpl.boot(args[0]);
		} catch (Exception e) {
			LOGGER.info(TAG + " Failed to start:" + e.getMessage(), e);
		}
	}

	public static WorkAllocatorImpl boot(String lookupAddress) throws URISyntaxException, UnknownHostException {
		LOGGER.info(TAG + " Starting");

		ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();
		LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(), "WorkAllocBootLU");

		final ResourceSpace resourceSpace = ResourceSpaceImpl.getRemoteService("WORK_ALLO", lookupSpace, mapperFactory.getProxyFactory());

		SpaceServiceImpl spaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, WorkAllocator.NAME, mapperFactory.getScheduler(), true, false, true);
		WorkAllocatorImpl workAllocator = new WorkAllocatorImpl(spaceService);

		mapperFactory.getProxyFactory().registerMethodReceiver(WorkAllocator.NAME, workAllocator);
		workAllocator.start();

		try {
			workAllocator.registerWorkListener(new WorkListener() {

				public String getId() {
					return "boot-RA";
				}

				public void start(WorkAssignment workAssignment) {
				}

				public void stop(WorkAssignment workAssignment) throws RuntimeException {
					try {
						LOGGER.info(WorkAllocator.NAME + " ReleaseALLOC for:" + workAssignment.getResourceId() + " Work:" + workAssignment);
						String resourceId = workAssignment.getResourceId();
						resourceSpace.releaseAllocsForOwner(resourceId);
					} catch (Throwable t) {
						LOGGER.warn("Failed to update ResourceSpace:" + workAssignment);
					}
				}

				public void update(WorkAssignment workAssignment) {
				}

			}, "", "boot-RA", true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return workAllocator;
	}

	public static WorkAllocator getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
		return SpaceServiceImpl.getRemoteService(whoAmI, WorkAllocator.class, lookupSpace, proxyFactory, WorkAllocator.NAME, true, false);
	}

	public SpaceService getWorkAllocatorSpace() {
		return spaceService;
	}
}
