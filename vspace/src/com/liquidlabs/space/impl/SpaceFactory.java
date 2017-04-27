package com.liquidlabs.space.impl;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.LifeCycleManager;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.space.impl.prevalent.PrevaylerManager;
import com.liquidlabs.space.lease.Lease;
import com.liquidlabs.space.lease.LeaseManagerImpl;
import com.liquidlabs.space.lease.SpaceReaper;
import com.liquidlabs.space.map.*;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.notify.NotifyEventHandler;
import com.liquidlabs.space.raw.SpaceImpl;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.PeerListener;
import com.liquidlabs.transport.PeerSender;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import org.apache.log4j.Logger;
import org.prevayler.Prevayler;

import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SpaceFactory implements LifeCycle {

	private static final Logger LOGGER = Logger.getLogger(SpaceFactory.class);
	
	int leaseInterval = Integer.getInteger(Lease.PROPERTY, Lease.DEFAULT_INTERVAL);
	long startTime = System.currentTimeMillis();
	LifeCycleManager lifeCycleManager = new LifeCycleManager();
	private PrevaylerManager prevaylerManager;
	
	private ScheduledExecutorService scheduler;

	private TransportFactory transportFactory;

	public SpaceFactory(ScheduledExecutorService scheduler, TransportFactory transportFactory) {
		this.scheduler = scheduler;
		this.transportFactory = transportFactory;
		prevaylerManager = new PrevaylerManager(VSpaceProperties.getSnapshotIntervalSecs(), scheduler);
	}

	public SpaceFactory() {
	}

	public Space createSpace(final String partitionName, ScheduledExecutorService scheduler, int spaceSize, ProxyFactory proxyFactory, boolean isClustered, boolean persistentMap, PeerSender peerSender, URI replicationUri, ExecutorService sendMsgExecutor) throws URISyntaxException, PersistentSpaceException, InterruptedException {
		
		EndPoint ep = proxyFactory.getEndPointServer();
		URI addr = proxyFactory.getAddress();
		String sourceId = getSourceUID(addr, partitionName);
		NWArrayStateReceiver syncReceiver = null;
		
		if (isClustered) {
			syncReceiver = new NWArrayStateReceiver();
			transportFactory.getEndPoint(replicationUri, syncReceiver, true);
		}

		NotifyEventHandler dataSpaceNotifyEventHandler = new NotifyEventHandler(sourceId, partitionName, spaceSize, scheduler);
		final Space dataSpace = createSpaceInstanceNEW(partitionName, spaceSize, sourceId,
															dataSpaceNotifyEventHandler, isClustered, persistentMap, ep, peerSender, syncReceiver, sendMsgExecutor);
		
		String sourceId2 = Lease.KEY +  getSourceUID(addr, partitionName);
		NotifyEventHandler leasedSpaceEventHandler = new NotifyEventHandler(sourceId2, partitionName+"_LEASE", spaceSize, scheduler);
		final Space leasedSpace = createSpaceInstanceNEW(Lease.KEY + partitionName, spaceSize,  
															sourceId2, 
															leasedSpaceEventHandler, isClustered, false, ep, peerSender, syncReceiver, sendMsgExecutor);
		
		SpaceReaper spaceReaper = new SpaceReaper(partitionName, dataSpace, leasedSpace);
		LeaseManagerImpl leaseManager = new LeaseManagerImpl(partitionName, leasedSpace, spaceReaper);
		scheduler.scheduleWithFixedDelay(spaceReaper, leaseInterval, leaseInterval, TimeUnit.SECONDS);
		
		if (isClustered) {
			proxyFactory.registerMethodReceiver(partitionName+NotificationClusterManager.ID, dataSpaceNotifyEventHandler);
		}

		LeasedClusteredSpace leasedSpaceNode = new LeasedClusteredSpace(partitionName, dataSpace, leasedSpace, leaseManager, proxyFactory, isClustered, sourceId);
		
		leasedSpaceNode.addLifeCycleListener(spaceReaper);
		leasedSpaceNode.addLifeCycleListener(dataSpaceNotifyEventHandler);
		leasedSpaceNode.addLifeCycleListener(leasedSpaceEventHandler);
		if (peerSender != null) {
			leasedSpaceNode.addPeerListener(peerSender);
			leasedSpaceNode.setReplicationURI(replicationUri);
		}
		this.lifeCycleManager.addLifeCycleListener(leasedSpaceNode);
		
		leasedSpaceNode.start();
		return leasedSpaceNode;
	}
	
	private Space createSpaceInstanceNEW(final String partitionName, int spaceSize, final String srcUID, EventHandler notifyEventHandler, boolean isClustered, boolean persistentMap, final EndPoint ep, PeerSender peerSender, NWArrayStateReceiver syncReceiver, ExecutorService sendMsgExecutor) throws URISyntaxException, PersistentSpaceException, InterruptedException{
		MapImpl mapImpl = new MapImpl(srcUID, partitionName, spaceSize, isClustered, null);
		SpaceImpl space = null;
		final ArrayStateSyncer stateSyncer = isClustered ? new ArrayStateSyncer(peerSender, srcUID, partitionName, mapImpl, sendMsgExecutor) : null; 
		if (isClustered) {
			mapImpl.setStateSyncer(stateSyncer);
//			syncSender.addSyncer(partitionName, stateSyncer);
// this stuffs up the prevayler system - WTF?			
//			stateSyncer.addPeerListener(new PeerListener() {
//				public void peerAdded(URI peer, Set<URI> peers) {
//					LOGGER.info("SYNC Added Peer:" + peer);
//				}
//				public void peerRemoved(URI peer, Set<URI> keySet) {
//					LOGGER.info("SYNC Removed Peer:" + peer);
////					ep.removePeer(peer);
//				}
//			});
			
			peerSender.addPeerListener(new PeerListener(){
				private URI peer;
				public void peerAdded(URI peer, Set<URI> peers) {
					this.peer = peer;
					// this listener is shared wth the set of others sharing the same endpoint/
					// therefore is needs to determine traffic just for it.
					// remove Peers from the ORM will have a ?svc param and -SPACE on he end of the SpaceService names
					// anything else will have 'null'
					if (isPeerURLMatch(partitionName,  peer)) {
						LOGGER.info(srcUID + " ?svc=" + partitionName + " API Added Peer:" + peer);
						stateSyncer.setPeerCount(peers.size(), peer);
					} else if (partitionName == null) {
						LOGGER.info(srcUID + " null - API Added Peer:" + peers);
						stateSyncer.setPeerCount(peers.size(), peer);						
					} else {
						if (LOGGER.isDebugEnabled()) LOGGER.debug("DROP: "+ srcUID + " thisPart:" + partitionName + " gPart:" + peer + " src:" + srcUID);
					}
				}
				public String toString() {
					return "SpaceFactory.PeerListener:" + partitionName + " " + peer;
				}
				public void peerRemoved(URI peer, Set<URI> keySet) {
					LOGGER.warn(srcUID + " API Removed Peer:" + peer);
					ep.removePeer(peer);
				}
			});
		}
		if (persistentMap) {
            try {
                Prevayler prevayler = createPrevayler(partitionName, mapImpl);
                PersistentMap persistentMap2 = new PersistentMap(prevayler);
                if (stateSyncer != null) {
                    persistentMap2.setStateSyncer(stateSyncer);
                    // which one? first one seems more correct
                    stateSyncer.setMap(persistentMap2);
                    //			stateSyncer.setMap(persistentMap2.getMap());
                }
                space = new SpaceImpl(partitionName, persistentMap2, notifyEventHandler);
            } catch (Throwable t) {
                t.printStackTrace();
                System.err.println("FATAL ERROR - Cannot load XML config from SPACE dir");
                LOGGER.fatal("FATAL ERROR - Cannot load XML config from SPACE dir",t);
                LOGGER.fatal("FATAL ERROR - EXITING - Check the XML for <null><null> pairs",t);
                System.exit(0);
            }
		} else {
			space = new SpaceImpl(partitionName, mapImpl, notifyEventHandler);
		}
		
		if (isClustered) {
			syncReceiver.addUpdater(srcUID, partitionName, stateSyncer, space);
		}
		
		return space;
	}
	
	public boolean isPeerURLMatch(String partitionName, URI peer) {
		String src = com.liquidlabs.common.net.URIUtil.getParam("svc", peer);
		String srcSpace = src + "-SPACE";
		String ppName = partitionName + "-SPACE";
		return (partitionName == null || src == null || src.equals(partitionName) || src.equals(ppName) ||
				srcSpace.equals(partitionName) || srcSpace.equals(ppName));
	}

	private String getSourceUID(URI address, String partition) {
		return "stcp://" + address.getHost()  + ":" + address.getPort() + "/" + partition;
	}

	private Prevayler createPrevayler(String partitionName, Map map) throws PersistentSpaceException {
		try {
			return prevaylerManager.startPrevayler(partitionName, map);
		} catch (Throwable t) {
			throw new PersistentSpaceException(t);
		}
	}
	public Space createNonClusteredLeaseSpace(int spaceSize, String partition) {
		try {
			return createSpace(partition, scheduler, spaceSize, null, false, false, null, null, scheduler);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	public Space createSimpleSpace(String partition){
		String src = "src";
		EventHandler eventHandler = new NotifyEventHandler(src, partition, 1000, scheduler);
		return new SpaceImpl(partition, new MapImpl(src, partition,1000 * 10, false, null), eventHandler);
	}
	
	public void start() {
		lifeCycleManager.start();
	}
	public void stop() {
		lifeCycleManager.stop();
	}

	private boolean isPeerURLMatch(final String partitionName,
			String pPartitionName, String pPartitionNameSPC) {
		return pPartitionName == null || partitionName.equals(pPartitionName) || partitionName.equals(pPartitionNameSPC);
	}


}

