package com.liquidlabs.space.map;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.SLoggerConfig;
import com.liquidlabs.transport.PeerListener;
import com.liquidlabs.transport.PeerSender;
import com.liquidlabs.transport.serialization.Convertor;

public class ArrayStateSyncer {

	private static final String PING = "PING";

	private static final String PARTITION_EVENT = "PARTITION_EVENT";

	private static final Logger LOGGER = Logger.getLogger(ArrayStateSyncer.class);

	private Map map;
	int newStateUpdateCount;
	private String partition;
	
	private int receivedPartitionEventCount;
	private int sentPartitionEventCount;

	int errorSyncCount = 0;
	String srcUID;

	private PeerSender sender;

	private ExecutorService executor; 
	
	public ArrayStateSyncer() {
	}

	public ArrayStateSyncer(PeerSender sender, String srcUID, String partition, Map mapToSync, ExecutorService executor) {
		this.sender = sender;
		this.partition = partition;
		this.map = mapToSync;
		this.srcUID = srcUID;
		this.executor = executor;
	}

	String lastOtherSyncSrcID = "";
	long lastSyncTime = 0;
	int noNoiseSyncMsgSeconds = Integer.getInteger("space.no.sync.gap", 5);
	
	private Object syncBulkUpdate = new Object();
	public void syncBulkUpdate(String otherSrcID) throws InterruptedException {
		
		if (otherSrcID.equals(this.srcUID)) return;
		
		if (otherSrcID.equals(lastOtherSyncSrcID) && lastSyncTime > new DateTime().minusSeconds(noNoiseSyncMsgSeconds).getMillis()) {
			LOGGER.warn("Ignoring duplicate SyncMsg from:" + otherSrcID);
			return;
		}
		synchronized (syncBulkUpdate) {
		
			lastSyncTime = System.currentTimeMillis();
			lastOtherSyncSrcID = otherSrcID;
			
				/**
				 *  Check we have the PEER that sent the PARTITION msg - if we dont then wait for a bit...
				 *  this is crappy - but there is a race-state when the partitions join and discover each other and the
				 *  other peer needs to be added to the list - it its not there then this event is being processed
				 *   too soon - its a crappy timing bug - the only work around is to wait for 1 Second 
				 *   TEST which tests this: SpaceServicePeerFailBackTest
				 *   
				 *   SCENARIO: Start 2 instances in a cluster, kill-A, write to B, start-A and read the messages in newA exist
				 * 
				 */
				
			try {
				if (!sender.getPeerNames().contains(new URI(otherSrcID))) {
					LOGGER.warn(this.srcUID + " >>>>>>>>>>>>>> DIDnt GET:" + otherSrcID + " PEERS:" + sender.getPeerNames());
					Thread.sleep(1000);
				}
			} catch (URISyntaxException e) {
			}
			
			LOGGER.warn(String.format(">> %s %s Going to Sync data [%s], maxCount:%d", this.srcUID, SLoggerConfig.VS_MAP, otherSrcID, map.size()));
			LOGGER.warn("PEERS:" + sender.getPeerNames());
			
			int copyCount = 0;
			Collection<String> keys = map.keySet();
			for (String key : keys) {
				String value = map.get(key);
	
				if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s %s SEND: k:%s v:%s", srcUID, SLoggerConfig.VS_MAP, key, value));
	
				send(new NewState(srcUID, partition, newStateUpdateCount++, 0, null, null, key, value));
				copyCount++;
				Thread.sleep(20);
			}
			LOGGER.warn(String.format("<< %s %s Syncd(out) %d items mapCount:%d", this.srcUID, SLoggerConfig.VS_MAP, copyCount, map.size()));
		}
	}

	public void updateToLocal(int index, String existingKey, String existingValue, String newKey, String newValue) {
		// if there are no peers then just bailout
		if (existingValue != null && existingValue.equals(newValue))
			return;
		
		
//		if (outgoingStateList.remainingCapacity() == 0) {
//			LOGGER.warn(partition + " has Excessive OutgoingReplicationSize:" + outgoingStateList.size() + " DROPPING eKey:" + existingKey + " nKey:" + newKey);
//			return;
//		}
		send(new NewState(srcUID, partition, newStateUpdateCount++, index, existingKey, existingValue, newKey, newValue));
	}
	public void sendPartitionEvent() {
		LOGGER.warn(srcUID + " Sending PARTITION_EVENT Peers:" + sender.getPeerNames());
		sentPartitionEventCount++;
		send(new NewState(srcUID, partition, newStateUpdateCount++, -1, "0", "0", PARTITION_EVENT, "0"));		
	}
	public void sendPing() {
		send(new NewState(srcUID, partition, newStateUpdateCount++, -1, "0", "0", PING, "0"));		
	}
	
	
	private void send(final NewState msg) {
		if (executor.isShutdown()) return;
		Runnable task = new Runnable() {
			@Override
			public void run() {
				
				// we have no peers to send to
				if (sender.getPeerNames().size() == 0) {
					return;
				}
				
				/** Peers are discovered **/
				boolean verbose = false;// partition.contains("LookupSpace");
				if (LOGGER.isDebugEnabled() || verbose) LOGGER.info("SENDING:" + msg.newKey + " peers:" + sender.getPeerNames()); 
				
				try {
					sender.sendToPeers(Convertor.serializeAndCompress(msg), verbose);
				} catch (IOException e) {
					e.printStackTrace();
				}				
			}
		};
		executor.execute(task);
	}

	
	private Object updateFromRemote = new Object();
	public boolean updateFromRemote(final NewState newState) {
		
		synchronized (updateFromRemote) {
			try {
				if (newState.srcUID.equals(this.srcUID)) return false;
	
				// PUT
				if (newState.existingKey == null) {
					if (LOGGER.isDebugEnabled()) LOGGER.debug(this.srcUID + "(0) PUT:" + newState);
					map.put(newState.newKey, newState.newValue, false);
				// TAKE
				} else if (newState.existingValue != null && newState.newValue == null) {
					if (LOGGER.isDebugEnabled()) 
						LOGGER.debug(this.srcUID + " (1) REMOVE/TAKE:" + newState.newKey);
					map.remove(newState.newKey, false);
				// TAKE 2
				} else if (newState.isRemove()) {
					if (LOGGER.isDebugEnabled()) LOGGER.debug(this.srcUID + " (2) REMOVE/TAKE:" + newState);
					map.remove(newState.newKey, false);
				// PARTITION EVENT - Sync request
				} else if (newState.newKey.startsWith(PARTITION_EVENT)){
					
					LOGGER.warn(String.format(this.srcUID + " %s Received PARTITION_EVENT from[%s]", srcUID, newState.srcUID));
						Runnable task = new Runnable() {
							public void run() {
								try {
									receivedPartitionEventCount++;
									syncBulkUpdate(newState.srcUID);
								} catch (InterruptedException e) {
								} catch (Exception e) {
									LOGGER.warn("PE ex", e);
								}
							};
						};
						executor.submit(task);
					
						// Keep Alive the peer
				} else if (newState.newKey.startsWith(PING)){
					
				// WRITE
				} else {
					if (newState.newKey == null) {
						LOGGER.error(srcUID + " NEWKEy is NULL:" + newState);
						return false;
					}
					if (newState.newValue == null) {
						LOGGER.error(srcUID + " NEWValue is NULL:" + newState);
						return false;
					}
					if (LOGGER.isDebugEnabled()) LOGGER.debug("(3) PUT:" + newState);
					map.put(newState.newKey, newState.newValue, false);
				}
	
				return false;
			} catch (MapIsFullException e) {
				LOGGER.warn(e.toString(), e);
				return false;
			}
		}
	}

	public String toString() {
		return  this.getClass().getSimpleName() + "@" + this.hashCode() +  "/" + this.srcUID;
	}

	public void setMap(Map map) {
		this.map = map;
	}

	public void setPeerCount(int size, URI peer) {
		LOGGER.info(this.srcUID + " ****** Added Peer, total:" + size);
		try {
			this.syncBulkUpdate(peer.toString());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void addPeerListener(PeerListener peerListener) {
	}
}
