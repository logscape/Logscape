package com.liquidlabs.space.map;

import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

import com.liquidlabs.space.SLoggerConfig;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.serialization.Convertor;

public class NWArrayStateReceiver implements Receiver {
	private static final Logger LOGGER = Logger.getLogger(NWArrayStateReceiver.class);

	private final java.util.Map<String, Updater> updaters = new HashMap<String, Updater>();
	Convertor convertor = new Convertor();

	private String srcUID;

	public NWArrayStateReceiver() {
	}
	public void addUpdater(String srcUID, String partition, ArrayStateSyncer syncer, UpdateListener updateListener){
		this.srcUID = srcUID;
		LOGGER.info(this.srcUID + " Adding Updater:" + partition);
		updaters.put(partition, new Updater(partition, syncer, updateListener));
	}
	public boolean isForMe(Object payload) {
		if (payload instanceof NewState) {
			NewState ns = (NewState) payload;
			if (ns.srcUID.equals(srcUID)) return true;
			Set<String> keySet = this.updaters.keySet();
			for (String partition : keySet) {
				if (ns.partition.equals(partition)) return true;				
			}
		}
		return false;
	}
	public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
		Object o;
		try {
			o = Convertor.deserializeAndDecompress(payload);
			return receive(o, remoteAddress, remoteHostname);
		} catch (Exception e) {
			LOGGER.error("Failed to handle:",e);
		}
		return new byte[0];
	}
		
	public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {

		String key = "";
		try {
			if (payload instanceof NewState) {
				NewState newState = (NewState) payload;
				key = newState.newKey;
				
				// TODO: We see a lot of msgs back to ourselves
				if (newState.srcUID.equals(srcUID)) {
	//				LOGGER.warn("SRC UIDs matched - rejecting incoming msg:" + newState.srcUID);
					return null;
				}
				Updater updater = this.updaters.get(newState.partition);
				if (updater == null) {
					LOGGER.error(SLoggerConfig.VS_MAP + this.srcUID + " Failed to locate Syncer for partition:" + newState.partition + " partitions:" + this.updaters.keySet());
		//			throw new RuntimeException("Failed to locate Syncer for partition:" + newState.partition + " partitions:" + this.updaters.keySet());
					return null; 
				}
				if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("%s ReceivedIncoming:%s", srcUID, newState.toString()));
				updater.update(newState);
				return null;
			} else {
				LOGGER.warn("Unknown ObjectType:" + payload);
			}
		} catch (Exception e) {
			LOGGER.warn(this.toString() + " Failed to process:" + key + " from:" + remoteHostname, e);
		}
		return null;
	}
	public String toString() {
		return String.format("%s %s", this.getClass().getName(), srcUID);
	}
	public static class Updater {
		public Updater(String partition, ArrayStateSyncer syncer, UpdateListener updateListener) {
			this.partition = partition;
			this.syncer = syncer;
			this.updateListener = updateListener;
		}
		public void update(NewState newState){
			
			if (LOGGER.isDebugEnabled()) LOGGER.debug("SYNC Got newState:" + newState.newKey + " v:" + newState.newValue);
			if (!Event.isA(newState.newValue)) {
				syncer.updateFromRemote(newState);
			} else if (newState.newValue != null && Event.isA(newState.newValue)){
				updateListener.updatedValue(newState.newKey, newState.newValue, false);
			}
		}
		String partition;
		ArrayStateSyncer syncer;
		UpdateListener updateListener;
		
	}
	public void start() {
	}
	public void stop() {
	}
}
