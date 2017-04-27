package com.liquidlabs.transport;

import java.io.FileOutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.liquidlabs.transport.serialization.Convertor;

/**
 * Delegate to sub-receivers based upon whoever in the list matches the payload-object
 * @author neil
 */
public class MultiReceiver implements Receiver {
	
	private static final Logger LOGGER = Logger.getLogger(MultiReceiver.class);
	private final String srcId;
	
	public MultiReceiver(String srcId) {
		this.srcId = srcId;
	}

	List<Receiver> receivers = new CopyOnWriteArrayList<Receiver>();
	public void addReceiver(Receiver receiver) {
		receivers.add(receiver);
		
	}

	public byte[] receive(byte[] payloadBytes, String remoteAddress, String remoteHostname) throws InterruptedException {
		try {
			Object payload =  Convertor.deserializeAndDecompress(payloadBytes);
			for (Receiver r : this.receivers) {
				// TODO: We see a lot of msgs back to ourselves
				 if (r.isForMe(payload)) {
					 return r.receive(payload, remoteAddress, remoteHostname);
				 }
			}
			LOGGER.warn(this.hashCode() + "/" + this.srcId + " No Receiver for:" + payload + "\n\tAvail:" + this.receivers);
		} catch (Exception e) {
//			try {
//				FileOutputStream fos2 = new FileOutputStream("BbooM-" + System.currentTimeMillis() + "-" + payloadBytes.length + ".gz");
//				fos2.write(payloadBytes);
//				fos2.close();
//			} catch (Throwable t) {
//			}

			LOGGER.warn("Failed to invoke msg from:" + remoteAddress + "/" + remoteHostname  + " bytes:" + payloadBytes.length + " ex:" + e.toString(), e);
		}
		return null;
	}

	public void start() {
	}

	public void stop() {
	}
	public boolean isForMe(Object payload) {
		throw new RuntimeException("Not implemented");
	}
	public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
		throw new RuntimeException("Not implemented");
	}
	@Override
	public String toString() {
		return super.toString() + " :" + this.receivers;
	}

}
