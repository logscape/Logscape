package com.liquidlabs.transport.proxy.addressing;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.RetryInvocationException;

/**
 * only rotates on failure, but orders the last part of the list so all peers
 * will fail in the same order
 * 
 * @author neil
 * 
 */
public class KeepOrderedAddresser implements AddressHandler, Externalizable, KryoSerializable {

	private static final Logger LOGGER = Logger.getLogger(KeepOrderedAddresser.class);

	private String serviceName = "";
	List<URI> endPoints = new ArrayList<URI>();
	List<URI> blackList = new ArrayList<URI>();
	protected boolean replayRequired = false;

	private RefreshAddrs updateTask;

	public KeepOrderedAddresser(String serviceName) {
		this.serviceName = serviceName;
	}

	public KeepOrderedAddresser() {
	}

	/**
	 * Note: Order is PARAMOUNT! it contains ZONE Failover preference so make
	 * sure it is maintained
	 */
	public synchronized void addEndPoints(String... givenEndPoints) {
		if (givenEndPoints == null)
			return;

		URI currentEP = endPoints.isEmpty() ? null : endPoints.get(0);
		endPoints.clear();

		for (String newEP : givenEndPoints) {
			try {
				URI newURI = new URI(newEP);
				if (!endPoints.contains(newURI)) endPoints.add(newURI);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
		URI newEP =  endPoints.isEmpty() ? null : endPoints.get(0);
		
		if (isModifiedURI(currentEP, newEP)) {
			replayRequired = true;
			replayReason = "Detected modified URI";
		}

		messWithList();
	}
	private boolean isModifiedURI(URI currentEP, URI newEP) {
		if (currentEP == null && newEP != null) return true;
		if (newEP == null) return false;
		return !currentEP.equals(newEP);
	}
	String replayReason = "";

	public synchronized void syncEndpoints(String[] addresses) {
		// use above method instead
	}

	/**
	 * Called when something is wrong!
	 * It will refresh the list and also use the next endpoint if the current one if first in the list
	 */
	synchronized public void validateEndPoints() {
		
		try {
			if (this.updateTask != null) {
				LOGGER.info(this.serviceName + " Refreshing Endpoints existingEPS:" + this.endPoints);
				String[] addresses = updateTask.getAddresses();
				int retry = 0;
				while ((addresses == null || addresses.length == 0) && retry++ < 10) {
					try {
						addresses = updateTask.getAddresses();
						Thread.sleep(10 * 1000);
					} catch (InterruptedException t) {
						throw new RuntimeException(t);
					} catch (Throwable t) {
					}
				}
				if (addresses == null) {
					LOGGER.info(this.serviceName + " Null Address List- bailing");
					return;
				}
				if (addresses.length > 0) {
					LOGGER.debug(" - new EndPoints:" + Arrays.toString(addresses));
					this.addEndPoints(addresses);
				}
			} else if (this.endPoints.size() > 1) {
				LOGGER.debug(this.serviceName + "_HA_ Refreshing Endpoints was not given a refresh Task:" + endPoints);
			} else if (this.endPoints.size() == 0) {
				this.endPoints = new ArrayList<URI>(blackList);
				blackList.clear();
			}
			
		} catch (Throwable t) {
			LOGGER.warn(this.serviceName + " Failed to execute Updater, currentEP:", t);
		}
	}

	public void registerAddressRefresher(RefreshAddrs update) {
		this.updateTask = update;
	}

	

	private String getURLValueOnly(String string) {

		if (string.contains("://")) {
			try {
				URI uri = new URI(string);
				return uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
			} catch (URISyntaxException e) {
			}
		}
		return null;
	}

	protected void messWithList() {
	}

	public boolean isEndPointAvailable() {
		return !this.endPoints.isEmpty();
	}

	public List<URI> getEndPointURIs() {
		if (this.endPoints.isEmpty()) validateEndPoints();
		return new ArrayList<URI>(endPoints);
	}

	public URI getEndPointURI() throws RetryInvocationException {
		try {
			return getEndPointURIs().get(0);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public URI getEndPointURISafe()  {
	List<URI> endPointURIs = getEndPointURIs();
		if (endPointURIs.isEmpty())
			return null;
		return endPointURIs.get(0);

	}

	synchronized public void registerFailure(URI uri) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Registering EP Failure:" + uri);
		this.endPoints.remove(uri);
		if (!blackList.contains(uri)) blackList.add(uri);
		this.replayRequired = true;
		replayReason = "Registered Failure";
	}

	public void resetReplayFlag() {
		this.replayRequired = false;
		this.replayReason = "";
	}
	public String replayReason() {
		return this.replayReason;
	}

	public boolean isReplayRequired() {
		return this.replayRequired;
	}

	public synchronized boolean remove(String removeAddresses) {
		boolean remove = false;
		boolean removedCurrent = false;
		for (String removedURI : removeAddresses.split(",")) {
			try {
				URI uri = new URI(removedURI);
				removedCurrent = uri.equals(this.endPoints.get(0)) ? true : removedCurrent;
				remove = this.endPoints.remove(uri) || remove;
				// make replay when swapping from current addresses
				blackList.remove(removedURI);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/**
		 * Current EP was removed - so we need to trigger a replay
		 */
		if (this.endPoints.isEmpty() || removedCurrent) {
			this.replayRequired = true;
			replayReason = "Removed:" + removeAddresses;
		}
		return remove;
	}

	public String toString() {
		while (true) {
			try {
				StringBuilder builder = new StringBuilder();
				builder.append("[");
				builder.append(getClass().getSimpleName());
				builder.append(":" + this.serviceName);
				builder.append(" /:");
				builder.append(" list[");
				builder.append(endPoints);
				builder.append("] badEP");
				builder.append(blackList.toString());
				builder.append(" replay?:");
				builder.append(replayRequired);
				builder.append(" reason:");
				builder.append(replayReason);

				builder.append("]");
				return builder.toString();
			} catch (ConcurrentModificationException ex) {
			}
		}
	}

	public Collection<URI> blackList() {
		return blackList;
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(serviceName);
		out.writeObject(this.endPoints);
	}
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.serviceName = in.readUTF();
		this.endPoints = (List<URI>) in.readObject();
	}
	public void read(Kryo kryo, Input input) {
		serviceName = kryo.readObject(input, String.class);
		List<String> addrs = kryo.readObject(input, ArrayList.class);
		this.endPoints = new ArrayList<URI>();
		for (String addr : addrs) {
			try {
				endPoints.add(new URI(addr));
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
	}
	public void write(Kryo kryo, Output output) {
		kryo.writeObject(output, serviceName);
		List<String> addrs = new ArrayList<String>();
		for (URI uri : this.endPoints) {
			addrs.add(uri.toString());
		}
		kryo.writeObject(output, addrs);
	}
}
