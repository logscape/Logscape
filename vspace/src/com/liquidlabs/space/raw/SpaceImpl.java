package com.liquidlabs.space.raw;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.map.Map;
import com.liquidlabs.space.map.MapIsFullException;
import com.liquidlabs.space.map.UpdateListener;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.transport.PeerSender;
import com.liquidlabs.transport.proxy.events.BlockingEventListener;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.serialization.Matchers;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SpaceImpl implements Space, UpdateListener {
	private static final Logger LOGGER = Logger.getLogger(SpaceImpl.class);
	public static String TAG = "SPACE";
	
	Map dataMap;
	Matchers matchRules = new Matchers();
	UpdaterRules updateRules = new UpdaterRules();
	EventHandler eventHandler;
	private final String partitionName;
	private PeerSender peerSender;
	private URI replicationUri;
	private SpaceImplAdmin spaceAdmin;

	public SpaceImpl(String partitionName, Map map, EventHandler eventHandler) {
		this.partitionName = partitionName;
		this.dataMap = map;
		this.eventHandler = eventHandler;
		this.spaceAdmin = new SpaceImplAdmin(this, partitionName, map.getMaxSize());
	}

	public int count(String[] templates, int limit) {
		if (limit < 0) limit = Integer.MAX_VALUE;
		String[] keys = getKeys(templates, limit);
		return keys.length;
	}
	public int size() {
		return dataMap.size();
	}

	public String write(String key, String value, long timeoutSeconds) {
		Type eventType = Event.Type.WRITE;
		if (dataMap.keySet().contains(key)){
			eventType = Event.Type.UPDATE;
		}
		// only generate an event if the value HAS changed!
		//
//		String existingValue = internalRead(key);
//		if (value.equals(existingValue)) {
//			return "existing value is the same";
//		}
		
		eventHandler.handleEvent(new Event(key, value, eventType));
		internalWrite(key, value, -1);
		return null;
	}
	public void cancelLease(String leaseKey) {
		throw new RuntimeException("not implemented");
	}
	public void renewLease(String leaseKey, long expires) {
		throw new RuntimeException("not implemented");
	}

	public Set<String> keySet() {
		return dataMap.keySet();
	}
	public Set<String> leaseKeySet() {
		return new HashSet<String>();
	}
	public boolean containsKey(String key){
		return this.dataMap.containsKey(key);
	}

	public String notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires) {
		eventHandler.notify(keys, templates, listener, eventMask, expires);
		return listener.getId();
	}
	public boolean removeNotification(String listenerKey) {
		return eventHandler.removeListener(listenerKey);
	}

	public String internalWrite(String key, String value, long timeout) {
		try {
            if (key == null || value == null) throw new RuntimeException(partitionName + " Cannot store NULL Values");
			dataMap.put(key, value);
			this.updatedValue(key, value, false);
		} catch (MapIsFullException e) {
//			String string = String.format("Space: size[%d] name[%s] is full, new Event[%s][%s], overwriting oldest item", dataMap.getValues().length, this.partitionName, key, value);
//			LOGGER.warn(string, new RuntimeException(string));
//			System.out.println("Space:" + this.partitionName + " is full, new Event[" + key + "][" + value + "], overwriting oldest item");
//			String oldestKey = dataMap.getOldestKey();
//			System.out.println("OldestKey:" + oldestKey);
//			this.take(oldestKey, -1, -1);
			try {
				dataMap.put(key, value);
				this.updatedValue(key, value, false);
			} catch (MapIsFullException e1) {
				LOGGER.error(e1);
			}
		}
		return null;
	}

	public String update(String key, String updateTemplate, long timeout) {
		String value = internalRead(key);
		String newValue = mergeUpdateValue(value, updateTemplate);
		eventHandler.handleEvent(new Event(key, newValue, Event.Type.UPDATE));
		internalWrite(key, newValue, timeout);
		return null;
	}

	public int updateMultiple(String[] templates, String[] updateTemplates, long timeout, int limit, String leaseKey) {
		int result = 0;
		for (String key : readKeys(templates, limit)) {
			for (String updateTemplate : updateTemplates) {
				update(key, updateTemplate, -1);
				result++;
			}
		}
		return result;
	}

	String mergeUpdateValue(String value, String updateTemplate) {
		if (value == null) return null;
		String[] split = Arrays.split(DELIM, value);
		String[] splitTemplate = Arrays.split(DELIM, updateTemplate);
		for (Updater updater : this.updateRules.rules) {
			for (int i = 0; i < splitTemplate.length; i++) {
				if (updater.isApplicable(splitTemplate[i])) {
					split[i] = updater.update(split[i], splitTemplate[i]);
				}
			}
		}
		return Arrays.toStringWithDelim(DELIM, split);
	}


	public String take(String key, long timeout, long expires) {
		String value = internalRead(key);
		if (value == null) waitForValueOrTimeout(new String[] { key }, new String[0], timeout);
		eventHandler.handleEvent(new Event(key, value, Event.Type.TAKE));		
		dataMap.remove(key, true);
		return value;
	}
	public String internalTake(String key) {
		return dataMap.remove(key, false);
	}

	public int purge(String key) {
		String take = internalTake(key);
		return take != null ? 1 : 0;
	}
	public void purge(List<String> key) {
		for (String string : key) {
			purge(string);
		}
	}

	public String[] take(String[] keys) {
		List<String> result = new ArrayList<String>();
		for (String key : keys) {
			String item = read(key);
			if (item == null && containsKey(key)) {
				LOGGER.info("****** NULL VALUE ON TAKE - key:" + key);
				LOGGER.info("****** GOING TO REMOVE BAD ITEM" + key);
				LOGGER.warn(new RuntimeException("NULL V ON TAKE"));
				dataMap.remove(key, true);
				continue;
			}
			result.add(item);
			eventHandler.handleEvent(new Event(key, item, Event.Type.TAKE));	
			dataMap.remove(key, true);
		}
		return Arrays.toStringArray(result);
	}

	public String[] takeMultiple(String[] template, int limit) {
		String[] readKeys = readKeys(template, limit);
		return take(readKeys);
	}

	public String[] takeMultiple(String[] template, int limit, long timeout, long expires) {
		if (timeout < 0) timeout = Integer.MAX_VALUE;
		if (limit < 0) limit = Integer.MAX_VALUE;
		String[] readKeys = readKeys(template, limit);
		if (readKeys == null) waitForValueOrTimeout(new String[0], template, timeout);
		if (readKeys.length == 0) return new String[0];
		String[] results = Arrays.subArray(readKeys, 0, Math.min(readKeys.length, limit));
		return take(results);
	}

	public String internalRead(String key) {
		return dataMap.get(key);
	}

	public String read(String key) {
		String value = internalRead(key);
		eventHandler.handleEvent(new Event(key, value, Event.Type.READ));
		return value;
	}
	public String read(String key, long timeout) {
		String value = internalRead(key);
		if (value == null) waitForValueOrTimeout(new String[] { key }, new String[0], timeout);
		value = internalRead(key);
		eventHandler.handleEvent(new Event(key, value, Event.Type.READ));
		return value;
	}

    public java.util.Map<String, String> exportData()
    {
        return dataMap.export();
    }

    public void importData(java.util.Map<String, String> data, boolean merge, boolean overwrite)
    {
        try {
            dataMap.importData(data, merge, overwrite);
        } catch (MapIsFullException e) {
            LOGGER.warn("SPACE - Map is full on import data");
        }
    }

	private void waitForValueOrTimeout(String[] keys, String[] templates, long timeout) {
		if (timeout < 0) timeout = Integer.MAX_VALUE;
		if (timeout < Integer.MAX_VALUE){
			BlockingEventListener blockUntilFiredListener = new BlockingEventListener();
			eventHandler.notify(keys , templates, blockUntilFiredListener, new Type[] { Type.WRITE }, timeout);
			blockUntilFiredListener.waitUntilNotified(timeout);	
		}
	}

	public String[] read(String[] keys) {
		String[] results = new String[keys.length];
		int pos = 0;
		for (String key : keys) {
			String value = internalRead(key);
			eventHandler.handleEvent(new Event(key, value, Event.Type.READ));
			results[pos++] = value;
		}
		return results;
	}

	public String[] readKeys(String[] templates, int limit) {
		if (limit < 0) limit = Integer.MAX_VALUE;
		List<String> results = new ArrayList<String>();
		String[] keys = getKeys(templates, limit);
		for (String key : keys) {
			if (results.size() < limit) results.add(key);
		}
		return Arrays.toStringArray(results);
	}

	public String[] readMultiple(String[] templates, long timeout, int limit) {
		if (limit < 0) limit = Integer.MAX_VALUE;
		if (timeout < 0) timeout = Integer.MAX_VALUE;
		String[] readMultiple = readMultiple(templates, limit);
		if (readMultiple.length == 0) {
			waitForValueOrTimeout(new String[0], templates, timeout);
		}
		return Arrays.subArray(readMultiple, 0, Math.min(limit, readMultiple.length));
	}
	public String[] readMultiple(String[] templates, int limit) {
		if (limit < 0) limit = Integer.MAX_VALUE;
		List<String> results = new ArrayList<String>();
		String[] strings = get(templates, Math.abs(limit - results.size()), true);
		results.addAll(Arrays.asList(strings));
//		for (String string : templates) {
//			String[] strings = get(Arrays.split(DELIM, string), Math.abs(limit - results.size()), true);
//			results.addAll(Arrays.asList(strings));
//		}
		List<String> subList = results.subList(0, Math.min(limit, results.size()));
		return Arrays.toStringArray(subList);
	}
	
	public String[] readAndUpdateMultiple(String[] template, String[] update, long timeout, int limit, String leaseKey) {
		String[] results = readMultiple(template, limit);
		updateMultiple(template, update, timeout, limit, leaseKey);
		return results;
	}
	
	private String[] get(String[] searchTemplates, int limit, boolean notify) {
//		String[][] searchTemplate2 = matchRules.trimSearchTemplate(searchTemplate);
		ArrayList<String> results = new ArrayList<String>();
		for (String template : searchTemplates) {
			String[] splitTemplate = Arrays.split(DELIM, template);
			boolean isColumnBasedEval = matchRules.isMatcherColumnBased(splitTemplate);
			int columnCount = isColumnBasedEval ? matchRules.getMatcherColumnCount(splitTemplate) : 0;
			String[][] searchTemplate2 = matchRules.trimSearchTemplate(splitTemplate);
			if (isColumnBasedEval) {
				// allow N columns
				for (int col = 0; col < columnCount; col++) {
					for (String key : this.dataMap.keySet()) {
						String[] itemLookup = this.dataMap.getSplit(key);
						if (results.size() < limit && (matchRules.isMatch(itemLookup, searchTemplate2[0], searchTemplate2[1], false, col))
								
								) {
							String value = this.dataMap.get(key);
							if (notify) eventHandler.handleEvent(new Event(key, value, Event.Type.READ));
							results.add(value);
						}
					}
				}
				
			} else {
				for (String key : this.dataMap.keySet()) {
					String[] itemLookup = this.dataMap.getSplit(key);
					if (results.size() < limit && (matchRules.isMatch(itemLookup, searchTemplate2[0], searchTemplate2[1], false, 0))
							
							) {
						String value = this.dataMap.get(key);
						if (notify) eventHandler.handleEvent(new Event(key, value, Event.Type.READ));
						results.add(value);
					}
				}
			}
		}
		return Arrays.toStringArray(results);
	}
	
	public String[] getKeys(String[] searchTemplates, int limit) {
		if (limit < 0) limit = Integer.MAX_VALUE;
		// ensure ordering and duplicate evaluation
		Set<String> processedKeys = new HashSet<String>();
		ArrayList<String> results = new ArrayList<String>();
		
		
		for (String template : searchTemplates) {
			
			String[] splitTemplate = Arrays.split(DELIM, template);
			boolean isColumnBasedEval = matchRules.isMatcherColumnBased(splitTemplate);
			int columnCount = isColumnBasedEval ? matchRules.getMatcherColumnCount(splitTemplate) : 0;
			String[][] searchTemplate2 = matchRules.trimSearchTemplate(splitTemplate);
			if (isColumnBasedEval) {
				// allow N columns
				for (int col = 0; col < columnCount; col++) {
					for (String key : this.dataMap.keySet()) {
						if (processedKeys.contains(key)) continue;
						String[] itemLookup = this.dataMap.getSplit(key);
						if (results.size() < limit && matchRules.isMatch(itemLookup, searchTemplate2[0], searchTemplate2[1], false, col)) {
							processedKeys.add(key);
							results.add(key);
						}
					}
				}
				
			} else {
				for (String key : this.dataMap.keySet()) {
					
					if (processedKeys.contains(key)) continue;
                    try {
                        String[] itemLookup = this.dataMap.getSplit(key);
                        if (results.size() < limit && matchRules.isMatch(itemLookup, searchTemplate2[0], searchTemplate2[1], false, 0)) {
                            processedKeys.add(key);
                            results.add(key);
                        }
                    } catch (Throwable t) {
                        LOGGER.error("Failed to build Item key:" + key);
                    }
				}
			}
		}
		return Arrays.toStringArray(results);
	}
	public void addPeer(URI uri) {
		LOGGER.warn(this.partitionName  + " AddingPeer:" + uri);
		if (this.spaceAdmin.addPeer(uri)) {
			try {
				this.dataMap.syncBulkUpdate();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
	public void addPeerListener(PeerSender peerSenderListener) {
		this.peerSender = peerSenderListener;
	}
	
	public void setReplicationURI(URI replicationUri) {
		this.replicationUri = replicationUri;
	}
	public URI getReplicationURI() {
		return replicationUri;
	}

    public void removePeer(URI uri) {
        if (peerSender != null){
            peerSender.removePeer(uri);
        }
        this.spaceAdmin.removePeer(uri);
    }

    /**
	 * 
	 * Updates to the underlying map are copied here with splitting
	 * to speed up template matching. This call is also made from cluster peers
	 */
	public void updatedValue(String key, String value, boolean notifySpace) {
		if (value != null && Event.isA(value)) {
			Event event = new Event();
			event.fromString(value);
//			LOGGER.info(SLoggerConfig.VS_MAP + "Handling Event:" + event.toString());
			eventHandler.handleEvent(event);
			return;
		}
	}

	public EventHandler getEventHandler() {
		return eventHandler;
	}
	public void assignLeaseOwner(String leaseKey, String owner) {
	}
	public int renewLeaseForOwner(String owner, int timeoutSeconds) {
		return 0;
	}
	public void start() {
	}
	public void stop() {
	}
}
