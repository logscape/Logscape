package com.liquidlabs.orm;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.impl.SpacePeer;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * Provides an object relational mapping persistence mechanism for
 * storing/retrieving objects from a distributed vspace cluster. Alternative to
 * Hibernate+RDBMs or ObjectDBs
 * 
 * 
 */
public class ORMapperImpl implements ORMapper {
	private static final String DASH = "-";

	private static final Logger LOGGER = Logger.getLogger(ORMapper.class);
	
	private final Space space;
	
	ObjectTranslator query = new ObjectTranslator();

	public ORMapperImpl(Space space) {
		this.space = space;
	}
	public int size() {
		return space.size();
	}
	
	public Set<String> keySet() {
		return space.keySet();
	}
	public Set<String> leaseKeySet() {
		return space.leaseKeySet();
	}

    public java.util.Map<String, String> exportData() {
        return space.exportData();
    }

    public void importData(Map<String, String> data, boolean merge, boolean overwrite) {
		space.importData(data, merge, overwrite);
	}


    public void cancelLease(String leaseKey) {
		space.cancelLease(leaseKey);
	}
	public void renewLease(String leaseKey, int expires){
		space.renewLease(leaseKey, expires);
	}
	
	public void addPeer(URI peer) {
		space.addPeer(peer);
	}
	
	public URI getReplicationURI() {
		return space.getReplicationURI();
	}

    public void removePeer(URI uri) {
        space.removePeer(uri);
    }

    public String rawObject(Class<?> type, String id) {
        return space.read(getKey(type.getName(), id));
    }
    public Space getSpace() {
    	return space;
    }

    public void assignLeaseOwner(String leaseKey, String owner) {
		space.assignLeaseOwner(leaseKey, owner);
	}
	public int renewLeaseForOwner(String owner, int timeoutSeconds) {
		return space.renewLeaseForOwner(owner, timeoutSeconds);
	}
	public int purge(String type, String id) {
		try {
			return space.purge(getKey(type, id));
		} catch (Throwable t){
			if (t.getMessage() != null && t.getMessage().contains("Failed to locate")) return 0;
			throw new RuntimeException(t);
		}
	}
	private String getKey(String type, String id) {
		return new StringBuilder(type).append(DASH).append(id).toString();
	}
	public void purge(String type, String... ids) {
		try {
			List<String> idList = new ArrayList<String>();
			for (String id : ids) {
				idList.add(getKey(type, id));
			}
			space.purge(idList);
			
		} catch (Throwable t){
			if (t.getMessage() != null && t.getMessage().contains("Failed to locate")) return;
			throw new RuntimeException(t);
		}
	}
	
	public int purge(String[] template) {
		String[] takeMultiple = space.takeMultiple(template, -1, -1, -1);
		return takeMultiple.length;
	}
	
	public void update(ORMItem[] items, int timeout) {
		for (ORMItem item : items) {
			storeObject(item.type, item.id, item.contents, timeout);
		}
	}
	public String update(String type, String key, String updateTemplate, long timeout) {
		return space.update(getKey(type, key), updateTemplate, timeout);
	}
	public int updateMultiple(String type, String[] selectQueryString, String[] updateTemplate, int limit, long timeout, String leaseKey) {
		return space.updateMultiple(selectQueryString, updateTemplate, timeout, limit, leaseKey);
	}
	public String[] findAndUpdate(String name2, String[] selectQueryString, String[] updateTemplate, int limit, long timeoutLease, String leaseKey) {
		return space.readAndUpdateMultiple(selectQueryString, updateTemplate, timeoutLease, limit, leaseKey);
	}

	public void storeMapping(String type, String id, String childId, String mapping, int timeout) {
		space.write(type + DASH + id + DASH + childId + "-mapping", mapping, timeout);
	}
	
	public String[] findIds(String class1, String[] template, int limit) {
		String[] results = space.readKeys(template, limit);
		for(int i = 0; i < results.length; i++){
			String source = results[i];
			int firstDash = source.indexOf(DASH)+1;
			results[i] = results[i].substring(firstDash, source.length());
		}
		return results;		
	}
	public int count(String class1, String[] template) {
		return space.count(template, -1);
	}

	public int count(String class1, String[] template, int limit) {
		return space.count(template, limit);
	}
	public boolean containsKey(String type, String id) {
		return space.containsKey(getKey(type, id));
	}
	
	/**
	 * Return Object contents, mappings, and children
	 * @param type - className
	 * @param cascade - should mappings and child objects be returned
	 * @param timeout 
	 * @param expires 
	 */
	public String[] takeObjects(String type, String[] template, boolean cascade, int limit, long timeout, long expires) {
		if (limit < 0) limit = Integer.MAX_VALUE;
		String[] findIds = this.findIds(type, template, limit);
		List<String> results = new ArrayList<String>();
		int count = 0;
		for (String string : findIds) {
			if (count < limit){
				String[] takeObject = takeObject(type, string, cascade, timeout, expires);
				if (takeObject != null){
					for (String objectPart : takeObject) {
						results.add(objectPart);
					}
					count++;
				}
			}
		}
		return Arrays.toStringArray(results);
	};
	
	public String[] takeObject(String type, String id, boolean cascade, long timeout, long expires) {
		// load the object
		String objectContents = space.take(getKey(type, id), timeout, expires);
		if (objectContents == null) return null;
		List<String> results = new ArrayList<String>();
		if (objectContents != null) results.add(objectContents);
		if (!cascade){
			return Arrays.toStringArray(results);
		}
	
		String[] queryStringTemplate = query.getQueryStringTemplate(Mapping.class, "parentId equals " + id + " AND parentTypeName equals "
				+ type);
	
		// load the mapping
		String[] objectMappings = space.takeMultiple(queryStringTemplate, 1000, timeout, expires);
		for (String objectMapping : objectMappings) {
			if (!objectMapping.contains("<Mapping>"))
				continue;
			results.add(objectMapping);
			Mapping mapping = (Mapping) query.getObjectFromFormat(Mapping.class, objectMapping);
			String[] childContents = this.takeObject(mapping.getChildTypeName(), mapping.getChildId(), cascade, timeout, expires);
			for (String childContent : childContents) {
				results.add(childContent);
			}
		}
	
		return Arrays.toStringArray(results);
	}
	/**
	 * Return Object contents, mappings, and children
	 * @param type - className
	 * @param id - uniqueId/entityId
	 * @param cascade - should mappings and child objects be returned
	 */
	public String[] readObject(String type, String id, boolean cascade) {
		// load the object
		String objectContents = space.read(getKey(type, id));
		if (objectContents == null) return new String[0];
		List<String> results = new ArrayList<String>();
		if (objectContents != null) results.add(objectContents);
		if (!cascade){
			return Arrays.toStringArray(results);
		}
		
		try {
			String[] queryStringTemplate = query.getQueryStringTemplate(Mapping.class, "parentId equals '" + id + "' AND parentTypeName equals '" + type+"'");
			
			// load the mapping
			String[] objectMappings = space.readMultiple(queryStringTemplate, Integer.MAX_VALUE);
			for (String objectMapping : objectMappings) {
				if (!objectMapping.contains("<Mapping>"))
					continue;
				results.add(objectMapping);
				Mapping mapping = (Mapping) query.getObjectFromFormat(Mapping.class, objectMapping);
				// TODO: had test blowing up because the childId was the same as this objects id
				// this check prevents recursive boom!
				if (mapping.getChildId().equals(id)) return new String[0];
				String[] childContents = readObject(mapping.getChildTypeName(), mapping.getChildId(), cascade);
				for (String childContent : childContents) {
					results.add(childContent);
				}
			}
			
			return Arrays.toStringArray(results);
		} catch (Throwable t) {
			//LOGGER.warn("Failed to load ChildMapping, this error can be ignored it the object has no children:" + t.toString());
		}
		return Arrays.toStringArray(results);
	}

	/**
	 * When an object gets stored, look for any OR annotations and if one
	 * exists, copy those fields names and values into a mapping instance
	 * created from the template
	 * 
	 * @param type =
	 *            "UserType-122222"
	 * @param id -
	 *            "valueA,valueB,mappedTypeInstance"
	 * @param contents
	 * @return 
	 */
	public String storeObject(String type, String id, String contents, long timeout) {
		String key = getKey(type, id);
		return space.write(key, contents, timeout);
	}
	public String silentStoreObject(String type, String id, String contents, long timeout) {
		String key = getKey(type, id);
		return space.internalWrite(key, contents, timeout);
	}
	public String registerEventListener(String[] templates, final ORMEventListener callback, Type[] eventMask, long expires) {
		try {
			EventListener eventListener = new EventListener() {
				public String getId() {
					return callback.getId();
				}

				public void notify(Event event) {
					callback.notify(event.getKey(), event.getValue(), event.getType(), event.getSource());
				}
			};
			String leaseKey = space.notify(new String[0], templates, eventListener, eventMask, expires);
			if (leaseKey == null)
				leaseKey = "none";
			return leaseKey;
		} catch (Exception e) {
			LOGGER.error("Failed to register eventListener:" + callback.getId(), e);
			return "Failed to register eventListener:" + callback.getId();
		}
	}
	public boolean unregisterEventListener(String listenerId) {
		return space.removeNotification(listenerId);
	}
	

	public static void main(String[] args) {
		System.out.println("ORMapperSpace args:" + Arrays.toString(args));
		int port = 1100;
		int spaceSize = 5 * 1024;
		String partitionName = NAME;
		if (args.length == 0 || args[0].equals("--help")) {
			System.out.println("usage: orm-exe -port:15000 -partition:somePartitionName");
			System.out.println("automatically selecting a port, searching from:" + port);
		} else {
			for (String arg : args) {
				if (arg.startsWith("-port:")){
					port = Integer.parseInt(arg.split(":")[1]);
				}
				if (arg.startsWith("-partition:")){
					partitionName = arg.split(":")[1] + "-ORM";
				}
				if (arg.startsWith("-size:")){
					spaceSize = Integer.parseInt(arg.split(":")[1]);
				}
			}
		}
		System.out.println("Using port:" + port + " parition:" + partitionName + " -size:" + spaceSize);
		
		try {
			TransportFactory transportFactory = new TransportFactoryImpl(Executors.newSingleThreadExecutor(new NamingThreadFactory("orMapper")), "main");
			transportFactory.start();
			
			SpacePeer spacePeer = new SpacePeer(TransportFactoryImpl.getDefaultProtocolURI(null, NetworkUtils.getHostname(), port, "ORMService"));
			Space space = spacePeer.createSpace(partitionName + "-space", true, false);
			 ORMapper service = new ORMapperImpl(space);
			 spacePeer.addReceiver(partitionName, service, ORMapper.class);
			 spacePeer.start();
			while (true) {
				Thread.sleep(5000);
			}
		} catch (Exception e) {
			LOGGER.warn(e);
		}
	}

}
