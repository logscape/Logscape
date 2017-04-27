package com.liquidlabs.orm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javolution.util.FastMap;

import org.apache.log4j.Logger;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class ORMapperClientImpl implements ORMapperClient {
	private static final Logger LOGGER = Logger.getLogger(ORMapperClientImpl.class);
	private int depth = TransportProperties.getObjectGraphDepth();
	
	private final ORMapper orMapper;
	private ObjectTranslator objectTranslator = new ObjectTranslator();

	public ORMapperClientImpl(ORMapper orMapper, ProxyFactoryImpl clientFactory) {
		this.orMapper = orMapper;
		fieldCache.shared();
	}
	
	public void addPeer(URI peer) {
		this.orMapper.addPeer(peer);
	}
	public URI getReplicationURI(){
		return this.orMapper.getReplicationURI();
	}

    public void removePeer(URI uri) {
        orMapper.removePeer(uri);
    }

    public String stringValue(Class<?> type, String id) {
        return orMapper.rawObject(type, id);
    }



    public int size() {
		return orMapper.size();
	}
	public Set<String> keySet() {
		return orMapper.keySet();
	}
	public Set<String> leaseKeySet() {
		return orMapper.leaseKeySet();
	}

    public java.util.Map<String, String> exportData() {
        return orMapper.exportData();    
    }

    public void importData(java.util.Map<String, String> data, boolean merge, boolean overwrite) {
		orMapper.importData(data, merge, overwrite);
	}

    public void cancelLease(String leaseKey) {
		orMapper.cancelLease(leaseKey);
	}
	public void renewLease(String leaseKey, int expires){
		orMapper.renewLease(leaseKey, expires);
	}
	public void assignLeaseOwner(String leaseKey, String owner) {
		orMapper.assignLeaseOwner(leaseKey, owner);
	}
	public int renewLeaseForOwner(String owner, int timeoutSeconds) {
		return orMapper.renewLeaseForOwner(owner, timeoutSeconds);
	}
	
	public <T> int purge(List<T> items) {
		int result = 0;
		for (T t : items) {
			String id = getId(t);
			result += orMapper.purge(t.getClass().getName(), id);
		}
		return result;
	}
	public int purge(Class<?> type, String query) {
		String[] findIds = this.findIds(type, query, Integer.MAX_VALUE);
		orMapper.purge(type.getName(), findIds);
		return findIds.length;
	}

	public void store(List<Object> target) {
		for (Object object : target) {
			store(object);
		}
	}
	public <T> void store(T target) {
		store(target, Integer.MAX_VALUE);
	}
	@SuppressWarnings("unchecked")
	public <T> String store(T target, int timeout) {
		String leaseKey = "";
		if (target instanceof List){
			store((List) target);
			return null;
		}
		if (target == null) return null;
		
		String parentId = getId(target);
		leaseKey = orMapper.storeObject(target.getClass().getName(), parentId, objectTranslator.getStringFromObject(target, depth), timeout);
		
		storeChildObjects(target, timeout, parentId);
		return leaseKey;
	}

    public void importData(Class<?> type, String items) {
        store(assemble(type, new String[]{items}), -1);
    }
    public Space getSpace() {
    	return orMapper.getSpace();
    }

	@SuppressWarnings("unchecked")
	public <T> String silentStore(T target, int timeout) {
		String leaseKey = "";
		if (target instanceof List){
			store((List) target);
			return null;
		}
		if (target == null) return null;
		
		String parentId = getId(target);
		leaseKey = orMapper.silentStoreObject(target.getClass().getName(), parentId, objectTranslator.getStringFromObject(target, depth), timeout);
		
		storeChildObjects(target, timeout, parentId);
		return leaseKey;
	}

	private <T> void storeChildObjects(T target, int timeout, String parentId) {
		List<Child> children = getChildren(target);
		for (Child child : children) {
			String childId = getId(child.child);
			Mapping mapping = new Mapping(parentId, target.getClass(),  child.fieldName, childId, child.child.getClass());
			orMapper.storeMapping(target.getClass().getName(), parentId, childId, objectTranslator.getStringFromObject(mapping, depth), timeout);

			store(child.child);
		}
	}
	public <T> T retrieve(Class<T> type, String id, boolean cascade) {
		String[] objectParts = new String[0];
		try {
			String typeName = type.getName();
			objectParts = orMapper.readObject(typeName, id, cascade);
			return assemble(type, objectParts);
		} catch (Throwable t) {
			//	LOGGER.warn(String.format("Failed to assemble object[%s] Id[%s] parts[%s]", type.toString(), id, java.util.Arrays.toString(objectParts)), t);
			throw new RuntimeException(t);
		}
	}

	public int count(Class<?> type, String query) {
		String[] queryStringTemplate = this.objectTranslator.getQueryStringTemplate(type, query);
		return orMapper.count(type.getName(), queryStringTemplate);
	}
	
	public int count(Class<?> type, String query, int limit) {
		String[] queryStringTemplate = this.objectTranslator.getQueryStringTemplate(type, query);
		return orMapper.count(type.getName(), queryStringTemplate, limit);
	}
	
	public <T> boolean containsKey(Class<T> type, String id) {
		return orMapper.containsKey(type.getName(), id);
	}

	public Object getObject(String key) {
		try {
            String typeName = key.substring(0, key.indexOf("-"));
            String[] objectParts = orMapper.readObject(typeName, key.substring(key.indexOf("-")+1, key.length()), true);
            if (objectParts == null || objectParts.length == 0) return null;

			return assemble(Class.forName(typeName), objectParts);
		} catch (Exception e) {
			LOGGER.error("Failed to read Object:", e);
		}
		return null;
	}
	

	public <T> String[] findIds(Class<T> type, String query, int limit) {
		if (query == null) query = "";
		// hack in top level AND - ') AND' or 'AND ('
		if (isTopLevelAND(query)) {
			return processTopLevelANDs(type, query, limit);
		}
		// hack in top level OR  ') OR' or 'OR ('
		else if (isTopLevelOR(query)) {
			return processTopLevelORs(type, query, limit);
		} else {
			String[] queryStringTemplate = this.objectTranslator.getQueryStringTemplate(type, query);
			return orMapper.findIds(type.getName(), queryStringTemplate, limit);
		}
	}
	private <T> String[] processTopLevelORs(Class<T> type, String query, int limit) {
		Set<String> resultsList = new HashSet<String>();
		List<List<String>> allResults = processTopLevel("OR", type, query, limit);
		for (List<String> aSet : allResults) {
			if (resultsList.size() == 0) resultsList.addAll(aSet);
			else resultsList.addAll(aSet);
		}
		return Arrays.toStringArray(resultsList);
	}
	private <T> String[] processTopLevelANDs(Class<T> type, String query, int limit) {
		List<String> resultsList = new ArrayList<String>();
		List<List<String>> allResults = processTopLevel("AND", type, query, limit);
		for (List<String> aSet : allResults) {
			if (resultsList.size() == 0) resultsList.addAll(aSet);
			else resultsList.retainAll(aSet);
		}
		return Arrays.toStringArray(resultsList);
	}

	private  <T> List<List<String>> processTopLevel(String operation, Class<T> type, String query, int limit) {
		String [] newQuery = null;
		if (query.contains(") " + operation + " (")) {
			newQuery = query.split("\\) " + operation + " \\(");
		} else if (query.contains(") " + operation + " ")) {
			newQuery = query.split("\\) " + operation + " ");
		} else if (query.contains(" " + operation + " (")) {
			newQuery = query.split(" " + operation + " \\(");
		}
		List<List<String>> allResults = new ArrayList<List<String>>();
		for (String aQuery : newQuery) {
			aQuery = aQuery.replaceAll("\\(", "");
			aQuery = aQuery.replaceAll("\\)", "");
			String[] queryStringTemplate = this.objectTranslator.getQueryStringTemplate(type, aQuery);
			allResults.add(Arrays.asList(orMapper.findIds(type.getName(), queryStringTemplate, limit)));
		}
		
		return allResults;
	}
	
	private boolean isTopLevelOR(String query2) {
		return query2.contains(") OR") || query2.contains(" OR (");
	}
	private boolean isTopLevelAND(String query2) {
		return query2.contains(") AND") || query2.contains(" AND (");
	}
	public <T> String update(Class<T> type, String key, String update, long timeoutLease){
		String[] updateTemplate = objectTranslator.getQueryStringTemplate(type, update);
		return orMapper.update(type.getName(), key, updateTemplate[0], timeoutLease);
	}
	public <T> int updateMultiple(Class<T> type, String selectQuery, String update, int limit, int timeoutLease, String leaseKey){
		String[] updateTemplate = objectTranslator.getQueryStringTemplate(type, update);
		String[] selectQueryString = objectTranslator.getQueryStringTemplate(type, selectQuery);
		return orMapper.updateMultiple(type.getName(), selectQueryString, updateTemplate, limit, timeoutLease, leaseKey);
	}
	
	public <T> List<T> findObjects(Class<T> type, String query, boolean cascade, int limit) {
		List<T> results = new ArrayList<T>();
		String[] findIds = this.findIds(type, query, limit);
		for (String id : findIds) {
			try {
				results.add(retrieve(type, id, cascade));
			} catch (Throwable t) {
				LOGGER.warn("Failed to find:" + id + " ex:" + t.getMessage());
			}
		}
		return results;
	}
	public <T> List<T> findAndUpdate(Class<T> type, String selectQuery, String updateStatement, long timeoutLease, boolean cascade, int limit, String leaseKey) {
//		List<T> results = findObjects(type, selectQuery, cascade);
		String[] updateTemplate = objectTranslator.getQueryStringTemplate(type, updateStatement);
		String[] selectQueryString = objectTranslator.getQueryStringTemplate(type, selectQuery);
		String[] results = orMapper.findAndUpdate(type.getName(), selectQueryString, updateTemplate, limit, timeoutLease, leaseKey);
		List<T> finalResults = new ArrayList<T>();
		for (String	result : results) {
			try {
				finalResults.add(assemble(type, new String[] { result }));
			} catch (Throwable t) {
				LOGGER.warn("Failed to Find/Update - assemble:" + type.getName() + "ex:" + t.getMessage());
			}
		}
		
		return finalResults;
	}
	
	public <T> List<T> removeObjects(Class<T> type, String query, boolean cascade, long timeout, long expires, int limit) {
		String[] objectParts = orMapper.takeObjects(type.getName(), this.objectTranslator.getQueryStringTemplate(type, query), cascade, limit, timeout, expires);
		List<T> results = new ArrayList<T>();
		int pos = 0;
		for (String valueObject : objectParts) {
			if (valueObject.startsWith(type.getName())){
				try {
					String[] currentObject = new String[objectParts.length - pos];
					System.arraycopy(objectParts, pos, currentObject, 0, currentObject.length);
					results.add(assemble(type, currentObject));
				} catch (Throwable t) {
					LOGGER.warn("Failed to Remove:" + type.getName() + " ex:" + t.getMessage());
				}
			}
			pos++;
			
		}
		
		return results;
	}
	public <T> T remove(Class<T> type, String id, boolean cascade, long timeout, long expires) {
		if (type == null) {
			String typeNameS = id.substring(0, id.indexOf("-"));
			try {
				type = (Class<T>) Class.forName(typeNameS);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		String typeName = type.getName();
		String[] objectParts = orMapper.takeObject(typeName, id, cascade, timeout, expires);
		if (objectParts == null) return null;
		return assemble(type, objectParts);
	}
	
	private <T> T assemble(Class<T> type, String[] objectParts) {
		if (objectParts == null || objectParts.length == 0) {
			throw new RuntimeException(String.format("Failed to construct type:%s obj[null]", type));
		}
		List<String> mappings = new ArrayList<String>();
		for (String item : objectParts) {
			if (item.contains("<Mapping>")){
				mappings.add(item);
			}
		}
		
		T result = objectTranslator.getObjectFromFormat(type, objectParts[0], depth);
		
		if (result == null) throw new RuntimeException(String.format("Failed to construct type:%s from obj[%s]", type, objectParts[0]));
		applyMappings(result, mappings, objectParts);
		return result;
	}


	public String registerEventListener(Class<?> type, String template, ORMEventListener callback, Type[] types, int timeout) {
		try {
			String[] queryStringTemplate = objectTranslator.getQueryStringTemplate(type, template);
			return orMapper.registerEventListener(queryStringTemplate, callback, types, timeout);
		} catch (Throwable t) {
			throw new RuntimeException("Failed to registerListener:" + type + " template:" + template, t);
		}
	}
	public boolean unregisterEventListener(String listenerId) {
		return orMapper.unregisterEventListener(listenerId);
	}

	
	/**
	 * Apply mappings to this objectInstance and recursively visit child nodes
	 * @param instance
	 * @param mappings
	 * @param items
	 */
	public void applyMappings(Object instance, List<String> mappings, String[] items){
		Class<? extends Object> class1 = instance.getClass();
		int itemMappingPos = 0;
		String objectId = getId(instance);
		
		List<String> mappingsForThisInstance = getMappingsForThisObject(class1.getName(), objectId, mappings);
		for (String mapping : mappingsForThisInstance) {
			try {
				Mapping rMapping = (Mapping) objectTranslator.getObjectFromFormat(Mapping.class, mapping, depth);
				// find mappings for this object type AND Id
				for (int i = 0; i < items.length; i++){
					if (items[i].equals(mapping)){
						itemMappingPos = i;
					}
				}
				// get the child Item - should be item after the mapping
				String child = items[itemMappingPos+1];
				Class<?> childClass = Class.forName(rMapping.getChildTypeName());
				Object childInstance = objectTranslator.getObjectFromFormat(childClass, child, depth);
				rMapping.apply(instance, childInstance);
				
				// visit child elements
				applyMappings(childInstance, mappings, items);

			} catch (Throwable t){
				LOGGER.warn(t);
			}
			
		}
	}

	private List<String> getMappingsForThisObject(String name, String objectInstanceId, List<String> mappings) {
		List<String> results = new ArrayList<String>();
		for (String mapping : mappings) {
			Mapping mappingItem = objectTranslator.getObjectFromFormat(Mapping.class, mapping, depth);
			if (mappingItem.getParentId().equals(objectInstanceId) && mappingItem.getParentTypeName().equals(name)) {
				results.add(mapping);
			}
		}
		return results;
	}

	private List<Child> getChildren(Object target) {
		Field[] declaredFields = target.getClass().getDeclaredFields();
		List<Child> results = new ArrayList<Child>();
		
		for (Field field : declaredFields) {
			try {
				Map annotation = field.getAnnotation(Map.class);
				if (annotation == null) continue;
				field.setAccessible(true);
				results.add(new Child(field.get(target), field.getName()));
			} catch (Throwable t){
			}
		}
		return results;
	}
	

	
	public static class Child {
		public Child(Object child, String fieldName) {
			this.child = child;
			this.fieldName = fieldName;
		}
		Object child;
		String fieldName;
	}


	private String getId(Object target) {
		Class<? extends Object> class1 = target.getClass();
		try {
			return getIdAnnotation(target, class1);
		} catch (Throwable t){
			return getIdAnnotation(target, target.getClass().getSuperclass());
		}
	}

	FastMap<Class,Field> fieldCache = new FastMap<Class, Field>();
	private String getIdAnnotation(Object target, Class<? extends Object> class1) {
		if (fieldCache.containsKey(class1)) {
			try {
				return String.valueOf(fieldCache.get(class1).get(target));
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		} else {
			Field[] declaredFields = class1.getDeclaredFields();
			
			for (Field field : declaredFields) {
				try {
					Id annotation = field.getAnnotation(Id.class);
					if (annotation == null) continue;
					field.setAccessible(true);
					return String.valueOf(field.get(target));//.toString();
				} catch (Throwable t){
					LOGGER.warn(t);
				}
			}
		}
		throw new RuntimeException("No @Id found on:" + class1);
	}
}
