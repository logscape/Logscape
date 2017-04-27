package com.liquidlabs.transport.proxy;

import java.lang.reflect.Method;
import java.util.HashMap;

/**
 * Proxy interface invocation onto a target object 
 *
 */
public class MethodInvoker {
	
	public Object instance;
	private HashMap<String, Method>	hashMap = new HashMap<String, Method>();
	private final String senderId;

	public MethodInvoker(Object instance, String senderId){
		this.instance = instance;
		this.senderId = senderId;
		Class<?>[] interfaces = instance.getClass().getInterfaces();
		for (Class<?> class1 : interfaces) {
			Method[] methods = class1.getMethods();
			for (Method method : methods) {
				hashMap.put(method.toString(), method);
			}			
		}
	}
	public Object invoke(String methodString, Object[] args){
		
		try {
			Method method = hashMap.get(methodString);
			return method.invoke(instance, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public Method getMethod(String methodString) {
		return hashMap.get(methodString);
	}
	public void stop() {
		this.instance = null;
		this.hashMap = null;
	}
	public Object getInstance() {
		return instance == null ? "null" : instance;
	}
	public String toString() {
		return "I:" + getInstance().getClass().getSimpleName() + "[" + senderId + "]";
	}
}
