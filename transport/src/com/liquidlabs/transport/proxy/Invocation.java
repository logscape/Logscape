package com.liquidlabs.transport.proxy;

import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.clientHandlers.Interceptor;
import com.liquidlabs.transport.serialization.Convertor;

public class Invocation implements KryoSerializable {
	
	private static final long serialVersionUID = 1L;

	String clientURI;
	String partitionName;
	String srcId = "none";
	String destId = "none";
	
	transient Method method;
	String methodName = "none";

	Object result;
	Object[] args;

	private InvocationState currentState;

	private transient Convertor convertor;

	transient private Throwable throwable;

	static transient Map<String, Method> methodNameCache = new ConcurrentHashMap<String, Method>();

	public Invocation(){
		convertor = new Convertor();
	}
	public Invocation(Method method, Convertor convertor) {
		this.convertor = convertor;
		setMethod(method);
	}
	public String toString(String srcId, String destId, String clientURI, String partitionName, Object result, Object... args){
		set(srcId, destId, clientURI, partitionName, result, args);
		return this.toString();
	}

	public void set(String srcId, String destId, String clientURI, String partitionName, Object result, Object... args) {
		this.srcId = srcId;
		this.destId = destId;
		this.clientURI = clientURI;
		this.partitionName = partitionName;
		this.args = args;
		setResult(result);
	}
	
	public String getClientURI() {
		return clientURI;
	}
	public void setDestId(String destId) {
		this.destId = destId;
	}


	public String getSrcId() {
		return srcId;
	}

	public Method getMethod() {
		if (method == null && methodName == null) throw new RuntimeException("method field is null - it needs to be setup");
		if (method == null) method = getMethod(methodName);
		return method;
	}

	Method getMethod(String methodName) {
		Method method2 = methodNameCache != null ? methodNameCache.get(methodName) : null;
		if (method2 != null) return method2;
		return resolvedMethod(methodName);
	}
	Method resolvedMethod(String methodName) {
	
		try {
			String tempMethodName = methodName;
			if (tempMethodName.contains("public")) tempMethodName = tempMethodName.replaceAll("public", "");
			if (tempMethodName.contains("protected")) tempMethodName = tempMethodName.replaceAll("protected", "");
			if (tempMethodName.contains("abstract")) tempMethodName = tempMethodName.replaceAll("abstract", "");
			tempMethodName = tempMethodName.trim();
			int startName = tempMethodName.indexOf(" ");
			int endName = tempMethodName.indexOf("(");
			if (startName < 0 || endName < 0) {
				throw new RuntimeException("Failed to parse Method from:" + tempMethodName + " given:" + methodName);
			}
			String className = tempMethodName.substring(startName, endName).trim();
			className = className.substring(0, className.lastIndexOf("."));
			
			Class<?> classInstance = Class.forName(className);
			Method[] methods = classInstance.getMethods();
			for (Method method : methods) {
				if (method.toString().equals(methodName)) {
					if (methodNameCache != null) methodNameCache.put(methodName, method);
					return method;
				}
			}
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (Exception ex){
			throw new RuntimeException("Could not find method:" + methodName, ex);
		}

		throw new RuntimeException("Could not find method:" + methodName);
	}

	public Object getResult() {
		return result;
	}

	public String getMethodName() {
		return methodName;
	}

	public Object getArg(int i) {
		if (args == null) throw new RuntimeException("Cannot access index:" + i + " Args length==" + args);
		if (args.length <= i) throw new RuntimeException("Cannot access:" + i + " Args length==" + args.length);
		return args[i];
	}

	public String getPartitionName() {
		return partitionName;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	public void setMethod(Method method) {
		this.method = method;
		this.methodName = method.toString();
	}

	public Object[] getArgs() {
		return this.args;
	}

	public void setArgs(Object... args) {
		this.args = args;		
	}

	public void setClientURI(String uri) {
		this.clientURI = uri;
	}

	public void setSrcId(String guid) {
		this.srcId = guid;
	}

	public void setPartitionName(String paritionName) {
		this.partitionName = paritionName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	@Override
	public String toString() {
			return toString(false);
//		StringBuilder result = new StringBuilder(getClass().getSimpleName());
//		result.append(" Client:" + this.clientURI);
//		result.append(" Src:" + this.srcId);
//		result.append(" Dest:" + this.destId);
//		result.append(" method:" + this.methodName);
//		return result.toString();
	}

	public String toString(boolean nulifyParams) {
		StringBuilder stringBuilder = new StringBuilder();
		for (InvocationState state : InvocationState.values()) {
			state.parseObjectToString(this, stringBuilder, convertor, nulifyParams);
		}
		return stringBuilder.toString();
	}
	

	public void fromString(String payload) {
		String[] split = Arrays.split(InvocationState.DELIM, payload);
		this.currentState = InvocationState.HEADER;
		
		try {
			for (String item : split) {
				currentState = currentState.parseStringToObject(item, this, convertor);
			}
		} catch (Throwable t){
			if (t.getMessage().contains(InvocationState.VSCAPE_REMOTE_EXCEPTION)) {
				this.throwable = t;
			} else {
				this.throwable = new RuntimeException(String.format("Could not decode method[%s] invocation[%s] ex:%s", methodName, currentState.name(), t.getMessage()), t.getCause());
			}
		}
	}

	public boolean replaysOnAddressChange() {
		return method.getAnnotation(ReplayOnAddressChange.class) != null;
	}

	public String getDestId() {
		return destId;
	}

	public boolean isException() {
		return this.throwable != null;
	}

	public Throwable getException() {
		return throwable;
	}
	

	/**
	 * Override hashCode.
	 *
	 * @return the Objects hashcode.
	 */
	public int hashCode() {
		int hashCode = 1;
		hashCode = 31 * hashCode + (methodName == null ? 0 : methodName.hashCode());
		return hashCode;
	}

	/**
	 * Returns <code>true</code> if this <code>Invocation</code> is the same as the o argument.
	 *
	 * @return <code>true</code> if this <code>Invocation</code> is the same as the o argument.
	 */
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null) {
			return false;
		}
		if (o.getClass() != getClass()) {
			return false;
		}
		Invocation castedObj = (Invocation) o;
		return 
			this.methodName == null ? castedObj.methodName == null : this.methodName.equals(castedObj.methodName);
	}

	public void setArg(int i, Object arg) {
		args[i] = arg;
	}

	public void resetArgsToNull() {
		for (int i = 0; i < getArgs().length; i++) {
			setArg(i, null);
		}
	}
	
	public void read(Kryo kryo, Input input) {
		this.clientURI = kryo.readObject(input, String.class);
		this.partitionName = kryo.readObject(input, String.class);
		this.srcId = kryo.readObject(input, String.class);
		this.destId = kryo.readObject(input, String.class);
		this.methodName = kryo.readObject(input, String.class);
		
		int argsCount = kryo.readObject(input, int.class);
		if (argsCount != -1) {
			this.args = new Object[argsCount];
			for (int i = 0; i < argsCount; i++) {
				this.args[i] = kryo.readClassAndObject(input);
			}
		}
		
		this.result = kryo.readClassAndObject(input);
		
		
	}
	public void write(Kryo kryo, Output output) {
		kryo.writeObject(output, this.clientURI);
		kryo.writeObject(output, this.partitionName);
		kryo.writeObject(output, this.srcId);
		kryo.writeObject(output, this.destId);
		kryo.writeObject(output, this.methodName);
		
		if (this.args == null) {
			kryo.writeObject(output, -1);
		} else {
			kryo.writeObject(output, this.args.length);
			for (int i = 0; i < args.length; i++) {
				kryo.writeClassAndObject(output, args[i]);
			}
		}
		
		kryo.writeClassAndObject(output, this.result);
		
		
	}

    /**
     * Interceptor hooks
     * @param impl
     */
    public void incoming(Object impl) {
        if (method.getAnnotation(Interceptor.class) != null) {
            Interceptor annotation = method.getAnnotation(Interceptor.class);
            String interceptorClass = annotation.clazz();
            if (interceptorClass.equals("none")) return;

            try {
                Class<?> inter = Class.forName(interceptorClass);
                // Store on a ThreadLocal Context
                InvocationInterceptor interceptorImpl = (InvocationInterceptor) inter.getConstructors()[0].newInstance();
                this.args = interceptorImpl.incoming(impl, method, this.args, null);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public void outgoing() {
        if (method.getAnnotation(Interceptor.class) != null) {
            Interceptor annotation = method.getAnnotation(Interceptor.class);
            String interceptorClass = annotation.clazz();
            if (interceptorClass.equals("none")) return;

            try {
                Class<?> inter = Class.forName(interceptorClass);
                // Store on a ThreadLocal Context
                InvocationInterceptor interceptorImpl = (InvocationInterceptor) inter.getConstructors()[0].newInstance();
                this.args = interceptorImpl.outgoing(null, method, this.args, null);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    // We do this to try and override the sent IP with one found on the incoming network connection
    public void setSenderIp(String senderIp) {
        try {
            String ipOnly = senderIp;
            if (senderIp.contains("_")) {
                ipOnly = senderIp.substring(0, senderIp.indexOf("_"));
            }
            URI uri = new URI(this.clientURI);
            String existingHost = uri.getHost();
            if (!existingHost.equals(ipOnly)) {
                this.clientURI.replace(existingHost, ipOnly);
            }

        } catch (URISyntaxException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
