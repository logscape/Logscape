package com.liquidlabs.transport.proxy;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.liquidlabs.transport.proxy.clientHandlers.*;
import javolution.util.FastMap;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.protocol.UDP;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;
import com.liquidlabs.transport.serialization.Convertor;

/**
 * Dynamic proxy that used to invoke on an endpoint
 *
 */
public class ProxyClient<E> implements InvocationHandler, ProxyCaller, LifeCycle, Externalizable, KryoSerializable {
    private static final long serialVersionUID = 1L;

    public static final Logger LOGGER = Logger.getLogger(ProxyClient.class);

    private static final int logMaxMsgSize = Integer.getInteger("nw.log.max", 1024 * 1024);
    public static String DELIM = ";";
    static AtomicInteger count = new AtomicInteger();
    static String [] PROT_START = {"stcp://", "udp://", "tcp://", "rudp://"};
    int invocationTimeoutSecs = TransportProperties.getInvocationTimeoutSecs();

    private String id;
    private String listenerId;
    private Class<E> interfaceClass;
    private State state = LifeCycle.State.STARTED;
    private int errors;

    private AddressHandler addressHandler;

    private transient Set<Invocation> replayableInvocations = new CopyOnWriteArraySet<Invocation>();
    private transient Convertor convertor;
    private transient PeerHandler peerHandler;
    private transient ExecutorService executor;
    private transient ProxyFactory proxyFactory;
    private transient URI clientAddress;
    private transient boolean disabled = false;
    private transient String lastSycTime = "";

    private transient long lastUsed = System.currentTimeMillis();

    private int proxyHash;

    public ProxyClient(){
    }

    public ProxyClient(Class<E> interfaceClass, URI clientURI, String[] endPoints, String listenerId, PeerHandler peerHandler, Convertor convertor, AddressHandler addressHandler, ExecutorService executor, ProxyFactory proxyFactory) {
        this.interfaceClass = interfaceClass;
        this.proxyFactory = proxyFactory;
        this.listenerId = listenerId;
        this.clientAddress = clientURI;
        this.addressHandler = addressHandler;
        this.executor = executor;

        // should be using IPAddresses
        this.addressHandler.addEndPoints(endPoints);

        if (this.addressHandler.getEndPointURIs().size() == 0) {
            LOGGER.info(String.format("/%d Listener[%s] Given Invalid Set of EPs%s - Proxy is lazy and will rely on updateListener", this.hashCode(), this.listenerId, this.addressHandler.getEndPointURIs().toString()));
        }

        this.peerHandler = peerHandler;
        this.convertor = convertor;
        this.id = listenerId;
        this.proxyHash = this.hashCode();

    }
    public String getId() {
        return id;
    }

    public void start() {

        inject(new HashbleAddressByParamImpl());
        inject(new CacheableImpl());
        inject(new RoundRobinHandler());
        inject(new BroadcastImpl());
        inject(new FailFastAndDisableImpl());
        inject(new FailFastOnceImpl());
        inject(new DecoupledImpl());

        state = LifeCycle.State.STARTED;
    }
    public void stop(){
        LOGGER.debug("Stopping:" + toString());
        state = LifeCycle.State.STOPPED;
        if (replayableInvocations != null) replayableInvocations.clear();
    }
    public boolean isStopped() {
        return state.equals(LifeCycle.State.STOPPED);
    }

    private static int getAddressIndex(String value) {
        for (String proto : PROT_START) {
            int index = value.indexOf(proto);
            if (index > 0) {
                return index;
            }
        }
        throw new RuntimeException("Could not parse Address from proxyId " + value);
    }


    public static String getURI(String value){
        return value.substring(getAddressIndex(value), value.length());
    }

    @SuppressWarnings("unchecked")
    public E getClient(){

        try {
            return (E) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                    new Class[] { interfaceClass },
                    this);
        } catch (Throwable t) {
            LOGGER.error("Failed to get Proxy for:" + interfaceClass, t);
            throw new RuntimeException(t);
        }
    }
    /**
     * Dynamic proxied invocation
     */
    private boolean hasWarned = false;

    Map<String, ClientHandler> handlers = new FastMap<String, ClientHandler>().shared();
    private void inject(ClientHandler clientHandler) {
        try {
        Method method = clientHandler.getClass().getDeclaredMethod("sample");
        handlers.put(method.getDeclaredAnnotations()[0].annotationType().toString(), clientHandler);
        } catch (Throwable t) {
            LOGGER.warn("Failed to bind handled:" + clientHandler, t);
        }
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        if (state.equals(LifeCycle.State.STOPPED)) {
            if (!hasWarned) {
                String string = this.hashCode() + " Attempt to used STOPPED client:" + getId() + " disabled:" + this.disabled;
                LOGGER.warn(new RuntimeException(string));
            }
            hasWarned = true;
            return null;
        }
        if (this.disabled == true) {
            if (!hasWarned) {
                String string = this.hashCode() + " Attempt to used DISABLED client:" + getId() + " disabled:" + this.disabled + " Errors:" + errors;
                LOGGER.warn(new RuntimeException(string));
            }
            hasWarned = true;
            return null;
        }

        if (method.equals(Object.class.getMethod("hashCode"))) {
            return hashCode();
        }
        if (method.equals(Object.class.getMethod("toString"))) {
            return this.toString();
        }
        if (method.getName().equals("getId") &&  args == null || (args != null && args.length == 0)) {
            return this.listenerId;
        }
        if (method.equals(Object.class.getMethod("equals", Object.class))) {
            return equals(args[0]);
        }

        lastUsed = System.currentTimeMillis();



        if (args == null)
            args = new Object[0];

        if (!addressHandler.isEndPointAvailable()) {
            LOGGER.error("ERROR:" + addressHandler);
            addressHandler.validateEndPoints();
            if (!addressHandler.isEndPointAvailable()) throw new RuntimeException("Listener[" + listenerId + "] Could not recover EPS");
        }
        Annotation[] annotations = method.getAnnotations();
        for (Annotation annotation : annotations) {
            ClientHandler clientHandler = handlers.get(annotation.annotationType().toString());
            if (clientHandler != null) return clientHandler.invoke(this, addressHandler, method, args);
        }

        // standard execution
        return sendWithRetry(method, args);
    }

    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof ProxyClient)) return false;

        if (other instanceof ProxyClient) {
            return ((ProxyClient)other).listenerId.equals(listenerId);
        }

        if (other instanceof Proxy) {
            InvocationHandler invocationHandler = Proxy.getInvocationHandler(other);
            return invocationHandler instanceof ProxyClient && ((ProxyClient)invocationHandler).listenerId.equals(listenerId);
        }
        return false;
    }

    public int hashCode() {
        return 31 + listenerId.hashCode();
    }



    public void setErrors(int errors) {
        this.errors = errors;
    }


    public boolean isDisabled() {
        return disabled;
    }
    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }



    public Object sendWithRetry(Method method, Object[] args) throws IOException, InterruptedException {

        replayIfRequired();
        boolean verbose = LOGGER.isDebugEnabled();

        List<URI> endPointURIs = addressHandler.getEndPointURIs();

        if (endPointURIs.isEmpty()) {
            LOGGER.error("No EPS for:" + method.getDeclaringClass() + "." + method.getName());
        }
        for (URI uri : endPointURIs) {
            try {
                return clientExecute(method, uri, args, verbose, -1);
            } catch (RetryInvocationException ex) {
                LOGGER.warn("RetryException:" + uri + " ex:" + ex.toString(), ex);
                addressHandler.registerFailure(uri);
            } catch (Exception ex) {
                LOGGER.error(" SEND Exception:" + uri + " ex:" + ex.toString(), ex);
                addressHandler.registerFailure(uri);
            }

        }

        LOGGER.error("Failing to make call:" + method.getDeclaringClass() + "." + method.getName() + " Errors:" + errors++);
        if (errors > Integer.getInteger("proxy.max.fails", 1000)) {
            disabled = true;
        }
//		addressHandler.recoverFromNoAddresses();
        throw new RuntimeException("Listener[" + listenerId + "] NoREPLY EPS:" + addressHandler.toString());
    }

    private void replayIfRequired() {
        if (addressHandler.isReplayRequired())
            try {
                replay("SendRequest");
            } catch (RetryInvocationException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }

    private String makeGuid(Method method) {
        return method.getName() + "-" + UID.getUUID() + "-"+ count.getAndIncrement();
    }


    public Object clientExecute(Method method, URI endPoint, Object[] args, boolean verbose, int ttlOverride) throws IOException, RetryInvocationException, InterruptedException{
        Invocation invocation = new Invocation(method, convertor);
        String guidKey = listenerId != null ? listenerId + makeGuid(method) : makeGuid(method);

        invocation.set(guidKey, listenerId, clientAddress.toString(), listenerId, null, makeArgumentsRemotable(args));

        Object result = sendInvocation(method, endPoint, peerHandler, invocation, guidKey, verbose, ttlOverride);

        if (invocation.replaysOnAddressChange()) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Adding replayable invocation " + invocation.methodName);
            replayableInvocations.add(invocation);
        }
        return result;
    }

    int hugeCount = 0;
    private Object sendInvocation(Method method, URI endPoint, PeerHandler peerHandler, Invocation invocation, String guidKey, boolean verbose, int ttlOverride) throws IOException, RetryInvocationException, InterruptedException {
        boolean isReplyExpected = !method.getReturnType().equals(void.class) || method.getExceptionTypes().length > 0;

        if (verbose) LOGGER.info(String.format("SendInvocation [%s==>%s] inv:%s id:%s", clientAddress, addressHandler.getEndPointURI(), invocation.getMethod(), guidKey));

        Object result = null;
        String protocol = "stcp";
        if (method.getAnnotation(UDP.class) != null) protocol = "udp";

        invocation.outgoing();


        byte[] serialize = Convertor.serializeAndCompress(invocation);

        if (serialize.length > logMaxMsgSize) {
            synchronized (this) {
                if (hugeCount++ < 10 || hugeCount % 100 == 0) LOGGER.warn(">>>> Message is HUGE:" + method.toString() + " sendTo:" + endPoint + "  Size(kb):" + serialize.length/1024);
            }
        }
        boolean remoteOnly = method.getAnnotation(RemoteOnly.class) != null;

        byte[] bytes = peerHandler.send(protocol, endPoint, serialize, Type.REQUEST, isReplyExpected, ttlOverride != -1 ? ttlOverride : invocationTimeoutSecs, listenerId, !remoteOnly);
        try {
            if (isReplyExpected) {
                if (invocation.getMethod().getReturnType().equals(byte[].class)) {
                    result = bytes;
                    if (isException(bytes)) {
                        LOGGER.error("Got EX from Bytes[]:" + bytes.length);
                        String string = new String(bytes);
                        // if the remote EP didnt find the received, rotate addresses for another one and retry
                        if (string.contains("ReceiverIdNotFound")) throw new RetryInvocationException(string);
                        throw new RuntimeException(string);
                    }
                } else {
                    result = Convertor.deserializeAndDecompress(bytes);
                }
                // Pass back remote exceptions as String
                if (result instanceof String) {
                    String ss = (String) result;
                    if (ss.contains(PeerHandler.RECEIVER_ID_NOT_FOUND)) throw new RetryInvocationException(ss);
                    if (ss.startsWith("VScapeRemoteException")) throw new RuntimeException(ss);
                }
                if (result != null && Throwable.class.isAssignableFrom(result.getClass())) {
                    Throwable givenException = (Throwable) result;
                    // Detect when PeerHandler didnt have a received and force a rety of the invocation
                    if (givenException.getMessage().contains(PeerHandler.RECEIVER_ID_NOT_FOUND)) throw new RetryInvocationException(givenException.getMessage());
                    throw new RuntimeException((Throwable) result);
                }
            }

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
    private boolean isException(byte[] bytes) {
        return bytes[0] == (byte) 0x01 &&
                bytes[1] == (byte) 0x02 &&
                bytes[2] == (byte) 0x01 &&
                bytes[3] == (byte) 0x03;
    }
    private Object[] makeArgumentsRemotable(Object[] args) {
        Object[] result = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg instanceof Remotable) {
                Remotable remotableProxy = (Remotable) proxyFactory.makeRemoteable((Remotable) arg);
                result[i] = remotableProxy;
            } else {
                result[i] = arg;
            }
        }
        return result;
    }

    int recurseStack = 0;

    private transient String lastRefresh = "";
    public void refreshAddresses(String addressToAddString) {

        try {
            if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("_HA_ Refresh Address hash[%d] id:%s/%s address:%s current:%s", this.hashCode(), this.listenerId, interfaceClass.toString(), addressToAddString, addressHandler));

            URI currentEndPoint = this.addressHandler.getEndPointURISafe();
            this.addressHandler.addEndPoints(addressToAddString.split(","));
            this.lastRefresh =  DateUtil.longDTFormatter.print(this.lastUsed);

            if (addressHandler.isReplayRequired()) {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("_HA_ " + toString() + " REFR_SYNC Changing endpoint for " + listenerId + " from: " + currentEndPoint + " to:" + addressHandler.getEndPointURISafe() + " list:" + addressHandler.getEndPointURIs());
                replay("RefreshAddresses");
            } else {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("_HA_ " + toString() + " NO REPLAY:" + this.addressHandler);
            }
            if (recurseStack++ > 50) {
                throw new InterruptedException("stack-blow-unwind");
            }

        } catch (RetryInvocationException e) {
//			addressHandler.registerFailure();
            LOGGER.warn("Failed to update address:" + this.id + " addr:" + addressToAddString);
        } catch (InterruptedException e) {
        } catch (Exception e) {
            LOGGER.error("Failed to update address:" + this.id + " addr:" + addressToAddString);
        }
    }
    public void removeAddress(String address) {
        LOGGER.info(String.format("_HA_ Remove id:%s address:%s", this.listenerId, address));

        try {
            this.addressHandler.remove(address);
            if (addressHandler.isReplayRequired()) replay("RemoveAddress");
        } catch (Throwable t) {
            LOGGER.warn(this.toString() + " Failed to invoke removeAddress event:" + address);
            LOGGER.warn("REM Removed address["+address+"] ==> ePoint:" + addressHandler.getEndPointURISafe() + " eps" + addressHandler.getEndPointURIs());
            LOGGER.warn("REM After:" + addressHandler);
            LOGGER.info("REM Final State:" + this.toString());
        }
    }

    private void replay(String context) throws RetryInvocationException, InterruptedException {
        addressHandler.resetReplayFlag();
        if (state.equals(State.STOPPED)) return;
        if (replayableInvocations == null) return;
        for (Invocation invocation : replayableInvocations) {
            try {
                Collection<URI> endPoints = addressHandler.getEndPointURIs();
                for (URI endPoint : endPoints) {
                    try {
                        LOGGER.info(String.format("_HA_ (%s) Replaying [%s==>%s] inv:%s", context, clientAddress, endPoint, invocation.getMethod()));
                        sendInvocation(invocation.getMethod(),endPoint, peerHandler, invocation, invocation.srcId, true, -1);
                    } catch (Throwable t) {
                    }
                }
            } catch (Exception e) {
                LOGGER.warn(clientAddress + " Failed to replay invocation " + invocation, e);
            }
        }
    }

    public URI getClientAddress() {
        return clientAddress;
    }
    public String toString(){
        String string = this.proxyFactory != null ? this.proxyFactory.toString() : " NullPF";
        String timeInfo = "LastUsed:" + DateUtil.longDTFormatter.print(this.lastUsed) + " LastSync:" + this.lastSycTime + " LastRefresh:" + this.lastRefresh;
        return String.format("%s/%d id:%s h:%d fact:%s if:%s %s %s %s", this.getClass().getSimpleName(), this.hashCode(), this.listenerId, this.proxyHash, string, getInterfaceNames(), this.addressHandler, state.name(), timeInfo);
    }
    public String toSimpleString() {
        return this.id + "/" + this.hashCode() + " " + getInterfaceNames();
    }

    String getInterfaceNames() {
        String result = this.interfaceClass.getSimpleName();
        Class<?>[] interfaces = interfaceClass.getInterfaces();
        if (interfaces != null) {
            for (Class<?> class1 : interfaces) {
                result += "," + class1.getSimpleName();
            }
        }

        return result;
    }
    public void setInvocationTimeout(int seconds) {
        this.invocationTimeoutSecs = seconds;
    }

    public void syncEndpoints(String[] addresses, String[] replicationLocations) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("_HA_ SyncEndpoints id:%s hash:%d address:%s", this.listenerId, this.hashCode(), Arrays.toString(addresses)));
        addressHandler.addEndPoints(addresses);
        this.lastSycTime =  DateUtil.longDTFormatter.print(System.currentTimeMillis());
        if (addressHandler.isReplayRequired()) {
            LOGGER.info("_HA_ " + toString() + " SYNC_REPLAY Reason:" + addressHandler.replayReason() + " Changing endpoint for " + listenerId + " to:" + " list:" + addressHandler.getEndPointURIs());
            try {
                replay("SyncAddresses");
            } catch (RetryInvocationException e) {
            } catch (InterruptedException e) {
            }
        }
    }

    public void attach(Convertor convertor, PeerHandler peerHandler2, ExecutorService executor2, ScheduledExecutorService scheduler2, URI clientAddress) {
        this.convertor = convertor;
        this.peerHandler = peerHandler2;
        this.executor = executor2;
        this.clientAddress = clientAddress;
        this.lastUsed = System.currentTimeMillis();
        start();
    }


    public void setId(String id2) {
        this.id = id2;
    }

    public Long lastUsed() {
        return lastUsed;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.id = in.readUTF();
        this.listenerId = in.readUTF();
        String iface = in.readUTF();
        this.interfaceClass = (Class<E>) this.getClass().forName(iface);
        this.addressHandler = (AddressHandler) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(listenerId);
        out.writeUTF(interfaceClass.getName());
        out.writeObject(addressHandler);
    }
    public void read(Kryo kryo, Input input) {
        id = kryo.readObject(input, String.class);
        listenerId = kryo.readObject(input, String.class);
        String iface = kryo.readObject(input, String.class);
        try {
            interfaceClass = (Class<E>) this.getClass().forName(iface);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.addressHandler = (AddressHandler) kryo.readClassAndObject(input);

    }
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, id);
        kryo.writeObject(output, listenerId);
        kryo.writeObject(output, interfaceClass.getName());
        kryo.writeClassAndObject(output, addressHandler);
    }

}