package com.liquidlabs.transport.proxy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import javolution.util.FastMap;

import org.apache.log4j.Logger;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.serialization.Convertor;

public class PeerHandler implements Receiver, Sender {

    public static final String RECEIVER_ID_NOT_FOUND = "ReceiverIdNotFound";

    private static final String URI_ID_EQUALS = "/id=";

    private static final transient Logger LOGGER = Logger.getLogger(PeerHandler.class);
    private boolean allowColocation = Boolean.getBoolean("allow.coloc");

    // notification/streaming events
    transient FastMap<String, ContinuousEventListener> continuousEventListeners = new FastMap<String, ContinuousEventListener>();

    // incoming invocation from peer
    transient FastMap<String, MethodInvoker> methodInvokers = new FastMap<String, MethodInvoker>();
    transient private Sender sender;

    transient private Convertor convertor;
    transient ExecutorService executor;
    transient ScheduledExecutorService scheduler;


    FastMap<String, AtomicInteger> badInvokerIds = new FastMap<String, AtomicInteger>();

    private ThreadPoolExecutor oneWayExecutor;

    private final String address;

    private ClientNotifier clientNotifier;
    private String serviceName;

    public String toString() {
        return super.toString() + " :" + address;
    }
    public Sender getSender() {
        return sender;
    }
    public PeerHandler(Convertor convertor, ScheduledExecutorService scheduler, ExecutorService executor, String address, String serviceName) {
        this.serviceName = serviceName;
        LOGGER.info("CREATED:" + address + ":" + serviceName);//, new RuntimeException(serviceName));
        this.convertor = convertor;
        this.scheduler = scheduler;
        this.executor = executor;
        this.address = address;
        methodInvokers.shared();
        continuousEventListeners.shared();
        badInvokerIds.shared();
        //oneWayExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool(new NamingThreadFactory("long-running" + serviceName + "-one-way"));
        oneWayExecutor = (ThreadPoolExecutor) com.liquidlabs.common.concurrent.ExecutorService.newSizedThreadPool(10, Integer.getInteger("long.run.max", 100), new NamingThreadFactory("long-running" + serviceName + "-one-way"));
    }

    public void setSender(Sender sender) {
        this.sender = sender;
    }

    public URI getAddress() {
        return sender.getAddress();
    }
    public boolean isForMe(Object payload) {
        return payload instanceof Invocation;
    }

    /**
     * Received from the socket, decode and dispatch to correct handler
     */
    public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
        Invocation invocation = null;
        try {
            invocation = (Invocation) Convertor.deserializeAndDecompress(payload);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return receive(invocation, remoteAddress, remoteHostname);
    }
    public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
        final String payloadString = null;
        Invocation invocation = null;
        Object reply = null;
        Throwable ex = null;
        boolean responseRequired = false;
        try {

            invocation = (Invocation) payload;
            fixRemotables(invocation);
            invocation.setSenderIp(remoteAddress);

            String destinationId = invocation.getDestId();
            if (LOGGER.isDebugEnabled())  {
                LOGGER.debug(String.format("%s *** Received:%s src:%s dest:%s", sender.getAddress(), invocation.getMethodName(), invocation.getClientURI(), destinationId));
            }
//			System.out.println(String.format("%s *********** Received:%s src:%s dest:%s", sender.getAddress(), invocation.getMethodName(), invocation.getClientURI(), invocation.getDestId()));

            final String srcId = invocation.getSrcId();
            responseRequired = shouldSendReponse(invocation);

            if (destinationId.contains(URI_ID_EQUALS))
                destinationId = srcId.substring(srcId.lastIndexOf(URI_ID_EQUALS) + 1, srcId.length());

            // STREAMING
            if (continuousEventListeners.containsKey(destinationId)) {
                handleContinuousEventListener(invocation, srcId, destinationId);

                // SERVER
            } else if (responseRequired && this.methodInvokers.containsKey(destinationId)) {
                reply = handleRemoteMethodInvocation(invocation, srcId, destinationId, payloadString);
            } else if (!responseRequired && this.methodInvokers.containsKey(destinationId)) {
                final Invocation mine = invocation;
                final String myDestId = destinationId;
                if (oneWayExecutor.getActiveCount() > 1000) {
                    LOGGER.warn("Active Tasks > 1000: Inv" + invocation.getMethodName());
                }
                if (oneWayExecutor.getActiveCount() > 10000) {
                    LOGGER.error("Active Tasks > 10,000: Inv" + invocation.getMethodName());
                    LOGGER.error("Too many active Tasks > 10000: Forcing System.Reboot");
                    System.exit(10);

                }
                oneWayExecutor.execute(new Runnable() {
                    public void run() {
                        try {
                            handleRemoteMethodInvocation(mine, srcId, myDestId, payloadString);
                        } catch (Throwable e) {
                            LOGGER.info("Task Error:"+ myDestId, e);
                        }
                    }
                });
            } else {

                //System.err.println("Bad call:" + destId + " " + payloadString);

                // can happen when an invocation has been aborted/timed out, the system is overloaded and the synclistener future didnt fire and we have
                // received a late reply - since the sync listener has been removed it will get to this part of the code
                if (isThisClient(new URI(invocation.getClientURI()))) return null;

                boolean methodReturningData = isMethodReturningData(invocation);

                if (methodReturningData) {
                    if (!badInvokerIds.containsKey(destinationId)) {
                        badInvokerIds.put(destinationId,new AtomicInteger());
                        LOGGER.warn(String.format("%s This:%s:%s Unknown Rcvr[%s].%s / client:%s ",
                                sender.getAddress(), address, serviceName, destinationId, invocation.getMethod(), invocation.getClientURI(), invocation.getMethodName(), destinationId));
                        LOGGER.info(" Failed to pass on event, srcId[" + srcId + "] destId[" + destinationId + "]");
                        errorLogHandlers();
                    } else {
                        AtomicInteger badCount = badInvokerIds.get(destinationId);
                        if (badCount.get() > 1000) {
                            return null;
                        } else {
                            badCount.incrementAndGet();
                        }
                        LOGGER.warn(this.getAddress() + " Bad invocation suppressed msg:" + destinationId + " BadInvocations:" + badCount);
                    }

                    LOGGER.warn(sender.getAddress() + "**** SENDING ERROR BACK TO CALLER ****** :" + invocation.getClientURI() + "." + invocation.getMethodName());
                    throw new RuntimeException(String.format("%s %s:%s = available%s", sender.getAddress().toString(), RECEIVER_ID_NOT_FOUND, destinationId, this.methodInvokers.keySet().toString().replaceAll(", ", ",\n\t")));
                }
            }
        } catch (NullPointerException t) {
            LOGGER.warn(this.address + " NullPtr", t);
            reply = t;
            ex = t;

        } catch (Throwable t) {
            LOGGER.warn(this.address + " ex:" + t.toString());
            reply = t;
            ex = t;
        } finally {
            if (responseRequired) {
                if (ex != null) {
                    reply = getRemoteException(invocation, ex);
                }
                try {
                    return sendResonseToCaller(invocation, reply);
                } catch (Exception e) {
                    LOGGER.error("Client:" + invocation.getClientURI() + " method:" + invocation.getMethodName(), e);
                }
            } else if (ex != null) {
                LOGGER.warn(this.address + " Client:" + invocation, ex);
                throw new RuntimeException(ex);
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void fixRemotables(Invocation invocation) {
        Object[] args = invocation.getArgs();
        if (args == null) return;
        for (Object object : args) {
            if (object instanceof Remotable) {
                try {
                    ProxyClient proxyClient = (ProxyClient) Proxy.getInvocationHandler(object);
                    proxyClient.attach(convertor, this, executor, scheduler, getAddress());
                    if (clientNotifier != null) clientNotifier.notify(proxyClient);
                } catch (Throwable t) {
                    LOGGER.warn(t);
                }
            }
        }
    }

    private Object handleRemoteMethodInvocation(Invocation invocation, String srcId, String destId, String payloadString) throws Throwable {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format(">>> %s from:%s method:%s", srcId, invocation.getClientURI(), invocation.getMethodName()));
        }


        MethodInvoker invocationTarget = this.methodInvokers.get(destId);
        Object result = null;
        invocation.incoming(invocationTarget.instance);
        try {
            if (invocation.getException() != null) throw invocation.getException();
            result = invocation.getMethod().invoke(invocationTarget.instance, invocation.getArgs());
        } catch (Throwable t){

            if (invocationTarget == null) invocationTarget = new MethodInvoker("NULL", destId);;
            if (t instanceof InvocationTargetException) {
                InvocationTargetException ive = (InvocationTargetException) t;
                Throwable cause = ive.getCause();
                if (cause != null) {
                    /**
                     * Things have gone horribly wrong!
                     */
                    if (cause.toString().contains("PermGen")) {
                        LOGGER.warn("Bounce - Cause PermGen:" + cause);
                        System.exit(10);
                    }
                    if (cause.toString().contains("OutOfMemory")) {
                        LOGGER.warn("Bounce - Cause OOM:" + cause);
                        System.exit(10);
                    }

                    // indicates something really wrong and not handled properly - so in this case log it
                    if (cause.toString().contains("NullPointer") || cause.toString().contains("Bounds")) {
                        LOGGER.warn("", ive);
                    }
                }
            } else {
                LOGGER.warn(t);
            }
            Object[] args = invocation.getArgs();
            String argsLength = args == null ? "NULL Args" : "ArgLength:" + args.length;

            int pos = 0;

            LOGGER.warn(String.format("Target) - key:%s instance:%s class:%s", invocation.getDestId(), invocationTarget.getInstance() , invocationTarget.getInstance().getClass()));
            Class<?>[] params = invocation.getMethod().getParameterTypes();
            for (Object object : args) {
                if (object == null) object = "null";
                String str = object.toString();
                if (str.length() > 256) str = str.substring(0, 255);
                LOGGER.warn(String.format("ARG %d) arg[%s]\tclass:[%s] \texpects[%s]", pos, str, object.getClass(), params[pos]));
                pos++;
            }
//			String invocationXML = new XStream().toXML(invocation);
            //LOGGER.info(invocationXML);

            if (invocationTarget != null && invocationTarget.instance != null) {
                LOGGER.warn("Object:" + invocationTarget.instance.getClass() + " id:" + destId + " " + t.getMessage());
                LOGGER.warn("TargetFailed Instance=>" + invocationTarget.instance, t);
            }

            LOGGER.warn("Ex:" + t.toString() + " Method:" + invocation.getMethod().toString() + " ArgumentCount:" + argsLength);

            LOGGER.warn(t.getMessage() + "- Passing back RemoteException:" + invocation.getMethod().toString() + " ArgCount:" + argsLength);
            String format = String.format("%s[%s]=>target[%s].method[%s] IllegalArgument[%s] ArgCount[%s] Cause[%s]", InvocationState.VSCAPE_REMOTE_EXCEPTION, sender.getAddress(), destId, invocation.getMethod(), t.getMessage(), argsLength, t.getCause());
            RuntimeException t2 = new RuntimeException(format, t);
            throw t2;
        }
        if (LOGGER.isDebugEnabled())  {
            String rr = result != null ? result.toString() : "";
            if (rr.length() > 100) rr = rr.substring(0, 99);
            LOGGER.debug("Returning:" + rr);
        }
        return result;
    }

    boolean isThisClient(URI other) {
        URI senderAddress = sender.getAddress();
        return senderAddress.getHost().equals(other.getHost()) && senderAddress.getPort() == other.getPort();
    }

    boolean shouldSendReponse(Invocation invocation) {
        String clientURI = invocation.getClientURI();
        if (clientURI.contains(URI_ID_EQUALS)) {
            clientURI = clientURI.substring(0, clientURI.indexOf(URI_ID_EQUALS));
        }
        boolean methodReturnsStuff = isMethodReturningData(invocation);
        boolean isOneAndTheSame = false;// clientURI.equals(sender.getAddress().toString());
        if (allowColocation) return true;
        if (isOneAndTheSame && methodReturnsStuff) {
            LOGGER.warn(String.format("Cannot return data isOneAndSame:%b client:%s sender:%s method:%s", isOneAndTheSame, clientURI, sender.getAddress(), invocation.getMethod()));
        }
        return !isOneAndTheSame && methodReturnsStuff;
    }

    private boolean isMethodReturningData(Invocation invocation) {
        return !invocation.getMethod().getReturnType().equals(void.class) || invocation.getMethod().getExceptionTypes().length > 0;
    }

    RuntimeException getRemoteException(Invocation invocation, Throwable t) {
        String cause = ExceptionUtil.stringFromStack(t, -1);
        return new RuntimeException(String.format("%s[%s] method[%s] exType[%s] cause[%s] \nstack[%s]", InvocationState.VSCAPE_REMOTE_EXCEPTION, sender.getAddress(), invocation.getMethod(), t.getClass(), t.getMessage(), cause));
    }

    private byte[] sendResonseToCaller(Invocation invocation, Object result) throws InterruptedException, RetryInvocationException, URISyntaxException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s Sending Reply =>%s method:%s id:%s",sender.getAddress(), invocation.getClientURI(), invocation.getMethod(), invocation.getDestId()));
            String string = invocation.toString();
            if (string.length() > 256) string = string.substring(0, 255);
            LOGGER.debug(String.format("%s Sending Reply => bytes[%d] >%s", sender.getAddress(), string.getBytes().length,  string));
        }

        try {
            if (invocation.getMethod().getReturnType().equals(byte[].class)) {
                if ((Throwable.class.isAssignableFrom(result.getClass()))) {
                    Throwable t = (Throwable) result;
                    String m = t.getMessage();
                    byte[] bytes = new byte[m.getBytes().length+4];

                    // magic number
                    bytes[0] = (byte) 0x01;
                    bytes[1] = (byte) 0x02;
                    bytes[2] = (byte) 0x01;
                    bytes[3] = (byte) 0x03;
                    System.arraycopy(m.getBytes(), 0, bytes, 4, m.getBytes().length);
                    return bytes;
                } else {
                    return (byte[]) result;
                }
            }
        } catch (Throwable t) {
            LOGGER.error("Failed to convert to byte[]: given:" + result);
        }

        try {
            return convertor.serializeAndCompress(result);
        } catch (IOException e) {
            throw new RuntimeException("SendReplyFailed",e);
        }
    }

    private void handleContinuousEventListener(Invocation invocation,
                                               String srcId, String destId) throws IllegalAccessException,
            InvocationTargetException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(">>>" + srcId + " CONTINUOUSResult:" + invocation.getResult());
        }
        ContinuousEventListener continuousEventListener = continuousEventListeners.get(destId);
        invocation.getMethod().invoke(continuousEventListener.getTarget(), invocation.getArgs());
    }

    private void errorLogHandlers() {
        LOGGER.warn(getAddress() +" methodInvokerIds:" + methodInvokers.keySet());
        LOGGER.warn(getAddress() +" notifyInvokerIds:" + continuousEventListeners.keySet());
    }

    public byte[] send(String protocol, URI uri, byte[] payload, Type type, boolean isReplyExpected, long timeoutSeconds, String listenerId, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException {
        // can we do local-loopback in here? - i.e. check sender address with this uri?

        URI address = sender.getAddress();
        if (allowlocalRoute && uri.getHost().equals(address.getHost()) && uri.getPort() == address.getPort() && uri.getAuthority().equals(address.getAuthority())) {
            return this.receive(payload, "127.0.0.1", uri.getHost());
        }
        return sender.send(protocol, uri, payload, type, isReplyExpected, timeoutSeconds, listenerId, allowlocalRoute);
    }


    public void addMethodReceiver(String guid, Object target) {
//		System.out.println(">>> methodRec:" + guid);
        if (LOGGER.isDebugEnabled()) LOGGER.debug(sender.getAddress() + " Adding: MethodReceiver:" + guid);
        this.methodInvokers.put(guid, new MethodInvoker(target, guid + sender.getAddress().toString()));
    }

    public void addContinuousEventListener(String clientId, ContinuousEventListener continuousEventListener) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug(sender.getAddress() + " Adding: ContinuousListener:" + clientId);
        this.continuousEventListeners.put(clientId, continuousEventListener);
    }

    public void start() {
    }

    public void stop() {
        this.methodInvokers.clear();
        this.continuousEventListeners.clear();
    }

    /**
     *
     * @param listenerId
     * @return true when success false otherwise
     */
    public boolean removeMethodReceiver(String listenerId) {
        if (listenerId == null) return false;
        LOGGER.debug(sender.getAddress() + " Removing: MethodReceiver:" + listenerId);
        MethodInvoker methodInvoker = methodInvokers.remove(listenerId);
        if (methodInvoker != null) {
            methodInvoker.stop();
            return true;
        }
        return false;
    }

    public void dumpConnections() {
        sender.dumpStats();
    }

    public void dumpStats() {
    }
    public List<String> printEndPoints() {
        ArrayList<String> results = new ArrayList<String>();
        for (String guid : this.methodInvokers.keySet()) {
            MethodInvoker methodInvoker = this.methodInvokers.get(guid);
            results.add("Service:" + guid + " " + methodInvoker.toString());
        }
        for (String string : this.continuousEventListeners.keySet()) {
            results.add("Continuous:" + string);

        }
        return results;
    }
    public void setClientNotifier(ClientNotifier clientNotifier) {
        this.clientNotifier = clientNotifier;
    }
}

