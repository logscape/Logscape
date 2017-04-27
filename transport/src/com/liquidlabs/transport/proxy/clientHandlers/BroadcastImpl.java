package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;

import java.lang.reflect.Method;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 18/11/14
 * Time: 21:02
 * To change this template use File | Settings | File Templates.
 */
public class BroadcastImpl implements ClientHandler {

    @Broadcast
    public void sample() {
    }
    @Override
    public Object invoke(ProxyCaller client, AddressHandler addressHandler, Method method, Object[] args) throws InterruptedException {
            Object result = null;

            for (URI endPoint : addressHandler.getEndPointURIs()) {
                try {
                    result = client.clientExecute(method, endPoint, args, false, -1);
                } catch (RetryInvocationException t){
                    addressHandler.registerFailure(endPoint);
                } catch (Throwable t){
                    ProxyClient.LOGGER.warn("Ignoring error from Peer:" + endPoint + " RemoteMethod:" + method.getName(), t);
                }
            }
            return result;
    }
}
