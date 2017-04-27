package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 18/11/14
 * Time: 21:28
 * To change this template use File | Settings | File Templates.
 */
public class FailFastOnceImpl implements ClientHandler {

    @FailFastOnce
    public void sample() {
    }
    @Override
    public Object invoke(ProxyCaller client, AddressHandler addressHandler, Method method, Object[] args) throws InterruptedException {
        FailFastOnce annotation = method.getAnnotation(FailFastOnce.class);
        int ttl = annotation.ttl();

        URI endPoint = null;
            try {
                endPoint = addressHandler.getEndPointURI();
                return client.clientExecute(method, endPoint, args, false, ttl);
            } catch (Exception e) {
                // too much noise here...
                if (ProxyClient.LOGGER.isDebugEnabled()) ProxyClient.LOGGER.debug("FailedFastOnce Fail-Invoked:" + endPoint + " FAILED-Address:" + endPoint, e);
                try {
                    addressHandler.registerFailure(endPoint);
                    endPoint = addressHandler.getEndPointURI();
                    if (ProxyClient.LOGGER.isDebugEnabled()) ProxyClient.LOGGER.debug("FailedFastOnce Fail-Invoked:" + endPoint + " NEW-Address:" + endPoint, e);
                    return client.clientExecute(method, endPoint, args, false, ttl);
                } catch (Throwable t) {
                    throw new RuntimeException("FailFastOnce FAILED-Address:" + endPoint + " ex:" + e.getMessage(), e);
                }
            }
        }
}
