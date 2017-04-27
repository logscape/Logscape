package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 18/11/14
 * Time: 21:11
 * To change this template use File | Settings | File Templates.
 */
public class FailFastAndDisableImpl implements ClientHandler {

    @FailFastAndDisable
    public void sample() {
    }

    @Override
    public Object invoke(ProxyCaller aclient, AddressHandler addressHandler, Method method, Object[] args) throws InterruptedException {
        // yuck!
        ProxyClient client = (ProxyClient) aclient;
        URI endPointURI = null;
        try {
            endPointURI = addressHandler.getEndPointURI();
            Object result = client.clientExecute(method, endPointURI, args, false, -1);
            client.setErrors(0);
            return result;

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RetryInvocationException e) {
            try {
                if (endPointURI != null) addressHandler.registerFailure(endPointURI);
            } catch (Exception e2) {

                String msg = "FailFastDisabling due to:" + e.getMessage() + " endPoint:" + client.toSimpleString();
                client.LOGGER.warn(msg,e);
                client.setDisabled(true);
                client.stop();
                throw new InterruptedException(msg);
            }
            throw new RuntimeException(e);
        }
    }
}
