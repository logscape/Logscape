package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 18/11/14
 * Time: 21:40
 * To change this template use File | Settings | File Templates.
 */
public class DecoupledImpl implements ClientHandler {

    @Decoupled
    public void sample() {
    }
    @Override
    public Object invoke(final ProxyCaller client, AddressHandler addressHandler, final Method method, final Object[] args) throws InterruptedException {
        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    client.sendWithRetry(method, args);
                } catch (IOException e) {
                } catch (InterruptedException e) {
                } catch (Exception e){
                    ProxyClient.LOGGER.debug("callDecoupled failed:" + e,e);
                }
            }
        };
        Thread task = new Thread(runnable);
        task.setDaemon(true);
        task.start();

            // cannot risk these tasks from being rejected and leading to network-deadlock
//		executor.submit(runnable);
        return null;
    }
}
