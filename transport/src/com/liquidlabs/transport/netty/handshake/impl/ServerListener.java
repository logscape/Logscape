package com.liquidlabs.transport.netty.handshake.impl;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 12/05/2014
 * Time: 11:51
 * To change this template use File | Settings | File Templates.
 */
public interface ServerListener {

    void messageReceived(ServerHandler handler, String message);

    void connectionOpen(ServerHandler serverHandler);
}