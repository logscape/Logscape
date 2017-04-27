package com.liquidlabs.transport.netty;

import com.liquidlabs.common.collection.Collections;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.PeerListener;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by neil on 29/06/16.
 */
public abstract class DefaultEndPoint implements EndPoint{

    private URI endpointAddress;
    private Receiver receiver;

    public DefaultEndPoint(URI endpointAddress, Receiver receiver) {
        this.endpointAddress = endpointAddress;

        this.receiver = receiver;
    }
    @Override
    public Receiver getReceiver() {
        return receiver;
    }

    @Override
    public boolean addPeer(URI peer) {
        return false;
    }

    @Override
    public void addPeerListener(PeerListener peerListener) {

    }

    @Override
    public void removePeer(URI peer) {

    }

    @Override
    public void sendToPeers(byte[] data, boolean verbose) {

    }

    @Override
    public Collection<URI> getPeerNames() {
        return new ArrayList<>();
    }
//    @Override
//    public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
//        return new byte[0];
//    }

//    @Override
//    public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) throws InterruptedException {
//        return new byte[0];
//    }

//    @Override
//    public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException {
//        return new byte[0];
//    }


    @Override
    public boolean isForMe(Object payload) {
        return false;
    }

    @Override
    public URI getAddress() {
        return endpointAddress;
    }

    @Override
    public void dumpStats() {

    }

//    @Override
//    public void start() {
//
//    }
//
//    @Override
//    public void stop() {
//
//    }
}
