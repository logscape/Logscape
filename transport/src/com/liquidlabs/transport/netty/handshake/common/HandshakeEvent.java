package com.liquidlabs.transport.netty.handshake.common;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;


/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 12/05/2014
 * Time: 11:46
 * To change this template use File | Settings | File Templates.
 */
public class HandshakeEvent implements ChannelEvent {

    // internal vars ----------------------------------------------------------

    private final boolean successful;
    private final String remoteId;
    private final Channel channel;

    // constructors -----------------------------------------------------------

    private HandshakeEvent(String remoteId, Channel channel) {
        this.remoteId = remoteId;
        this.successful = remoteId != null;
        this.channel = channel;
    }

    // public static methods --------------------------------------------------

    public static HandshakeEvent handshakeSucceeded(String remoteId,
                                                    Channel channel) {
        return new HandshakeEvent(remoteId, channel);
    }

    public static HandshakeEvent handshakeFailed(Channel channel) {
        return new HandshakeEvent(null, channel);
    }

    // ChannelEvent -----------------------------------------------------------

    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public ChannelFuture getFuture() {
        return Channels.succeededFuture(this.channel);
    }

    // getters & setters ------------------------------------------------------

    public boolean isSuccessful() {
        return successful;
    }

    public String getRemoteId() {
        return remoteId;
    }
}