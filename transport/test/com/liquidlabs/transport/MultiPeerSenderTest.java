package com.liquidlabs.transport;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;



public class MultiPeerSenderTest {

    @Test
    public void shouldAddPeer() throws Exception {
        MultiPeerSender sender = new MultiPeerSender(new URI("tcp://localhost:8080"), createSender());
        boolean result = sender.addPeer(new URI("tcp://localhost:8000"));
        assertThat(result, is(true));
    }

    @Test
    public void shouldNotAddPeerIfMe() throws Exception {
        MultiPeerSender sender = new MultiPeerSender(new URI("tcp://localhost:8080"), createSender());
        boolean result = sender.addPeer(new URI("tcp://localhost:8080"));
        assertThat(result, is(false));
    }

    @Test
    public void shouldNotAddPeerIfAlreadyAdded() throws Exception {
        MultiPeerSender sender = new MultiPeerSender(new URI("tcp://localhost:8080"), createSender());
        sender.addPeer(new URI("tcp://localhost:8000"));
        boolean result = sender.addPeer(new URI("tcp://localhost:8000"));
        assertThat(result, is(false));
    }

    @Test
    public void shouldRemovePeer() throws Exception {
        MultiPeerSender sender = new MultiPeerSender(new URI("tcp://localhost:8080"), createSender());
        sender.addPeer(new URI("tcp://localhost:8000"));
        sender.removePeer(new URI("tcp://localhost:8000"));
        assertThat(sender.getPeerNames().contains(new URI("tcp://localhost:8000")), is(false));
    }
    
    @Test
    public void shouldReAddRemovedPeer() throws Exception {
        MultiPeerSender sender = new MultiPeerSender(new URI("tcp://localhost:8080"), createSender());
        sender.addPeer(new URI("tcp://localhost:8000"));
        sender.removePeer(new URI("tcp://localhost:8000"));
        assertThat(sender.addPeer(new URI("tcp://localhost:8000")), is(true));
    }



    private Sender createSender() {
        return new Sender(){
            public void start() {
            }

            public void stop() {
            }

            public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException {
                return new byte[0];
            }

            public URI getAddress() {
                return null;
            }

            public void dumpStats() {
            }
        };
    }

}
