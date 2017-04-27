package com.liquidlabs.log.server;

import org.junit.Test;

public class TheWebSocketTest {

    @Test
    public void should() {
        TheWebSocket theWebSocket = new TheWebSocket(null);
        theWebSocket.onMessage("{\"event\":\"tail\",\"data\":{\"file\":\"/Users/damian/cfd.txt\",\"uuid\":\"tail\"}}");
    }
}
