package com.liquidlabs.syslog4vscape.handler;

import org.junit.Test;
import org.productivity.java.syslog4j.util.SyslogUtility;

import static org.junit.Assert.*;

/**
 * Created by neil on 04/08/2015.
 */
public class MultiplexerHandlerTest {

    @Test
    public void testGetFacility() throws Exception {
        byte[] raw = "<12>Aug  4 08:43:23 alteredcarbon.local boom!! Mail_facility Message Sent".getBytes();
        MultiplexerHandler handler = new MultiplexerHandler();
        String facility = handler.getFacility(raw, 7);
        assertEquals("USER", facility);


    }
}