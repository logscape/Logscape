package com.liquidlabs.transport.netty.handshake;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 13/05/2014
 * Time: 10:11
 * To change this template use File | Settings | File Templates.
 */
public class HostIpFilterTest {
    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testIsValid() throws Exception {
        HostIpFilter hostIpFilter = new HostIpFilter();
        boolean valid = hostIpFilter.isValid(new InetSocketAddress("10.28.1.1", 1000));
        Assert.assertTrue(valid);

    }
    @Test
    public void testIsFilteringProperly() throws Exception {
        System.setProperty("host.filter","10\\.28\\.[0-9]+\\.[0-9]+");
        HostIpFilter hostIpFilter = new HostIpFilter();
        Assert.assertTrue(hostIpFilter.isValid(new InetSocketAddress("10.28.1.1", 1000)));
        Assert.assertFalse(hostIpFilter.isValid(new InetSocketAddress("110.28.1.1", 1000)));

    }

    /**
     * 10.28.1.[0-2]+
     * 10.28.2.[0-2]+
     * 10.28.3.[0-2]+
     * 10.28.4.[0-2]+
     * 10.28.5.[0-2]+
     * @throws Exception
     */
    @Test
    public void testIsMultipleNumbersFilteringProperly() throws Exception {
        System.setProperty("host.filter","(10|11)\\.28\\.[0-9]+\\.[0-9]+");
        HostIpFilter hostIpFilter = new HostIpFilter();
        Assert.assertTrue(hostIpFilter.isValid(new InetSocketAddress("10.28.1.1", 1000)));
        Assert.assertTrue(hostIpFilter.isValid(new InetSocketAddress("11.28.1.1", 1000)));
        Assert.assertFalse(hostIpFilter.isValid(new InetSocketAddress("12.28.1.1", 1000)));

    }

}
