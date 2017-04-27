package com.logscape.meter;

import com.liquidlabs.logserver.LogServer;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 23/10/2014
 * Time: 09:42
 * To change this template use File | Settings | File Templates.
 */
public class MeterServiceTest {

    Mockery context = new Mockery();
    final LookupSpace lookupSpace = context.mock(LookupSpace.class);
    final LogServer logServer = context.mock(LogServer.class);
    private MeterServiceImpl meterService;

    @Before
    public void setup() {
        System.setProperty("metering.enabled", "true");
        ORMapperFactory factory = new ORMapperFactory();
        SpaceServiceImpl spaceService = new SpaceServiceImpl(lookupSpace, factory, "TEST", factory.getProxyFactory().getScheduler(), false, false, true);


        meterService = new MeterServiceImpl(spaceService, logServer, factory.getProxyFactory().getScheduler());

        context.checking(new Expectations() {{
            ignoring(lookupSpace);
            ignoring(logServer);
        }});


        meterService.start();


    }
    public void after() {
        meterService.stop();
    }

    @Test
    public void testStripOutTag() throws Exception {
        String asa = "LOGSCAPETOKEN:REPLACETOKEN LOGSCAPETAG:REPLACETAG Nov 11 06:54:58 Nov 11 2014 06:47:11 EXCELIAN-F01-LONDON : %ASA-6-305011: Built dynamic TCP translation from EXCELIAN-INSIDE:10.28.0.73/53063 to EXCELIAN-OUTSIDE:217.20.25.200/53063";
        String asa1 = "Nov 03 02:08:12 :Nov 03 01:55:39 EDT: %ASA-session-6-302014: LOGSCAPETOKEN:REPLACETOKEN LOGSCAPETAG:REPLACETAG Teardown TCP connection 90896319 for public:192.168.3.113/36203 to DB:192.168.5.120/3306 duration 0:00:00 bytes 7196 TCP FINs";

        String tagRemoved = Meter.removeTagAndToken(asa);

        assertEquals(-1, tagRemoved.indexOf("LOGSCAPETAG"));
    }




    @Test
    public void testAddIp() throws Exception {
        meterService.createAccount("test1", "10.28.0.150, 10.28.1.163,10.28.0.150, 10.28.1.163", 1, true, "", 1);
        Meter test1 = meterService.get("test1");
        test1.addIp("123.4.5.6");
        test1.addIp("123.4.5.6");
        test1.addIp("123.4.5.6");

        assertEquals("10.28.0.150, 10.28.1.163, 123.4.5.6", test1.getHostsList());
    }


    @Test
    public void testShouldExtractEmptyTag() throws Exception {
        assertEquals("", Meter.extractToken("hello token " + Meter.LS + ":"));
        assertEquals("TEST", Meter.extractToken("hello token " + Meter.LS + ":TEST"));
    }

    @Test
    public void testShouldExtractToken() throws Exception {
        assertEquals("123", Meter.extractToken("hello token " + Meter.LS + ":123 "));
        assertEquals("", Meter.extractToken("hello token " + ":123 "));
    }


    @Test
    public void testCreateAccountAndGet() throws Exception {
        String test1 = meterService.createAccount("test1", "10.10.10.10", 1, false, "", 1);
        Meter gotit = meterService.get("test1");
        assertNotNull(gotit);
        assertEquals("test1", gotit.id);

    }
    @Test
    public void testTotalAccounts() throws Exception {
        meterService.createAccount("test1", "10.10.10.10", 1, true, "", 1);
        assertEquals(1, meterService.totalAccounts());

    }

    @Test
    public void testActiveAccounts() throws Exception {
        String token = "abc";
        meterService.createAccount("test1", "10.99.123.42,10.10.10.10", 1, true, token, 1);
        assertEquals(0, meterService.activeAccounts());
        meterService.handle(token, "10.10.10.10","/stuff","msggg");
        assertEquals(1, meterService.activeAccounts());
    }



    @Test
    public void testTotalAccountsNearQuota() throws Exception {

        context.checking(new Expectations() {{
            ignoring(logServer);
        }});

        String token = "abc";

        meterService.createAccount("test1", "192.168.1.144,10.10.10.10", 10, true, token, 1);
        meterService.handle(token, "10.10.10.10","/stuff","msggg");
        assertEquals(0, meterService.totalAccountsNearQuota());
        for (int i = 0; i < 10 * 1024 * 1024; i++) {
            meterService.handle(token, "10.10.10.10","/stuff","msgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsgggmsggg");
        }
        meterService.flush();
        Meter test1 = meterService.get("test1");
        System.out.println("Meter:" + test1);

        assertEquals(1, meterService.totalAccountsNearQuota());
    }

    @Test
    public void testDeleteAccount() throws Exception {
        meterService.createAccount("test1", "10.10.10.10", 1, true, "", 1);
        assertEquals(1, meterService.totalAccounts());
        meterService.deleteAccount("test1");
        assertEquals(0, meterService.totalAccounts());
    }
}
