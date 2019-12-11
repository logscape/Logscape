package com.liquidlabs.vso.lookup;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;

/**
 * Tests to see that the client can still find Service Info when the first instance dies
 * Test A  => A&B => B -> A&B => A => A=>B
 * @author neil
 *
 */
public class FIX_LookupClusterServiceClientTest {


    private URI lu1 = getURI("stcp://localhost:11000");
    private URI lu1_REP = getURI("stcp://localhost:15000");
    private URI lu2 = getURI("stcp://localhost:12000");
    private URI lu2_REP = getURI("stcp://localhost:15005");
	private String location = "DEFAULT";
	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
	private LookupSpaceImpl lu1_SP;
	private LookupSpaceImpl lu2_SP;
    private boolean strictMatch;

    //	@Before
	public void setup() {
		com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
	}
	
//	@After
	public void after() {
		scheduler.shutdownNow();
	}

    @Test
    public void shouldDoNothing() {

    }

    //	@Test
	public void xxxtestShouldReplicateFromOneLookupToAnother() throws Exception {
		
		System.out.println("\n\n ======================== (A) ======================");
		// (A)
		shouldStartLookupOne();
		
		shouldPutServiceInfoIntoLookup1234(11);
		
		System.out.println("\n\n>>>>>>>>>>>> Get Client & Register callback");
		
		// now look it up in the other instance
		LookupSpace lookupClient = getLookupClient(lu1.toString(), 22);
        String[] serviceAddresses = lookupClient.getServiceAddresses("JOHNO", "", strictMatch);
        Assert.assertTrue("A) Should have > 0 addresses, got:" + serviceAddresses.length, serviceAddresses.length > 0);
        
        System.out.println("\n\n ======================== (A & B) HA ======================");
        
		// (A & B)
		shouldStartLookupTwo();
		
		// lookup Client should have been told about the new address
		serviceAddresses = lookupClient.getServiceAddresses("JOHNO", "", strictMatch);
        Assert.assertTrue("(A&B) Should have > 0 addresses, got:" + serviceAddresses.length, serviceAddresses.length > 0);
        
        pause();
        
        System.out.println("\n\n ======================== (_A_ => B) Failover to B ======================");
        // (B) Only
        lu1_SP.stop();
        pause();
        
		serviceAddresses = lookupClient.getServiceAddresses("JOHNO", "", strictMatch);
        Assert.assertTrue("(B Only) Should have > 0 addresses, got:" + serviceAddresses.length, serviceAddresses.length > 0);

        System.out.println("\n\n ======================== (A & B) HA 2nd time ======================");
        
		// (A & B) again
		shouldStartLookupOne();
		serviceAddresses = lookupClient.getServiceAddresses("JOHNO", "", strictMatch);
        Assert.assertTrue("(B Only) Should have > 0 addresses, got:" + serviceAddresses.length, serviceAddresses.length > 0);
		
        System.out.println("\n\n ======================== (A <= _B_) Fail to A ======================");
        // (A) Only
        lu2_SP.stop();
        pause();
    	serviceAddresses = lookupClient.getServiceAddresses("JOHNO", "", strictMatch);
        Assert.assertTrue("(B Only) Should have > 0 addresses, got:" + serviceAddresses.length, serviceAddresses.length > 0);
        
 
        System.out.println("\n\n ======================== (A & B) HA - where we completed the HA cycle (i.e. both bounced & recovered) ======================");
        // Final HA - bring back the bounced (B)
        // (A & B)
		shouldStartLookupTwo();
		
		// lookup Client should have been told about the new address
		serviceAddresses = lookupClient.getServiceAddresses("JOHNO", "", strictMatch);
        Assert.assertTrue("(A&B) Should have > 0 addresses, got:" + serviceAddresses.length, serviceAddresses.length > 0);
        
		
	}

	private void pause() throws InterruptedException {
		Thread.sleep(500);
	}

	private LookupSpace getLookupClient(String address, int clientPort) {
		ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + clientPort, "DAMIAN", 10 * 1024,VSOProperties.getLookupPort() + clientPort+10);
        LookupSpace lookup2 = LookupSpaceImpl.getRemoteService(address, mapperFactory.getProxyFactory(),"CLIENT-:" + clientPort);
		return lookup2;
	}
	
    public void shouldStartLookupOne() throws Exception {
        lu1_SP = new LookupSpaceImpl(this.lu1.getPort(), lu1_REP.getPort());
        lu1_SP.start();
        lu1_SP.addLookupPeer(lu2_REP);
        pause();
    }

    public void shouldStartLookupTwo() throws Exception {
        lu2_SP = new LookupSpaceImpl(this.lu2.getPort(), lu2_REP.getPort());
        lu2_SP.start();
        lu2_SP.addLookupPeer(lu1_REP);
        pause();
    }

    public void shouldStartServiceOne() throws Exception {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 1234, "DAMIAN", 10 * 1024, lu1_REP.getPort());
        SpaceServiceImpl spaceService = createSpaceService(mapperFactory);
        spaceService.store(new Thing("One", "Hi There Two!"), Integer.MAX_VALUE);
        spinServiceLookup(spaceService, "Two");
    }

 

    public void shouldStartServiceTwo() throws Exception {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 5678, "DAMIAN", 10 * 1024, lu2_REP.getPort());
        SpaceServiceImpl spaceService = createSpaceService(mapperFactory);
        spaceService.store(new Thing("Two", "Hi There One!"), Integer.MAX_VALUE);
        spinServiceLookup(spaceService, "One");

    }

    private SpaceServiceImpl createSpaceService(ORMapperFactory mapperFactory) {
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(),"ctx");

        SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl(lookup, mapperFactory, "TestSPACE", Executors.newScheduledThreadPool(5), true, false, false);
        spaceServiceImpl.start(this, "myBundle-1.0");
        return spaceServiceImpl;
    }

   
    
    private void spinServiceLookup(final SpaceServiceImpl spaceService, final String name) throws InterruptedException {
    	scheduler.scheduleAtFixedRate(new Runnable() {
    		public void run() {
    			try {
	    			Thing item = spaceService.findById(Thing.class, name);
	    			if (item!=null) System.out.printf("%s - found item %s\n", name, item.message);
    			} catch (Throwable t) {
    				t.printStackTrace();
    			}
    		}
    	}, 1, 1, TimeUnit.SECONDS);
    }

 //   @Test
    public void shouldPutServiceInfoIntoLookup1234(int port) throws InterruptedException {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + port, "DAMIAN", 10 * 1024, VSOProperties.getLookupPort() + port);
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(),"ctx");
        System.out.println(new Date() + " RegisteringService");
        lookup.registerService(new ServiceInfo("JOHNO", "http://localhost:8080", null, location, ""), Long.MAX_VALUE);
        pause();
    }
    
    private URI getURI(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("boom - no uri");
    }
}
