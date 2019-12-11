package com.liquidlabs.vso.lookup;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.common.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;

public class LookupClusterTest {


    private URI lu1 = getURI("stcp://localhost:11000");
    private URI lu1_REP = getURI("stcp://localhost:15000");
    private URI lu2 = getURI("stcp://localhost:12000");
    private URI lu2_REP = getURI("stcp://localhost:15005");
	private String location = "DEFAULT";
	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

    List<LifeCycle> stoppable = new ArrayList<>();
	
	@Before
	public void setup() {
		try {
            com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
			System.out.println("===========================111111111111111111");
			shouldStartLookupOne();
			System.out.println("===========================222222222222222222");
			shouldStartLookupTwo();
			System.out.println("===========================333333333333333333");
			Thread.sleep(2000);
			
			System.out.println("===========================================");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@After
	public void after() {
	    stoppable.stream().forEach(LifeCycle::stop);
		scheduler.shutdownNow();

	}



//	TODO - fix test @Test
	public void testShouldReplicateFromOneLookupToAnother() throws Exception {
		shouldPutServiceInfoIntoLookup1234();
		
		Thread.sleep(500);
		
		// now look it up in the other instance
		ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 5678, "DAMIAN", 10 * 1024, lu2_REP.getPort());
		stoppable.add(mapperFactory);
        LookupSpace lookup2 = LookupSpaceImpl.getRemoteService(lu2.toString(), mapperFactory.getProxyFactory(),"ctx");
        stoppable.add(lookup2);
        String[] serviceAddresses = lookup2.getServiceAddresses("JOHNO", "", false);
        Assert.assertTrue("Should have > 0 addresses, got:" + serviceAddresses.length, serviceAddresses.length > 0);
	}
	
    public void shouldStartLookupOne() throws Exception {
        LookupSpaceImpl lu1 = new LookupSpaceImpl(this.lu1.getPort(), lu1_REP.getPort());
        lu1.start();
        lu1.addLookupPeer(lu2_REP);
        stoppable.add(lu1);
    }

    public void shouldStartLookupTwo() throws Exception {
        LookupSpaceImpl lu2 = new LookupSpaceImpl(this.lu2.getPort(), lu2_REP.getPort());
        lu2.start();
        lu2.addLookupPeer(lu1_REP);
        stoppable.add(lu2);
    }

    public void shouldStartServiceOne() throws Exception {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 1234, "DAMIAN", 10 * 1024, lu1_REP.getPort());
        SpaceServiceImpl spaceService = createSpaceService(mapperFactory);
        spaceService.store(new Thing("One", "Hi There Two!"), Integer.MAX_VALUE);
        spinServiceLookup(spaceService, "Two");
        stoppable.add(mapperFactory);
        stoppable.add(spaceService);
    }

 

//    @Test
    public void shouldStartServiceTwo() throws Exception {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 5678, "DAMIAN", 10 * 1024, lu2_REP.getPort());
        SpaceServiceImpl spaceService = createSpaceService(mapperFactory);
        spaceService.store(new Thing("Two", "Hi There One!"), Integer.MAX_VALUE);
        spinServiceLookup(spaceService, "One");
        stoppable.add(mapperFactory);
        stoppable.add(spaceService);

    }

    private SpaceServiceImpl createSpaceService(ORMapperFactory mapperFactory) {
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(),"ctx");

        SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl(lookup, mapperFactory, "TestSPACE", Executors.newScheduledThreadPool(5), true, false, false);
        spaceServiceImpl.start(this, "myBundle-1.0");
        stoppable.add(spaceServiceImpl);
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
    public void shouldPutServiceInfoIntoLookup1234() throws InterruptedException {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 1234, "DAMIAN", 10 * 1024, lu1_REP.getPort());
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(),"ctx");
        System.out.println(new Date() + " RegisteringService");
        lookup.registerService(new ServiceInfo("JOHNO", "http://localhost:8080", null, location, ""), Long.MAX_VALUE);
        stoppable.add(mapperFactory);
    }
    
    private void spinLookup(final LookupSpaceImpl lu1, final String name) throws InterruptedException {
    	scheduler.scheduleAtFixedRate(
    			new Runnable() {
					public void run() {
						try {
							String[] addresses = lu1.getServiceAddresses("JOHNO", location, false);
							System.out.println(new Date() + " SpinLookup:" + name + " " + Arrays.toString(addresses));
						} catch (Throwable t) {
							t.printStackTrace();
						}
					}
    	}, 1, 1, TimeUnit.SECONDS);
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
