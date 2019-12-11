package com.liquidlabs.vso.lookup;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.liquidlabs.common.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;

/**
 * Tests clustering of SpaceServices - through a replicated LUSpace
 */
public class LookupClusteredSpaceServiceClusteredTest {


    private static final String SERVICE_NAME = "TEST_SERVICE";
    
	private URI lu1 = getURI("stcp://localhost:11000");
    private URI lu1_REP = getURI("stcp://localhost:15000");
    
    private URI lu2 = getURI("stcp://localhost:22000");
    private URI lu2_REP = getURI("stcp://localhost:25000");
    
	private String location;
	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
	private LookupSpaceImpl luSpace1;
	private LookupSpaceImpl luSpace2;
	List<LifeCycle> stoppable = new ArrayList<>();
	@Before
	public void setup() {
		try {
			com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
			
			shouldStartLookupOne();
			
			shouldStartLookupTwo();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@After
	public void after() {
		stoppable.stream().forEach(LifeCycle::stop);
		scheduler.shutdownNow();
	}
	
	@Test
	public void shouldNotClusterDifferentServices() throws Exception {
		SpaceServiceImpl spaceServiceOne = shouldStartSharedService(VSOProperties.getLookupPort() + 5555, "", false, lu1, "SERVICE");
		
		SpaceServiceImpl BADspaceServiceTwo = shouldStartSharedService(VSOProperties.getLookupPort() + 6666, "", false, lu2, "BAD-SERVICE");

		Thread.sleep(1000);

		spaceServiceOne.store(new Thing("id1", "Hi There Im:0"), 10);

		Thread.sleep(1000);

		Thing i01 = spaceServiceOne.findById(Thing.class, "id1");
		Thing i02 = BADspaceServiceTwo.findById(Thing.class, "id1");


		assertNotNull(i01);
		assertNull("Bad service should not have stuff", i02);
	}
	
//	@Test DodgyTest? Probably should make these work!
	public void shouldMaintainLeaseOnInstanceOne() throws Exception {
		System.out.println("============================================START ONE");
		SpaceServiceImpl spaceServiceOne = shouldStartService(VSOProperties.getLookupPort() + 111, "dataOne", false, lu1);
		
		Thread.sleep(50);
		
		List<ServiceInfo> initialServiceWorked = luSpace1.findService("name equals " + SERVICE_NAME);
		assertEquals("Space 1 - Store Service 1:" + initialServiceWorked.size(), 1, initialServiceWorked.size());
		
		Thread.sleep(500);
		
		System.out.println("============================================START TWO");
		SpaceServiceImpl spaceServiceTwo = shouldStartService(VSOProperties.getLookupPort() + 222, "dataTwo", false, lu2);
		
		Thread.sleep(1000);

		String leaseKey = spaceServiceOne.store(new Thing("id1", "Hi There Im:0"), 10);
		Thread.sleep(5000);
		Thing i01 = spaceServiceOne.findById(Thing.class, "id1");
		Thing i02 = spaceServiceTwo.findById(Thing.class, "id1");
		assertNotNull(i01);
		assertNotNull(i02);

		spaceServiceOne.renewLease(leaseKey, 5);
		Thread.sleep(5000);
		spaceServiceOne.renewLease(leaseKey, 5);
		Thread.sleep(5000);
		spaceServiceOne.renewLease(leaseKey, 5);
		Thread.sleep(5000);
		spaceServiceOne.renewLease(leaseKey, 5);

		// the data should still be available in both nodes
		Thing i11 = spaceServiceOne.findById(Thing.class, "id1");

		Thing i12 = spaceServiceTwo.findById(Thing.class, "id1");
		assertNotNull(i11);
		assertNotNull(i12);
	}
	
//	@Test DodgyTest? Probably should make these work!
	public void shouldReplicateFromTWOToONEDuringWrite() throws Exception {
		
		System.out.println("============================================START ONE");
		SpaceServiceImpl spaceServiceOne = shouldStartService(VSOProperties.getLookupPort() + 1112, "dataOne", false, lu1);
		
		Thread.sleep(50);
		
		List<ServiceInfo> initialServiceWorked = luSpace1.findService("name equals " + SERVICE_NAME);
		Assert.assertEquals("Space 1 - Store Service 1:" + initialServiceWorked.size(), 1, initialServiceWorked.size());
		
		Thread.sleep(500);
		
		System.out.println("============================================START TWO");
		SpaceServiceImpl spaceServiceTwo = shouldStartService(VSOProperties.getLookupPort() + 2221, "dataTwo", false, lu2);
		
		Thread.sleep(1000);
		System.out.println("============================================WRITE DATA");
		
		writeDataToSpaceService("Two", spaceServiceTwo);

		Thread.sleep(1000);
		
		// test Two worked and replicated Thing-One
		Thing item = spaceServiceOne.findById(Thing.class, "Two");
		Thread.sleep(1000);
		System.out.println("============================================WAITING");
		Assert.assertNotNull("SpaceONE failed to pick up SpaceONE data", item);
		
		List<ServiceInfo> findServiceFromONE = luSpace1.findService("name equals " + SERVICE_NAME);
		Assert.assertEquals("Space 1 - Failed to get 2 services, got:" + findServiceFromONE.size(), 2, findServiceFromONE.size());
		
		List<ServiceInfo> findServiceFromTWO = luSpace2.findService("name equals " + SERVICE_NAME);
		Assert.assertEquals("Space 2 - Failed to get 2 services, got:" + findServiceFromTWO.size(), 2, findServiceFromTWO.size());
	}
	
	@Test
	public void shouldReplicateFromOneToTWODuringWrite() throws Exception {
		
		SpaceServiceImpl spaceServiceOne = shouldStartService(VSOProperties.getLookupPort() + 3333, "One", false, lu1);
		
		SpaceServiceImpl spaceServiceTwo = shouldStartService(VSOProperties.getLookupPort() + 4444, "Two", false, lu2);

		Thread.sleep(2000);

		writeDataToSpaceService("One", spaceServiceOne);

		Thread.sleep(500);

		// test Two worked and replicated Thing-One
		Thing item2 = spaceServiceTwo.findById(Thing.class, "One");
		Assert.assertNotNull("SpaceTWO failed to pick up SpaceONE data", item2);
		System.out.printf("Space ONE - found item %s\n", item2.message);
	}
	
//	@Test DodgyTest
	public void shouldReplicateBothDirectionsOnInitialSync() throws Exception {
		SpaceServiceImpl spaceServiceOne = shouldStartService(VSOProperties.getLookupPort() + 7777, "One", true, lu1);
		SpaceServiceImpl spaceServiceTwo = shouldStartService(VSOProperties.getLookupPort() + 8888, "Two", true, lu2);
		
		Thread.sleep(1000);
		
		// test Two worked and replicated Thing-One
		Thing item2 = spaceServiceTwo.findById(Thing.class, "One");
		Assert.assertNotNull("SpaceTWO failed to pick up SpaceONE data", item2);
		System.out.printf("Space ONE - found item %s\n", item2.message);
		
		
		// test One worked and replicated Thing-Two
		Thing item1 = spaceServiceOne.findById(Thing.class, "Two");
		Assert.assertNotNull("SpaceONE failed to pick up SpaceTwo data", item1);
		
		System.out.printf("Space TWO - found item %s\n", SERVICE_NAME, item1.message);
	}
	
    public void shouldStartLookupOne() throws Exception {
        luSpace1 = new LookupSpaceImpl(this.lu1.getPort(), lu1_REP.getPort());
        luSpace1.start();
        luSpace1.addLookupPeer(lu2_REP);
		stoppable.add(luSpace1);
    }
    public void shouldStartLookupTwo() throws Exception {
    	luSpace2 = new LookupSpaceImpl(this.lu2.getPort(), lu2_REP.getPort());
    	luSpace2.start();
    	luSpace2.addLookupPeer(lu1_REP);
    	stoppable.add(luSpace2);
    }

    public SpaceServiceImpl shouldStartService(int port, String dataId, boolean writeThing, URI lu) throws Exception {
        ORMapperFactory mapperFactory = new ORMapperFactory(port, SERVICE_NAME, 100, port);
        SpaceServiceImpl spaceService = createSpaceService(mapperFactory, lu);
        if (writeThing) writeDataToSpaceService(dataId, spaceService);
        stoppable.add(mapperFactory);
        stoppable.add(spaceService);
        return spaceService;
    }
    
    public SpaceServiceImpl shouldStartSharedService(int port, String dataId, boolean writeThing, URI lu, String serviceName) throws Exception {
        ORMapperFactory mapperFactory = new ORMapperFactory(port, "SHARED", 100, port);
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu.toString(), mapperFactory.getProxyFactory(),"ctx");

        SpaceServiceImpl spaceService = new SpaceServiceImpl(lookup, mapperFactory, serviceName, Executors.newScheduledThreadPool(5), true, false, false);
        spaceService.start(this, "myBundle-1.0");
        if (writeThing) writeDataToSpaceService(dataId, spaceService);
        stoppable.add(mapperFactory);
        stoppable.add(spaceService);
        return spaceService;
    }

	private String writeDataToSpaceService(String dataId, SpaceServiceImpl spaceService) {
		return spaceService.store(new Thing(dataId, "Hi There Im:" + dataId), Integer.MAX_VALUE);
	}
 
    private SpaceServiceImpl createSpaceService(ORMapperFactory mapperFactory, URI lu) {
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu.toString(), mapperFactory.getProxyFactory(),true, "ctx");

        SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl(lookup, mapperFactory, SERVICE_NAME, Executors.newScheduledThreadPool(5), true, false, false);
        spaceServiceImpl.start(this, "myBundle-1.0");
        return spaceServiceImpl;
    }

   
    public void shouldPutServiceInfoIntoLookup1234() throws InterruptedException {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 1234, SERVICE_NAME, 10 * 1024, lu1_REP.getPort());
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(),"ctx");
        System.out.println(new Date() + " RegisteringService");
        lookup.registerService(new ServiceInfo("JOHNO", "http://localhost:8080", null, location, ""), Long.MAX_VALUE);
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
