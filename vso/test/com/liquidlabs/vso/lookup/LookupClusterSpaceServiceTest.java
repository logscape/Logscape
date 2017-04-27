package com.liquidlabs.vso.lookup;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;

/**
 * Tests clustering of spaces on a single LUSpace
 * @author navery
 *
 */
public class LookupClusterSpaceServiceTest {


    private static final String SERVICE_NAME = "TEST_SERVICE";
	private URI lu1 = getURI("stcp://localhost:11000");
    private URI lu1_REP = getURI("stcp://localhost:15000");
	private String location;
	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
	private LookupSpaceImpl lu12;
	
	@Before
	public void setup() {
		try {
			com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
			System.out.println("===========================111111111111111111");
			shouldStartLookupOne();
			
			System.out.println("===========================================");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@After
	public void after() {
		lu12.stop();
		scheduler.shutdownNow();
	}

	@Test
	public void shouldReplicateFromTWOToONEDuringWrite() throws Exception {
		
		System.out.println("============================================START ONE");
		SpaceServiceImpl spaceServiceOne = shouldStartService(VSOProperties.getLookupPort() + 111, "One", false);
		
		Thread.sleep(500);
		
		System.out.println("============================================START TWO");
		SpaceServiceImpl spaceServiceTwo = shouldStartService(VSOProperties.getLookupPort() + 222, "Two", false);
		
		Thread.sleep(1000);
		System.out.println("============================================WRITE DATA");
		
		writeDataToSpaceService(10, "Two", spaceServiceTwo);

		Thread.sleep(100);

		// test Two worked and replicated Thing-One
		Thing item = spaceServiceOne.findById(Thing.class, "Two0");
		Assert.assertNotNull("SpaceONE failed to pick up SpaceONE data", item);
		Set<String> keySet = spaceServiceOne.keySet(Thing.class);
		Assert.assertEquals(10, keySet.size());
	}
	
	@Test
	public void shouldReplicateFromOneToTWODuringWrite() throws Exception {
		
		SpaceServiceImpl spaceServiceOne = shouldStartService(VSOProperties.getLookupPort() + 1111, "One", false);
		
		SpaceServiceImpl spaceServiceTwo = shouldStartService(VSOProperties.getLookupPort() + 2222, "Two", false);
		
		Thread.sleep(1000);
		
		writeDataToSpaceService(10, "One", spaceServiceOne);
		
		Thread.sleep(1000);
		
		// test Two worked and replicated Thing-One
		Thing item2 = spaceServiceTwo.findById(Thing.class, "One0");
		
		Assert.assertNotNull("SpaceTWO failed to pick up SpaceONE data", item2);
		System.out.printf("Space ONE - found item %s\n", item2.message);
		
	}
	
	@Test
	public void shouldReplicateBothDirectionsOnInitialSync() throws Exception {
		
		SpaceServiceImpl spaceServiceOne = shouldStartService(VSOProperties.getLookupPort() + 3333, "One", true);
		
		SpaceServiceImpl spaceServiceTwo = shouldStartService(VSOProperties.getLookupPort() + 4444, "Two", true);
		
		Thread.sleep(2000);
		
		// test Two worked and replicated Thing-One
		Thing item2 = spaceServiceTwo.findById(Thing.class, "One0");
		Assert.assertNotNull("SpaceTWO failed to pick up SpaceONE data", item2);
		System.out.printf("Space ONE - found item %s\n", item2.message);
		
		
		// test One worked and replicated Thing-Two
		Thing item1 = spaceServiceOne.findById(Thing.class, "Two0");
		Assert.assertNotNull("SpaceONE failed to pick up SpaceTwo data", item1);
		System.out.printf("Space TWO - found item %s\n", SERVICE_NAME, item1.message);
	}
	
    public void shouldStartLookupOne() throws Exception {
        lu12 = new LookupSpaceImpl(this.lu1.getPort(), lu1_REP.getPort());
        lu12.start();
    }

    public SpaceServiceImpl shouldStartService(int port, String name, boolean writeThing) throws Exception {
        ORMapperFactory mapperFactory = new ORMapperFactory(port, SERVICE_NAME, 10, port);
        SpaceServiceImpl spaceService = createSpaceService(mapperFactory);
        if (writeThing) writeDataToSpaceService(10, name, spaceService);
        return spaceService;
    }

	private void writeDataToSpaceService(int amount, String name, SpaceServiceImpl spaceService) {
		for (int i = 0; i < amount; i++) {
			spaceService.store(new Thing(name + i, "Hi There Im:" + name), Integer.MAX_VALUE);
		}
	}
 
    private SpaceServiceImpl createSpaceService(ORMapperFactory mapperFactory) {
        LookupSpace lookup = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(),"ctx");

        SpaceServiceImpl spaceServiceImpl = new SpaceServiceImpl(lookup, mapperFactory, "TEST-SPACE", Executors.newScheduledThreadPool(5), true, false, false);
        spaceServiceImpl.start(this, "myBundle-1.0");
        return spaceServiceImpl;
    }

   
    
 //   @Test
    public void shouldPutServiceInfoIntoLookup1234() throws InterruptedException {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 1234, SERVICE_NAME, 10 * 1024, VSOProperties.getLookupPort() + 1234);
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
