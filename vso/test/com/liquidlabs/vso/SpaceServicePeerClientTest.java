package com.liquidlabs.vso;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;

public class SpaceServicePeerClientTest {

	private LookupSpace lookup;
	private TestSpaceServiceImpl spaceServiceOne;
	private ORMapperFactory mapperFactory;

	@Before
	public void setUp() throws Exception {
		com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
		lookup = new LookupSpaceImpl(11000, 15000);
		lookup.start();
		
		spaceServiceOne = new TestSpaceServiceImpl(lookup, 11111);
		spaceServiceOne.start();
		
		this.mapperFactory = new ORMapperFactory(11223, "TestClient", 11223);
		this.mapperFactory.start();
	}


	@After
	public void tearDown() throws Exception {
        lookup.stop();
        mapperFactory.stop();
		spaceServiceOne.stop();
	}

	public int count = 0;
	private TestSpaceServiceImpl spaceServiceTwo;

	@Test
	public void shouldExportData() throws Exception {
		ClientTestData clientTestData = new ClientTestData();
		clientTestData.set("one", "data");
		spaceServiceOne.putStuff(clientTestData);
		Object results = spaceServiceOne.export();
		assertNotNull(results);
	}

	@Test
	public void testShouldWriteInOneAndReadInTheOther() throws Exception {
		TestSpaceService client = spaceServiceOne.getClient(mapperFactory.getProxyFactory());
		ClientTestData clientTestData = new ClientTestData();
		clientTestData.set("one", "data");
		client.putStuff(clientTestData);
		Thread.sleep(100);
		
		List<ClientTestData> resultsFromONE = client.getStuff();
		
		Assert.assertEquals("Didnt get data in space 1", 1, resultsFromONE.size());
		
		// Start the SECOND SpaceService
		spaceServiceTwo = new TestSpaceServiceImpl(lookup, 12222);
		spaceServiceTwo.start();
		
		// they should replicate
		Thread.sleep(100);
		List<ClientTestData> resultsFromTWO = spaceServiceTwo.getStuff();

		System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXX ONE Results" + resultsFromONE);
		System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXX TWO Results" + resultsFromTWO);
		
		spaceServiceTwo.stop();
		Thread.sleep(1000);
		
		// Get the client again
		client = spaceServiceOne.getClient(mapperFactory.getProxyFactory());
		List<ClientTestData> resultsFromONE_AGAIN = client.getStuff();
		System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXX ONE_AGAIN Results" + resultsFromONE_AGAIN);
		
		Assert.assertEquals(1, resultsFromTWO.size());
		Assert.assertEquals(1, resultsFromONE_AGAIN.size());
	}

	

}
