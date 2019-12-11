package com.liquidlabs.vso.lookup;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.liquidlabs.common.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.VSOProperties;

public class LookupClusterClientTest {


    private URI lu1 = getURI("stcp://localhost:11111");
    private URI lu1_REP = getURI("stcp://localhost:15111");
    private URI lu2 = getURI("stcp://localhost:22222");
    private URI lu2_REP = getURI("stcp://localhost:25222");
	private String location = "DEFAULT";
	List<LifeCycle> stoppabled = new ArrayList<>();
	
	@Before
	public void setup() {
        com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
	}
	
	@After
	public void after() {
	    stoppabled.stream().forEach(item -> item.stop());
	}

	@Test
	public void testShouldAllowClientToGetClusterAddr() throws Exception {
		
		startLookupOne();
		
		ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 1234, "CLIENT", 10 * 1024, 999);
        mapperFactory.start();
        stoppabled.add(mapperFactory);


        LookupSpace lookupClient1 = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(), "CONTEXT-1");


        System.out.println(">>>>>>>> 1 >>" + lookupClient1.toString());

		startLookupTwo();

        Thread.sleep(1000);
		LookupSpace lookupClient2 = LookupSpaceImpl.getRemoteService(lu1.toString(), mapperFactory.getProxyFactory(), "CONTEXT-2");

		System.out.println(">>>>>>>> 1 >>" + lookupClient2.toString());
			
		Thread.sleep(1000);
		
		System.out.println("\n\n>>>>>>>> 2 >>" + lookupClient2.toString());
		Assert.assertTrue(lookupClient2.toString().contains("11111"));
		Assert.assertTrue(lookupClient2.toString().contains("22222"));
	}
	
    public void startLookupOne() throws Exception {
        LookupSpaceImpl lu1 = new LookupSpaceImpl(this.lu1.getPort(), lu1_REP.getPort());
        lu1.start();
        lu1.addLookupPeer(lu2_REP);
        stoppabled.add(lu1);
    }

    public void startLookupTwo() throws Exception {
        LookupSpaceImpl lu2 = new LookupSpaceImpl(this.lu2.getPort(), lu2_REP.getPort());
        lu2.start();
        lu2.addLookupPeer(lu1_REP);
        stoppabled.add(lu2);
    }

 //   @Test
    public void shouldPutServiceInfoIntoLookup1234() throws InterruptedException {
        ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getLookupPort() + 1234, "DAMIAN", 10 * 1024, lu1_REP.getPort());
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
