package com.liquidlabs.vso.lookup;

import java.util.Arrays;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.orm.ORMapperClient;

public class LookupSpaceTest extends MockObjectTestCase {
	
	private LookupSpaceImpl lookupSpace;
	private Mock orMapperClient;
	private String location = "";

	protected void setUp() throws Exception {
		super.setUp();
		orMapperClient = mock(ORMapperClient.class);
		lookupSpace = new LookupSpaceImpl((ORMapperClient) orMapperClient.proxy());
	}
	protected void tearDown() throws Exception {
		lookupSpace.stop();
		super.tearDown();
	}
	
	public void testRegisterSevice() throws Exception {
		orMapperClient.expects(once()).method("store").withAnyArguments();
		lookupSpace.registerService(new ServiceInfo("someName", "resourceSpace-01", JmxHtmlServerImpl.locateHttpUrL(), location, "Management"), -1);
	}
	public void testLookupSevice() throws Exception {
		ServiceInfo serviceInfo = new ServiceInfo("someName", "resourceSpace-01", JmxHtmlServerImpl.locateHttpUrL(), location, "Management");
		
		ServiceInfo[] results = new ServiceInfo[] { serviceInfo };
		orMapperClient.expects(once()).method("findObjects").withAnyArguments().will(returnValue(Arrays.asList(results)));
		String[] resourceAddress = lookupSpace.getServiceAddresses("someName", location, false);
		assertEquals(1, resourceAddress.length);
	}
	
	

}
