package com.liquidlabs.log.sla;

import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.container.SLAContainerAdminMBean;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class SLANotifierTest extends MockObjectTestCase{
	
	private Mock lookupSpace;
	private Mock logSpace;
	private MyProxyFactory proxyFactory;
	private SLAEventWirer eventWirer;
	private Mock aggSpace;
	private String location;



	public class MySLAContainerMBean implements SLAContainerAdminMBean {

		public boolean getNotifyFilterCalled;
		public String filter = "FOO";

		public String displaySLAContent() {
			return null;
		}

		public String triggerSLAAction(String script, int priority) {
			return null;
		}

		public String updateSLA(String sla) {
			return null;
		}

		public String getNotifyFilter() {
			getNotifyFilterCalled = true;
			return filter;
		}

		public void handleLogEvent(String message) {
		}

		public String getServiceToRun() {
			return null;
		}

		public String resetSLAStatus() {
			return null;
		}
		public String validateSLA(String sla) {
			return null;
		}

		public String getServiceCriteria() {
			return null;
		}

		public String status() {
			return null;
		}
	}
	
	public class MyProxyFactory extends ProxyFactoryImpl {
		private MySLAContainerMBean mySLAContainerMBean = new MySLAContainerMBean();

		public MyProxyFactory() throws UnknownHostException, URISyntaxException {
			super(new TransportFactoryImpl(Executors.newCachedThreadPool(), "test"), 9000, Executors.newCachedThreadPool(), "testService");
		}
		
		@Override
		public <T> T getRemoteService(String listenerId,
				Class<T> interfaceClass, String... endPoints) {
			return (T) mySLAContainerMBean;
		}
	}
	
	@Override
	protected void setUp() throws Exception {
		lookupSpace = mock(LookupSpace.class);
		logSpace = mock(LogSpace.class);
		proxyFactory = new MyProxyFactory();
		aggSpace = mock(AggSpace.class);
		eventWirer = new SLAEventWirer((LookupSpace)lookupSpace.proxy(), (LogSpace)logSpace.proxy(), proxyFactory, (AggSpace) aggSpace.proxy());
	}

	public void testShouldWireEventListenerToSLAContainers() throws Exception {
		mockEvents(services());
		eventWirer.run();
		assertTrue(proxyFactory.mySLAContainerMBean.getNotifyFilterCalled);
	}

	private void mockEvents(List<ServiceInfo> services) {
		lookupSpace.expects(once()).method("findService").with(eq("interfaceName equals " + SLAContainerAdminMBean.class.getName())).will(returnValue(services));
        aggSpace.expects(once()).method("registerEventListener").with(ANYTHING, ANYTHING, eq("message contains FOO"), ANYTHING);
	}

	public void testShouldNotRegisterListenerIfAlreadyRegistered() throws Exception {
		mockEvents(services());
		eventWirer.run();
		lookupSpace.expects(once()).method("findService").with(eq("interfaceName equals " + SLAContainerAdminMBean.class.getName())).will(returnValue(services()));
		// second run should not call back on to logSpace
		eventWirer.run();
	}
	
	public void testShouldUnRegisterIfSLARemoved() throws Exception {
		mockEvents(services());
		eventWirer.run();
		lookupSpace.expects(once()).method("findService").with(eq("interfaceName equals " + SLAContainerAdminMBean.class.getName())).will(returnValue(new ArrayList<ServiceInfo>()));
		aggSpace.expects(once()).method("unregisterEventListener").with(ANYTHING);
		eventWirer.run();
	}
	
	
	public void testShouldReRegisterIfFilterUpdated() throws Exception {
		mockEvents(services());
		eventWirer.run();
		lookupSpace.expects(once()).method("findService").with(eq("interfaceName equals " + SLAContainerAdminMBean.class.getName())).will(returnValue(services()));
		aggSpace.expects(once()).method("unregisterEventListener").with(ANYTHING);
		aggSpace.expects(once()).method("registerEventListener").with(ANYTHING, ANYTHING, eq("message contains BAH"), ANYTHING);
		proxyFactory.mySLAContainerMBean.filter = "BAH";
		
		eventWirer.run();
	}
	
	
	private List<ServiceInfo> services() {
		List<ServiceInfo> services = new ArrayList<ServiceInfo>();
		ServiceInfo serviceInfo = new ServiceInfo("SomeName", "tcp://someaddress:9000", SLAContainerAdminMBean.class.getName(), JmxHtmlServerImpl.locateHttpUrL(), "", "", location, "");
		services.add(serviceInfo);
		return services;
	}

}
