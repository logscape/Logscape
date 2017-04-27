package com.liquidlabs.log.sla;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.log.space.AggSpaceImpl;
import com.liquidlabs.log.space.LogEvent;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.LogSpaceImpl;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.FunctionalTestBase;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.container.SLAContainerAdminMBean;
import com.liquidlabs.vso.lookup.ServiceInfo;


public class SLAEventFunctionalTest extends FunctionalTestBase{

	public static class MySLAAdmin implements SLAContainerAdminMBean {

		private List<String> messages = new ArrayList<String>();
		String filter = "MYSLA";
		
		public String displaySLAContent() {
			return null;
		}

		public String getNotifyFilter() {
			return filter;
		}

		public void handleLogEvent(String message) {
			messages.add(message);
		}

		public String triggerSLAAction(String script, int priority) {
			return null;
		}

		public String updateSLA(String sla) {
			return null;
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
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1);
	LogSpace logSpace;
	private AggSpaceImpl aggSpace;
	String location = "UNKNOWN";

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		ORMapperFactory mapperFactory = new ORMapperFactory();
		ProxyFactoryImpl proxyFactory = mapperFactory.getProxyFactory();
		logSpace = new LogSpaceImpl(new SpaceServiceImpl(lookupSpace, new ORMapperFactory(), LogSpace.NAME, schedule, false, false, true),
									null, null, null, null, null, null, resourceSpace, lookupSpace);
		
		SpaceServiceImpl bucketService = new SpaceServiceImpl(lookupSpace, mapperFactory, "AGGSPACE", proxyFactory.getScheduler(), false, false, false);
		SpaceServiceImpl replayService = new SpaceServiceImpl(lookupSpace, mapperFactory, "AGGSPACE", proxyFactory.getScheduler(), false, false, false);
		SpaceServiceImpl logEventSpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, "AGGSPACE", proxyFactory.getScheduler(), false, false, true);
		
		aggSpace = new AggSpaceImpl("providerId", bucketService, replayService, logEventSpaceService, proxyFactory.getScheduler());
		aggSpace.start();
	}
	
	@Override
	protected void tearDown() throws Exception {
		try {
			logSpace.stop();
			aggSpace.stop();
		} catch(Exception e) {}
		schedule.shutdown();
		super.tearDown();
	}
	
	public void testShouldReceiveMessages() throws Exception {
		logSpace.start();
		schedule.scheduleWithFixedDelay(new SLAEventWirer(lookupSpace, logSpace, proxyFactories.get(0), aggSpace), 0, 1, TimeUnit.SECONDS);
	
		lookupSpace.registerService(new ServiceInfo("MYSLA", proxyFactories.get(0).getAddress().toString(), SLAContainerAdminMBean.class.getName(), JmxHtmlServerImpl.locateHttpUrL(), "boot", "", location, ""), -1);
		MySLAAdmin admin = new MySLAAdmin();
		proxyFactories.get(0).registerMethodReceiver("MYSLA", admin);
		Thread.sleep(1500);
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 1", "foo", "file", 1, 1, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 2", "foo", "file", 2, 2, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 3", "foo", "file", 3, 3, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 4", "foo", "file", 4, 4, ""));
		Thread.sleep(500);
		assertEquals(4, admin.messages.size());
					
	}
	
	public void testShouldStopReceivingMessagesWhenFilterSetToNull() throws Exception {
		logSpace.start();
		schedule.scheduleWithFixedDelay(new SLAEventWirer(lookupSpace, logSpace, proxyFactories.get(0), aggSpace), 0, 1, TimeUnit.SECONDS);
	
		lookupSpace.registerService(new ServiceInfo("MYSLA", proxyFactories.get(0).getAddress().toString(), SLAContainerAdminMBean.class.getName(), JmxHtmlServerImpl.locateHttpUrL(), "boot", "", location, ""), -1);
		MySLAAdmin admin = new MySLAAdmin();
		proxyFactories.get(0).registerMethodReceiver("MYSLA", admin);
		Thread.sleep(1500);
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 1", "foo", "file", 1, 1, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 2", "foo", "file", 2, 2, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 3", "foo", "file", 3, 3, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 4", "foo", "file", 4, 4, ""));
		Thread.sleep(500);
		admin.filter = null;
		admin.messages.clear();
		Thread.sleep(1500);
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 1", "foo", "file", 1, 1, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 2", "foo", "file", 2, 2, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 3", "foo", "file", 3, 3, ""));
		aggSpace.write(new LogEvent("sourceURI", "MYSLA - 4", "foo", "file", 4, 4, ""));
		assertEquals(0, admin.messages.size());
					
	}
}
