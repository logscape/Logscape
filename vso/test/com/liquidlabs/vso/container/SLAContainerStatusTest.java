package com.liquidlabs.vso.container;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.SLA;
import com.liquidlabs.vso.container.sla.SLAValidator;
import com.liquidlabs.vso.container.sla.TimePeriod;
import com.liquidlabs.vso.container.sla.Variable;
import com.liquidlabs.vso.monitor.Metrics;
import com.liquidlabs.vso.monitor.MonitorSpace;
import com.liquidlabs.vso.resource.AllocListener;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.jmock.Expectations;
import org.jmock.Mockery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class SLAContainerStatusTest extends TestCase {
	
	Mockery context = new Mockery();

	private ResourceSpace resourceSpaceMock;
	private MonitorSpace monitorSpaceMock;
	private Consumer consumerMock;
	private SLA slaMock;
	private ProxyFactory proxyFactory;
	private SLAContainer container;
	private ResourceAgent resourceAgent;
	private WorkAllocator workAllocatorMock;

	GenericMetric consumerCount = new GenericMetric("consumerUsed", 5);
	GenericMetric allocCount = new GenericMetric("consumerAlloc", 0);
	Metric[] metrics = new Metric[] { new PretendMetric("foo", 10.0), consumerCount, allocCount  };
	
	class AddAction implements Action {

		boolean performed;

		public void perform(ResourceSpace resourceSpace, String consumerId, Consumer consumer, String workIntent,
				String fullBundleName, SLAContainer slaContainer) {
			performed = true;
		}
		public void setPriority(int priority) {
		}
		public void setResourceGroups(List<String> resourceGroups) {
		}
		public void setLabel(String label) {
		}

	}
	
	@SuppressWarnings("unchecked")
//	protected void abomination() throws Exception {
//		System.setProperty("validation.disabled", "true");
//
//		resourceSpaceMock = context.mock(ResourceSpace.class);
//		monitorSpaceMock = context.mock(MonitorSpace.class);
//		consumerMock = context.mock(Consumer.class);
//		slaMock = context.mock(SLA.class);
//		workAllocatorMock = context.mock(WorkAllocator.class);
//		proxyFactory = context.mock(ProxyFactory.class);
//		resourceAgent = context.mock(ResourceAgent.class);
//
//		final ArrayList<Variable> variables = new ArrayList<Variable>();
//		variables.add(new Variable("a", "b"));
//
//		context.checking(new Expectations() {{
//			atLeast(1).of(resourceSpaceMock).registerAllocListener(with(any(AllocListener.class)), with(any(String.class)), with(any(String.class)));
//			atLeast(1).of(resourceSpaceMock).renewAllocLeasesForOwner(with(any(String.class)), with(any(Integer.class)));
//			atLeast(0).of(resourceSpaceMock).unregisterAllocListener(with(any(String.class)));
//			atLeast(0).of(resourceSpaceMock).getResourceCount(with(any(String.class))); will(returnValue(1));
//			atLeast(1).of(monitorSpaceMock).write(with(any(Metrics.class)));
//			one(proxyFactory).makeRemoteable(with(any(Remotable.class))); will(returnValue(null));
//
//		}});
//		context.checking(new Expectations() {{
//			one(slaMock).setScriptLogger(with(any(Logger.class)));
//			atLeast(1).of(slaMock).getVariables(); will(returnValue(variables));
//			atLeast(1).of(slaMock).currentPriority(with(any(Integer.class))); will(returnValue(10));
//			atLeast(1).of(slaMock).getConsumerClass(); will(returnValue(DumbConsumer.class.getName()));
//			atLeast(1).of(slaMock).getTimePeriods(); will(returnValue(new ArrayList<TimePeriod>()));
//		}}
//		);
//		context.checking(new Expectations() {{
//			one(consumerMock).setVariables(with(any(java.util.Map.class)));
//			one(consumerMock).setInfo(with(any(String.class)), with(any(String.class)), with(any(String.class)));
//			one(consumerMock).getUI(); will(returnValue(null));
//			one(consumerMock).collectMetrics(); will(returnValue(metrics));
//			one(consumerMock).getUsedResourceCount(); will(returnValue(1));
//		}}
//		);
//		context.checking(new Expectations() {{
//			atLeast(1).of(proxyFactory).getScheduler(); will(returnValue(Executors.newScheduledThreadPool(2)));
//			one(proxyFactory).registerMethodReceiver(with(any(String.class)), with(anything()));
//		}}
//		);
////		proxyFactory.stubs();
//
//		container = new SLAContainer("resourceId", "serviceToRun", new WorkAssignment(),
//				"consumerName", consumerMock, slaMock,
//				"sla.xml", resourceSpaceMock, workAllocatorMock, monitorSpaceMock, new URI("localhost"),
//				"", "bundleName", null, proxyFactory, resourceAgent, new SLAValidator());
//
//		System.out.println(" ================================= starting:"  + getName());
//	}
    public void testDoesSquatCozThisDoesntWorkOnTeamCity() {}

	public void xtestShouldPerformAddAction() throws Exception {

		final AddAction addAction = new AddAction();
		context.checking(new Expectations() {{
			atLeast(1).of(consumerMock).getUsedResourceCount(); will(returnValue(1));
			one(consumerMock).collectMetrics(); will(returnValue(metrics));
			one(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(returnValue(addAction));
			atLeast(1).of(consumerMock).getReleasedResources(); will(returnValue(new ArrayList<String>()));
		}}
		);

		container.run();
		
		assertTrue("AddAction was not performed!", addAction.performed);
	}
	
	public void xtestShouldSetRUNNINGAlertStatus() throws Exception {

		final Metric[] metrics = new Metric[] { new PretendMetric("foo", 10.0) };
		final AddAction addAction = new AddAction();
		
		context.checking(new Expectations() {{
			atLeast(1).of(consumerMock).collectMetrics(); will(returnValue(metrics));
			atLeast(1).of(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(returnValue(addAction));
			atLeast(1).of(consumerMock).getUsedResourceCount(); will(returnValue(1));
			atLeast(1).of(consumerMock).getReleasedResources(); will(returnValue(new ArrayList<String>()));
		}}
		);

		container.run();
		container.run();
		container.run();
		container.run();
		
		assertEquals(LifeCycle.State.RUNNING, container.getServiceStatus().status());
	}
	
	public void xtestShouldSetWARNAlertStatusAndKeepsRunning() throws Exception {
		
		final Metric[] metrics = new Metric[] { new PretendMetric("foo", 10.0) };
		
		context.checking(new Expectations() {{
			atLeast(1).of(consumerMock).collectMetrics(); will(returnValue(metrics));
			atLeast(1).of(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(returnValue(new AddAction()));
			// RETURNING 100 here will cause an error because delta of used and allocated is > 5
			atLeast(1).of(consumerMock).getUsedResourceCount(); will(returnValue(100));
			atLeast(1).of(consumerMock).getReleasedResources(); will(returnValue(new ArrayList<String>()));
			atLeast(1).of(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class)));

		}}
		);
		
		container.run();
		container.run();
		container.run();
		container.run();
		
		assertEquals(LifeCycle.State.WARNING, container.getServiceStatus().status());
	}
	
	public void xtestShouldSetERRORAlertStatusANDStopsRunning() throws Exception {
		
		final Metric[] metrics = new Metric[] { new PretendMetric("foo", 10.0) };
		
		context.checking(new Expectations() {{
			one(consumerMock).collectMetrics(); will(returnValue(metrics));
			atLeast(1).of(consumerMock).getUsedResourceCount(); will(returnValue(100));
			one(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(throwException(new RuntimeException("Something went wrong")));
			one(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class))); will(throwException(new RuntimeException("Cause ERROR STATE")));
			one(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class)));

		}}
		);
		
		container.getServiceStatus().setRunningTime(0);
		
		container.run();
		assertEquals(LifeCycle.State.WARNING, container.getServiceStatus().status());
		container.run();
//		assertEquals(LifeCycle.State.ERROR, container.getServiceStatus().status());
//		container.run();
//		assertEquals(LifeCycle.State.ERROR, container.getServiceStatus().status());
//		container.run();
//		assertEquals(LifeCycle.State.ERROR, container.getServiceStatus().status());
	}
	
	public void xtestShouldRecoverFromWARNState() throws Exception {
	
		final Metric[] metrics = new Metric[] { new PretendMetric("foo", 10.0) };
		
		context.checking(new Expectations() {{
			atLeast(1).of(consumerMock).getReleasedResources(); will(returnValue(new ArrayList<String>()));
			
			one(consumerMock).collectMetrics(); will(returnValue(metrics));
			one(consumerMock).collectMetrics(); will(returnValue(metrics));
			atLeast(1).of(consumerMock).getUsedResourceCount(); will(returnValue(5));
			
			one(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(throwException(new RuntimeException("Something went wrong")));
			one(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(returnValue(new AddAction()));
			one(slaMock).getRule(with(any(Integer.class))); will(returnValue(new Rule("", 10, 10)));

			one(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class)));
			one(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class)));
			one(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class)));
		}}
		);

		container.getServiceStatus().setRunningTime(0);
		
		container.run();
		
		assertEquals(LifeCycle.State.WARNING, container.getServiceStatus().status());
		
		System.out.println("RESUME NORMAL OPERATION----------------");
		
		context.checking(new Expectations() {{
			one(consumerMock).collectMetrics(); will(returnValue(metrics));
			one(consumerMock).collectMetrics(); will(returnValue(metrics));
			one(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(returnValue(new AddAction()));
			one(slaMock).evaluate(with(any(Integer.class)), with(any(Consumer.class)), with(any(Metric[].class))); will(returnValue(new AddAction()));
			one(consumerMock).getReleasedResources(); will(returnValue(new ArrayList<String>()));
			one(consumerMock).getReleasedResources(); will(returnValue(new ArrayList<String>()));
			one(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class)));
			one(resourceAgent).updateStatus(with(any(String.class)), with(any(LifeCycle.State.class)), with(any(String.class)));
		}}
		);
		
		container.run();
		container.run();
		
		assertEquals(LifeCycle.State.RUNNING, container.getServiceStatus().status());
	
	}

}
