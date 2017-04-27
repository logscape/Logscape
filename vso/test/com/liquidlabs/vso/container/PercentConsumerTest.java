package com.liquidlabs.vso.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.vso.container.sla.SLA;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.work.WorkAssignment;

public class PercentConsumerTest {
	
	
	private PercentConsumer consumer;
	boolean success = false;

	@Before
	public void setUp() throws Exception {
		consumer = new PercentConsumer("10%", "mflops > 1", true);
	}
	
	@Test
	public void shouldCreateWorkAssignment() throws Exception {
		List<String> args = Arrays.asList("one","two");//processMaker.getSLAArgs("one","two","-runInterval:60", "-serviceToRun:%s", "-consumerClass:%s", "-sla:%s","-consumerPercent:%s", "-serviceCriteria:%s");
		String[] argsArray = Arrays.toStringArray(args);
//		SLAContainer.main(argsArray);
		Service myService = new Service("bundle", "testService", "slaScript", "1");
		WorkAssignment workAssignment = PercentConsumer.getSLAContainerWorkAssignmentForService(myService, true);
		assertNotNull(workAssignment);
		
		System.out.println(workAssignment.getScript());
		
	}
	
	@Test
	public void testShouldGetGoodMin() throws Exception {
		assertEquals(2, consumer.getMin("10.2%"));
	}
	
	@Test
	public void testShouldGetGoodMinWithEmptyString() throws Exception {
		assertEquals(1, consumer.getMin(""));
	}
	
	@Test
	public void testShouldGetGoodMinWithCrapString() throws Exception {
		assertEquals(1, consumer.getMin("10,2%"));
	}
	@Test
	public void testShouldGetGoodPercent() throws Exception {
		assertEquals(20, consumer.getPercent("20.2%"));
	}
	@Test
	public void testShouldGetGoodPercentWithoutDot() throws Exception {
		assertEquals(20, consumer.getPercent("20%"));
	}
	
	@Test
	public void testShouldGetGoodPercentWithTooBugNumber() throws Exception {
		assertEquals(100, consumer.getPercent("200%"));
	}
	
	@Test
	public void testShouldGetSLA() throws Exception {
		SLA sla = consumer.getSLA();
		assertNotNull(sla);
		assertTrue(sla.getTimePeriods().size() == 1);
		assertEquals(10, sla.getTimePeriods().get(0).getRules().get(0).getPriority());
	}
	
	@Test
	public void testShouldCountResources() throws Exception {
		AddListener addListener = new AddListener(){
			public void failed(String resourceId, String errorMsg) {
			}
			public void success(String resourceId) {
				success = true;
			}
		};
		consumer.add("one", Arrays.asList("one"), addListener);
		assertTrue(success);
		assertTrue(consumer.myResources.size() == 1);
	}

}
