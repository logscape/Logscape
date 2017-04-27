package com.liquidlabs.util;

import java.util.List;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.plot.XYPoint;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkListener;

public class WorkAccountTest extends TestCase {
	
	private static final int ONE_MIN = 60 * 1000;

	private static final String SERVICE = "GridEngine";

	private static final String APP = "matrix-1.0";

	Mockery context = new Mockery();

	private WorkAccountantImpl workAccount;

	private WorkAllocator workAllocator;

	private LookupSpace lookupSpace;

	private SpaceService spaceService;

	private ProxyFactory proxyFactory;

	private String location;

	protected void setUp() throws Exception {
		workAllocator = context.mock(WorkAllocator.class);
		lookupSpace = context.mock(LookupSpace.class);
		proxyFactory = context.mock(ProxyFactory.class);
		context.checking(new Expectations() {{
			atLeast(1).of(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class)));
			atLeast(1).of(lookupSpace).unregisterService(with(any(ServiceInfo.class)));
			atLeast(1).of(lookupSpace).renewLease(with(any(String.class)), with(any(int.class)));
			one(workAllocator).getWorkIdsForQuery(with(any(String.class))); will(returnValue(new String[0]));
			atLeast(1).of(workAllocator).registerWorkListener(with(any(WorkListener.class)), with(any(String.class)), with(any(String.class)), with(any(Boolean.class)));
			atLeast(1).of(proxyFactory).registerMethodReceiver(with(any(String.class)), with(any(Object.class)));
			atLeast(1).of(proxyFactory).getScheduler(); will(returnValue(Executors.newScheduledThreadPool(4)));
		}});
		
		ORMapperFactory mapperFactory = new ORMapperFactory(Config.TEST_PORT);
		spaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, WorkAccountant.NAME, mapperFactory.getScheduler(), false, false, true);
		workAccount = new WorkAccountantImpl(spaceService, workAllocator, new EventStoreImpl(mapperFactory, false, false), proxyFactory);
		workAccount.start();
	}
	protected void tearDown() throws Exception {
		workAccount.stop();
	}
	
	public void testShouldCalcCostFor1Hour() throws Exception {
		long oneDollar = (long) workAccount.calcCost(1000 * 60 * 60, 100);
		assertEquals(100, oneDollar);
	}
	public void testShouldCalcCostFor30Mins() throws Exception {
		long fiftyCents = (long) workAccount.calcCost(500 * 60 * 60, 100);
		assertEquals(50, fiftyCents);
	}
	
	public void testShouldCalcCostFor5Mins() throws Exception {
		long fiveMins = (long) workAccount.calcCost(1000 * 60 * 5, 100);
		assertEquals(8, fiveMins);
	}
	public void testShouldCalcCostForTwoMins() throws Exception {
		long twoMins = (long) workAccount.calcCost(1000 * 60 * 2, 100);
		assertEquals(3, twoMins);
	}
	
	
	public void testShouldAccountRealCountOnLiveItems() throws Exception {
		
		
		
		long start = System.currentTimeMillis() - ONE_MIN;
		long end = System.currentTimeMillis();
		
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", start, 10));
		
		List<XYPoint> result = workAccount.getServiceAllocationHistory(APP, SERVICE, start, end, 20);
		
		assertEquals(3, result.size());
		assertEquals(1, result.get(0).getY());
		assertEquals(1, result.get(1).getY());
		assertEquals(1, result.get(2).getY());
	}
	
	public void testShouldAccountUtilisationOnLiveItems() throws Exception {
			int sixtyMinutesMs = 60 * (1000 * 60);
			long startTimeOneHourAgo = DateTimeUtils.currentTimeMillis() - sixtyMinutesMs;
			long endTimeFuturePlus10Ms= DateTimeUtils.currentTimeMillis()  + 10;
			
			int twelveCentsPerHour = 12;
			workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTimeOneHourAgo, twelveCentsPerHour));
			
			List<XYPoint> result = workAccount.getServiceUtilHistory(APP, SERVICE, startTimeOneHourAgo, endTimeFuturePlus10Ms, ((sixtyMinutesMs/3))/1000, twelveCentsPerHour, false);
			
			// duration is 60minutes + 10ms
			assertEquals(4, result.size());
			assertEquals(4, result.get(0).getY());
			assertEquals(4, result.get(1).getY());
			assertEquals(4, result.get(2).getY());
			
			// the 10ms bucket in the future
			assertEquals(0, result.get(3).getY());
	}

	public void testShouldAccountCountOnLiveItems() throws Exception {
		int fiveMins = (1000 * 60) * 5;
		long startTime = DateTimeUtils.currentTimeMillis() - fiveMins;
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTime, 10));
		
		List<XYPoint> result = workAccount.getServiceAllocationHistory(APP, SERVICE, startTime, DateTimeUtils.currentTimeMillis()+10, 120);
		
		assertEquals(3, result.size());
		assertEquals(1, result.get(0).getY());
		assertEquals(1, result.get(1).getY());
		assertEquals(1, result.get(2).getY());
	}
	
	public void testShouldNOTAccountCountOnPostLiveItems() throws Exception {
		int fiveMins = (1000 * 60) * 5;
		long startTime = DateTimeUtils.currentTimeMillis() + fiveMins;
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTime + 1000, 10));
		
		List<XYPoint> result = workAccount.getServiceAllocationHistory(APP, SERVICE, DateTimeUtils.currentTimeMillis(), DateTimeUtils.currentTimeMillis()+(1000 * 60) * 5, 120);
		
		assertEquals(3, result.size());
		assertEquals(0, result.get(0).getY());
		assertEquals(0, result.get(1).getY());
		assertEquals(0, result.get(2).getY());
	}
	
	public void testShouldAccountUtilisationOnCompletedItems() throws Exception {
		int oneHour = (1000 * 60) * 60;
		long startTime = DateTimeUtils.currentTimeMillis() - oneHour;
		long endTime = DateTimeUtils.currentTimeMillis();
		int costPerUnit = 12;
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTime, costPerUnit));
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", endTime, costPerUnit));
		
		List<XYPoint> result = workAccount.getServiceUtilHistory(APP, SERVICE, startTime, endTime + 10, (60 / 3) * 60, costPerUnit, false);
		
		assertEquals(4, result.size());
		assertEquals(4, result.get(0).getY());
		assertEquals(4, result.get(1).getY());
		assertEquals(4, result.get(2).getY());
		assertEquals(0, result.get(3).getY());
	}
	public void testShouldAccountCountCompletedItems() throws Exception {
		long nowNearestSecond = DateTimeUtils.currentTimeMillis();
		nowNearestSecond = (nowNearestSecond - nowNearestSecond/1000) * 1000;
		int oneHour = (1000 * 60) * 60;
		int twentyMins = (1000 * 60) * 20;
		long startTime = nowNearestSecond - oneHour;
		long endTime = nowNearestSecond;
		int costPerUnit = 12;
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTime + twentyMins, costPerUnit));
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", endTime - twentyMins, costPerUnit),  endTime - twentyMins);
		
		List<XYPoint> result = workAccount.getServiceAllocationHistory(APP, SERVICE, startTime, endTime + 10, (60 / 3) * 60);
		
		assertEquals(4, result.size());
		assertEquals(0, result.get(0).getY());
		assertEquals(1, result.get(1).getY());
		assertEquals(0, result.get(2).getY());
		assertEquals(0, result.get(3).getY());
	}
	
	public void testShouldAccountUtilisationOnCompletedItems2() throws Exception {
		int oneHour = (1000 * 60) * 60;
		int twentyMins = (1000 * 60) * 20;
		long startTime = DateTimeUtils.currentTimeMillis() - oneHour;
		long endTime = DateTimeUtils.currentTimeMillis();
		int costPerUnit = 12;
		
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId2", startTime + twentyMins, costPerUnit));
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId2", startTime + twentyMins, costPerUnit), endTime - twentyMins);
		
		List<XYPoint> result = workAccount.getServiceUtilHistory(APP, SERVICE, startTime, endTime + 10, (60 / 3) * 60, costPerUnit, false);
		
		assertEquals(4, result.size());
		assertEquals(0, result.get(0).getY());
		assertEquals(4, result.get(1).getY());
		assertEquals(0, result.get(2).getY());
		assertEquals(0, result.get(3).getY());
	}
	
	public void testShouldAccountUtilisationOnMultipleCompletedItems() throws Exception {
		int oneHour = (1000 * 60) * 60;
		int twentyMins = (1000 * 60) * 20;
		long startTime = DateTimeUtils.currentTimeMillis() - oneHour;
		long endTime = DateTimeUtils.currentTimeMillis();
		int costPerUnit = 12;
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTime, costPerUnit));
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTime, costPerUnit), endTime);
		
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId2", startTime + twentyMins, costPerUnit));
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId2", startTime + twentyMins, costPerUnit), endTime - twentyMins);
		
		List<XYPoint> result = workAccount.getServiceUtilHistory(APP, SERVICE, startTime, endTime + 10, (60 / 3) * 60, costPerUnit, false);
		
		assertEquals(4, result.size());
		assertEquals(4, result.get(0).getY());
		assertEquals(8, result.get(1).getY());
		assertEquals(4, result.get(2).getY());
		assertEquals(0, result.get(3).getY());
	}
	public void testShouldCollapseDifferentServicesToSingleEvent() throws Exception {
		int oneHour = (1000 * 60) * 60;
		int twentyMins = (1000 * 60) * 20;
		long startTime = DateTimeUtils.currentTimeMillis() - (oneHour + twentyMins);
		long endTime = DateTimeUtils.currentTimeMillis();
		int costPerUnit = 12;
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE+"1", "resourceId", startTime + twentyMins, costPerUnit));
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE+"1", "resourceId", startTime + twentyMins, costPerUnit), endTime);
		
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE+"2", "resourceId2", startTime + twentyMins + twentyMins, costPerUnit));
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE+"2", "resourceId2", startTime + twentyMins + twentyMins, costPerUnit), endTime - twentyMins);

		List<AccountStatement> result = workAccount.getCostsForTime(APP, (60 / 3) * 60, startTime, endTime+10, Arrays.asList(SERVICE+"1", SERVICE+"2"), java.util.Arrays.asList((double)costPerUnit, (double)costPerUnit));
		
		assertEquals(5, result.size());
		assertEquals(0, result.get(0).getCompletedUnitCosts());
		assertEquals(4, result.get(1).getCompletedUnitCosts());
		assertEquals(8, result.get(2).getCompletedUnitCosts());
		assertEquals(4, result.get(3).getCompletedUnitCosts());
		assertEquals(0, result.get(4).getCompletedUnitCosts());
		
		
	}
	
	public void testShouldAccountCountOnCompletedItems() throws Exception {
		int fiveMins = (1000 * 60) * 5;
		long startTime = DateTimeUtils.currentTimeMillis() - fiveMins;
		workAccount.startAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", startTime, 10));
		
		DateTime endTime = new DateTime(DateTimeUtils.currentTimeMillis());
		workAccount.stopAccountEvent(new AccountEvent(APP, 10, SERVICE, "resourceId", endTime.getMillis(), 10));
		
		List<XYPoint> result = workAccount.getServiceAllocationHistory(APP, SERVICE, startTime, DateTimeUtils.currentTimeMillis()+10, 120);
		
		assertEquals(3, result.size());
		assertEquals(1, result.get(0).getY());
		assertEquals(1, result.get(1).getY());
		assertEquals(1, result.get(2).getY());
	}
}
