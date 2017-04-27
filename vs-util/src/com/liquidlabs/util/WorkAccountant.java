package com.liquidlabs.util;

import java.util.List;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.plot.XYPoint;
import com.liquidlabs.vso.work.WorkListener;


public interface WorkAccountant extends LifeCycle, WorkListener {
	
	String NAME = WorkAccountant.class.getSimpleName();

	void updateAccount(String bundleId, int priority, long startTime, long stopTime);

	void startAccountEvent(AccountEvent accountEvent);

	void stopAccountEvent(AccountEvent event);
	void stopAccountEvent(AccountEvent templateEvent, long endTime);

	List<AccountStatement> getCostsForTime(String appNameId, int intervalSeconds, long fromTimeMs, long toTimeMs, List<String> serviceIds,		List<Double> costPerUnit);
	AccountStatement getCurrentUnitCost(String bundleId);
	
	List<XYPoint> getServiceAllocationHistory(String appNameId, String serviceName, long fromTimeMs, long toTimeMs, int intervalSeconds);
	
	List<XYPoint> getServiceUtilHistory(String appNameId, String serviceName, long fromTimeMs, long toTimeMs, int intervalSeconds, double costPerUnit, boolean returnMsInsteadOfCost);






}
