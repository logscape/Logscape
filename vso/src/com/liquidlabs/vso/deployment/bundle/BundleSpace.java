package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.common.net.URI;
import java.util.List;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;
import com.liquidlabs.transport.proxy.ReplayOnAddressChange;
import com.liquidlabs.vso.work.WorkAssignment;


public interface BundleSpace extends LifeCycle {
	
	public static String NAME = BundleSpace.class.getSimpleName();

	void registerBundle(Bundle bundle) throws Exception;

	void registerBundleServices(List<Service> services);
	
	void registerBundleService(Service service);

	@Cacheable
	List<String> getBundleNames(String query);
	List<Bundle> getBundles();
	Bundle getBundle(String fullBundleName);


	List<Service> getBundleServices(String bundleId);
	List<Service> getBundleServicesForQuery(String query);
	
	String[] getBundleServiceNames(String bundleId);
	
	Service getBundleService(String serviceName);

	/**
	 * @return TRUE when the request was satisfied or could be handled in BG, otherwise if the request is still pending return FALSE
	 */
	@ReplayOnAddressChange
	boolean requestServiceStart(String requestId, WorkAssignment workInfo);

	void updateBundle(Bundle bundle);

	URI getEndPoint();

	/**
	 * Returns True when > 1 allocation is outstanding
	 * @param workAssignmenet
	 * @return
	 */
	boolean isWorkAssignmentPending(WorkAssignment workAssignmenet);
	
	boolean isServiceRunning(String serviceId) ;

	void remove(String bundleId);

    @Cacheable(ttl=10)
	String getBundleXML(String bundleId);

	String getBundleStatus(String bundleId);

	void bounceSystem();

}
