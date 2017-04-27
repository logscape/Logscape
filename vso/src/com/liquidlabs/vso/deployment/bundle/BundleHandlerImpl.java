package com.liquidlabs.vso.deployment.bundle;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.container.PercentConsumer;
import com.liquidlabs.vso.deployment.BundleDeploymentService;
import com.liquidlabs.vso.deployment.bundle.Bundle.Status;
import com.liquidlabs.vso.work.WorkAssignment;

/**
 *
 */
public class BundleHandlerImpl implements BundleHandler {
	private static final Logger LOGGER = Logger.getLogger(BundleHandlerImpl.class);
	private final BundleSpace bundleSpace;
	Map<String, String> variables = new ConcurrentHashMap<String, String>();
	DateTimeFormatter dateFormatter = DateTimeFormat.mediumDate();
	

	public BundleHandlerImpl(BundleSpace bundleSpace) {
		this.bundleSpace = bundleSpace;
	}
	public Bundle loadBundle(String filenameOnClassPath) {
		return new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle(filenameOnClassPath);
	}


	/* (non-Javadoc)
	 * @see com.liquidlabs.vso.deployment.bundle.BundleHandler#loadBundleF(java.lang.String)
	 */
	public Bundle loadBundleF(String fileName, String bundleId) {
		BundleSerializer serializer = new BundleSerializer(new File(VSOProperties.getDownloadsDir()));
		Bundle bundle = serializer.loadBundle(fileName);
		bundle.setBundleIdFromZip(bundleId + ".zip");
		serializer.postProcessServices(bundle);
		return bundle;
	}
	/* (non-Javadoc)
	 * @see com.liquidlabs.vso.deployment.bundle.BundleHandler#install(com.liquidlabs.vso.deployment.bundle.Bundle)
	 */
	public void install(Bundle bundle) throws Exception {
		List<Service> services = bundle.getServices();
		// make a bundle entry for this bundle
		bundle.setInstallDate(dateFormatter.print(new DateTime()));
		bundle.setStatus(Status.INSTALLED);
		this.bundleSpace.registerBundle(bundle);
		
		// make a service entry each service
		for (Service service : services) {
			this.bundleSpace.registerBundleService(service);			
		}
	}
	/**
	 * Deploys services, starts the bundle
	 * @param bundleId
	 */
	public void deploy(String bundleId, String workingDirectory){
		
		
		LOGGER.info("======================= DEPLOYING BUNDLE:" + bundleId + " ======================= ");
		
		try {
			LOGGER.info(String.format("WorkingDirectory: %s absPath %s", workingDirectory, new File(workingDirectory).getAbsolutePath()));
			Bundle bundle = getBundle(bundleId);
			
			if (bundle == null) {
				LOGGER.error("\n\t\t****Failed to locate Bundle Object for:" + bundleId);
				return;
			}
			
			bundle.setStatus(Status.STARTING);
			
			this.bundleSpace.updateBundle(bundle);
			List<Service> bundleServices = bundle.getServices();
			
			LOGGER.info("BH_Starting services:" + bundleServices);
			WorkAssignment lastFGWorkAssignment = null;
			for (Service service : bundleServices) {
				try {
					LOGGER.info("  ===== BH_Handling:" + service);
					WorkAssignment workInfo = getWorkAssignmentFromService(service, bundle.isSystem());
					String requestId = "BH_DPLY_" + service.getId();
					
					workInfo.setResourceId(requestId);
					workInfo.setWorkingDirectory(workingDirectory);
					
					LOGGER.info(requestId + "  ===== BH_Loading:" + workInfo.getId());
					
					if (workInfo.getAllocationsOutstanding() == 0 && !workInfo.isBackground()) {
						LOGGER.info(requestId +" BH_WorkAssignment - AllocCount:0 Nothing to do:" + workInfo.getId() + "....");
						continue;
					}
					if (service.hasDependencies()) {
						waitOnDependencies(service);
					}
					LOGGER.info(requestId + "  ===== BH_Starting:" + workInfo.getId() + " BundleSpace:" + bundleSpace);
					bundleSpace.requestServiceStart(requestId, workInfo);
					
					// pause to allow for work allocation and variables to be updated
					if (workInfo.isBackground()) {
						LOGGER.info(requestId + " BH_Pause for BG WorkAssignment:" + workInfo + "....");
						pause(1);
						continue;
					}
					
					lastFGWorkAssignment = workInfo;
					LOGGER.info(requestId + " BH_Waiting for WorkAssignment:" + workInfo + "....");
					int count = 0;
					while (count++ < 3 && bundleSpace.isWorkAssignmentPending(workInfo)) {
	//					pause(workInfo.getPauseSeconds());
						pause();
					}
					if (bundleSpace.isWorkAssignmentPending(workInfo)) {
						LOGGER.info(requestId + " **** Finished Waiting, it will be handled by RegistrationListeners for WorkAssignment:" + workInfo + "....");
					} else { 
						LOGGER.info(requestId + " ****************** WorkAssignment DEPLOYED:" + workInfo + "....");
					}
				} catch (Throwable t) {
					LOGGER.error("Failed to Start:" + service, t);
				}
			}
			// wait for start event on bundle
			bundle.setStatus(Status.ACTIVE);
			this.bundleSpace.updateBundle(bundle);
			LOGGER.debug("======================= BUNDLE:" + bundleId + " is ACTIVE ======================= ");
			pause(lastFGWorkAssignment == null ? 0 : lastFGWorkAssignment.getPauseSeconds());
			
		} catch (Throwable t){
			LOGGER.error("FAILED to deploy bundle:" + bundleId, t);
		}
	}
	private Bundle getBundle(String bundleId) {
		Bundle bundle = this.bundleSpace.getBundle(bundleId);
		LOGGER.info("Using bundlespace:" + bundleSpace );
		return bundle;
	}
	
	private void waitOnDependencies(Service service) {
		String[] dependencies = service.getDependencies();
		List<String> deps = new ArrayList<String>(Arrays.asList(dependencies));
		int waitCount = 0;
		while(deps.size() > 0 && waitCount++ < service.getDependencyWaitCount()) {
			if(!bundleSpace.isServiceRunning(deps.get(0))) {
				LOGGER.info(String.format("Service %s is waiting for %s to start, waitLimit[%d]", service.getId(), deps.get(0), service.getDependencyWaitCount()));
				pause();
				pause();
				pause();

			} else {
				deps.remove(0);
			}
		}
		if (deps.size() > 0) {
			LOGGER.warn(String.format("Service[%s] - timed-out waiting for serviceDependency[%s]", service.getId(), deps.get(0)));
		}
	}
	
	
	public WorkAssignment getWorkAssignmentForService(String serviceId) {
		LOGGER.info("getWorkAssignmentForService (serviceId):" + serviceId);
		Service service = this.bundleSpace.getBundleService(serviceId);
		boolean isSystemBundle = this.bundleSpace.getBundle(service.getFullBundleName()).isSystem();
		if (service == null) {
			throw new RuntimeException("Failed to find service[" + serviceId + "]");
		}
		if (!service.isSimpleCount()) service.setInstanceCount("1");
		return getWorkAssignmentFromService(service, isSystemBundle);
	}	

	public WorkAssignment getWorkAssignmentFromService(Service service, boolean isSystem) {
		if (service.isSimpleCount()) {
			LOGGER.info("getWorkAssignmentForService (service):" + service.getId());
			WorkAssignment workInfo = new WorkAssignment("noResourceId", "noResourceId-0", 0, service.getBundleId(), service.getName(), service.getScript(), service.getPriority());
			workInfo.setProperty(service.getProperty());
			workInfo.setBackground(service.isBackground());
			String resourceSelection = service.getResourceSelection();
			if (service.isBackground()) {
				if (resourceSelection.trim().equals("")) {
					resourceSelection = "workId notContains " + service.getId();
				} else {
					resourceSelection += " AND workId notContains " + service.getId();
				}
			}
			workInfo.setResourceSelection(resourceSelection);
			workInfo.setPauseSeconds(service.getPauseSeconds());
			workInfo.setFork(service.isFork());
			workInfo.setAllocationsOutstanding(service.getInstanceCountAsInteger());
			workInfo.setSlaFilename(service.getSlaFilename());
			workInfo.setSystemService(service.isSystem());
			workInfo.setOverridesService(service.getOverridesService());
			workInfo.setCostPerUnit(service.getCostPerUnit());
			return workInfo;
		} else {
			return PercentConsumer.getSLAContainerWorkAssignmentForService(service, isSystem);
		}
	}
	
	private void pause(int i) {
		try {
			int j = i * 1000;
			if (j == 0) j = 100;
			Thread.sleep(j);
		} catch (InterruptedException e) {
		}
	}
	private void pause() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
	}
	

	public void deployBundle(String bundleDescriptorPath, String bundleId, String workingDirectory) {
		try {
			LOGGER.info("Deploying bundle: " + bundleId + " defaultWorkingDir:" + workingDirectory);
			
			// generally the bundle should have been unzipped when it was uploaded - however
			// if the user placed it into downloads directory it wont exist - so unpack it - any peers
			// that joined and didnt have it - will download and unpack it.
			if (!new File(bundleDescriptorPath).exists()) {
				LOGGER.info("Failed to find unpacked bundle - unpacking");
				BundleUnpacker unpacker = new BundleUnpacker(new File(VSOProperties.getSystemBundleDir()), new File(VSOProperties.getDeployedBundleDir()));
				Bundle bundle = unpacker.unpack(new File("downloads", bundleId + ".zip"), true);
				if (bundle == null) {
					throw new RuntimeException(String.format("Failed to find bundle:%s after unpacking", bundleDescriptorPath));
				}
				File bundleDir = bundle.isSystem() ? new File(VSOProperties.getSystemBundleDir()) : new File(VSOProperties.getDeployedBundleDir());
				BundleDeploymentService bds = new BundleDeploymentService();
				bundleDescriptorPath = bds.bundleDescriptorPath(bundleDir, bundleId);
				workingDirectory = bds.workingDirectory(bundleDir, bundleId);
			}
			Bundle bundle = loadBundleF(bundleDescriptorPath, bundleId);
			
			String wd = bundle.getWorkingDirectory();
			if (wd == null) wd = workingDirectory;
			install(bundle);
			deploy(bundleId, wd);
			LOGGER.info(String.format("%s deployed, wDir:%s", bundle, workingDirectory));
		} catch (Throwable t) {
			LOGGER.warn("Failed to deploy:" + bundleId + " msg:" + t.toString());
			throw new RuntimeException(t);
		}
	}
	
	public void undeployBundle(String bundleId) {
		LOGGER.info("Undeploying Bundle:" + bundleId);
		bundleSpace.remove(bundleId);		
	}
}
