package com.liquidlabs.vso.deployment;

import com.liquidlabs.common.Logging;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.deployment.bundle.*;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class BundleDeploymentService implements DeploymentService {

    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "BundleDeploymentService");

	public static final String TAG = "DEPLOYMENT_SERVICE";
	private static final String BOOT = System.getProperty("vscape.boot.bundle.name", "boot");
	private File appBundleDir = new File(VSOProperties.getDeployedBundleDir());
	private File systemBundleDir = new File(VSOProperties.getSystemBundleDir());
	private static final Logger LOGGER = Logger.getLogger(BundleDeploymentService.class);
	private SpaceService spaceService;
	private BundleHandler bundleHandler;
	private Map<String, DeploymentListener> listeners = new ConcurrentHashMap<String, DeploymentListener>();
	
	private Map<String, String> failures = new ConcurrentHashMap<String, String>();
	
	public BundleDeploymentService() {
	}

	public BundleDeploymentService(SpaceService spaceService, BundleHandler bundleHandler, ExecutorService executor) {
		this.spaceService = spaceService;
		this.bundleHandler = bundleHandler;
	}
	
	public void start() {
		spaceService.start(this,"boot-1.0");
		log("- has started");
	}

	public void deployAllBundles() {
		log("Started Deploying All Bundles");
		deployBundles(systemBundleDir.getPath(), false);
		deployBundles(appBundleDir.getPath(), true);
		log("Completed Deploying All Bundles");

	}
	
	private void deployBundles(final String dirToScan, boolean checkForDeployFlag) {
		log("deployBundles:" + dirToScan);
		
		File scanDir = new File(dirToScan);
		if (!scanDir.exists()) {
			LOGGER.warn(String.format("Nothing to deploy on dir %s - aborting request", dirToScan));
			return;
		}
		File[] bundleDirs = scanDir.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isDirectory() && !file.getName().equals(BOOT);
		}});
		if (bundleDirs == null || bundleDirs.length == 0) {
			log("WARN = didnt find bundles in dir:" + new File(dirToScan).getAbsolutePath());
			return;
		}
		else {
			log("Deploying bundleCount:" + bundleDirs.length);
		}
		int count = 0;
		for (final File bundleDir : bundleDirs) {
			try {
				log("\t\t====================== Deploy:" + bundleDir.getAbsolutePath() + " item:" + count++ + " of " + bundleDirs.length);
				if (checkForDeployFlag) {
					if (!new File(bundleDir, "DEPLOYED").exists()) {
						log("Not Deploying:" + bundleDir.getAbsolutePath() + " did not find DEPLOYED flag");
						continue;
					}
				}
				int index = bundleDir.getName().lastIndexOf("-");
				final String name = bundleDir.getName().substring(0, index);
				log(String.format("\t\t\t ============================= deploying bundle %s : %s", bundleDir.getAbsolutePath(), name));
				String bundleFilename = bundleDir.getAbsolutePath() + "/" + name + ".bundle";
				Bundle bundle = new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle(bundleFilename);
				if (bundle == null) {
					throw new RuntimeException(String.format("Failed to find bundle:%s after unpacking", bundleDir.getAbsolutePath()));
				}
				deployBundle(bundleDir.getAbsolutePath(), bundle);
			} catch (Throwable t) {
				LOGGER.warn("Failed to deploy bundle" + bundleDir.getAbsolutePath());
			}
		}
	
	}

	public String deployBundle(final String bundleDirAbsolutePath, Bundle bundle) {
		String hash = FileUtil.readAsString(new File(bundleDirAbsolutePath, "vs.hash").getAbsolutePath());
		if (hash == null) {
			LOGGER.warn("Cannot deploy:" + bundleDirAbsolutePath + " because vs.hash was not found");
		} else {
			deploy(bundle.getId(), hash, bundle.isSystem());
		}
		return "done";
	}

	private void log(String msg) {
		LOGGER.info(TAG + " " + msg);
	}

	private void sleep() {
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
		}
	}

	public void stop() {
		spaceService.stop();
	}
	
	public void deploy(String bundleId, String bundleZipHash, boolean isSystem) {
		
		File bundleDir = isSystem ? systemBundleDir : appBundleDir;
		String bundleDescriptorPath = bundleDescriptorPath(bundleDir, bundleId);
		
		if (bundleDescriptorPath != null && shouldDeploy(bundleZipHash, bundleId, bundleDescriptorPath)) {
			
			log(String.format("LS_EVENT:Deploying path:%s %s", bundleDir.getPath(), bundleId));
			
			try {
								
				bundleHandler.deployBundle(bundleDescriptorPath, bundleId, workingDirectory(bundleDir, bundleId));
				
				log(String.format("- Bundle %s has been deployed", bundleId));
				
				fireDeployed(bundleId, bundleZipHash);
				
				writeDEPLOYEDFile(bundleDescriptorPath, true);
				failures.remove(bundleId);
			} catch (Throwable t) {
				log(String.format("- Bundle %s had error with:%s, Going to Retry", bundleId, t.toString()));
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {}
				try {
					bundleHandler.deployBundle(bundleDescriptorPath, bundleId, workingDirectory(bundleDir, bundleId));
					writeDEPLOYEDFile(bundleDescriptorPath, true);
					log(String.format("- Bundle %s has been deployed bundle", bundleId));
					fireDeployed(bundleId, bundleZipHash);
					failures.remove(bundleId);
				} catch (Throwable t2) {
					spaceService.remove(Deployed.class, bundleZipHash);
					LOGGER.error(String.format("- Bundle %s Failed to deploy", bundleId), t);
					fireFailed(bundleId, bundleZipHash, t2);
					failures.put(bundleId, new Date().toString() + " ex:" + t.toString());
				}
				
			}
		} else {
            if (bundleDescriptorPath != null) LOGGER.info("Already Deployed:" + bundleId);
            else {
                auditLogger.emit("Status", "No_Tasks_to_run");
                LOGGER.info("No Tasks to run");
            }
		}
	}

	private void writeDEPLOYEDFile(String bundleDescriptorPath, boolean create) {
		File deployedFile = new File(new File(bundleDescriptorPath).getParent(),"DEPLOYED");
		
		try {
			if (create){
				LOGGER.info("Writing DEPLOYED FILE:" + bundleDescriptorPath);
				deployedFile.createNewFile();
			}
			else {
				LOGGER.info("Deleting DEPLOYED FILE:" + bundleDescriptorPath);
				deployedFile.delete();
			}
		} catch (Exception e) {
			LOGGER.error("Failed to Write DEPLOYED file:" + e.toString() + " PATH:" + deployedFile.getAbsolutePath());
		}
	}
	public String getBundleName(String bundleName) {
		if (bundleName.indexOf(".") == -1) return null;
		bundleName = bundleName.substring(0, bundleName.lastIndexOf("."));
		return bundleName;
	}
	public String getFailure(String bundleId) {
		return failures.get(bundleId) == null ? "" : " error:" + failures.get(bundleId);
	}

	private void fireDeployed(String bundleName, String hash) {
		Set<String> failed = new HashSet<String>();
		for (Map.Entry<String, DeploymentListener> entry : new HashMap<String, DeploymentListener>(listeners).entrySet()) {
			try {
				entry.getValue().successfullyDeployed(bundleName, hash);
			} catch (Throwable t) {
				failed.add(entry.getKey());
			}
		}
		removeListeners(failed);
	}
	
	private void fireFailed(String bundleName, String hash, Throwable t) {
		Set<String> failed = new HashSet<String>();
		for (Map.Entry<String, DeploymentListener> entry : new HashMap<String, DeploymentListener>(listeners).entrySet()) {
			try {
				entry.getValue().errorDeploying(bundleName, hash, t.toString());
			} catch (Throwable t2) {
				failed.add(entry.getKey());
			}
		}
		removeListeners(failed);
	}

	private void fireUndeployed(String bundleName, String hash) {
		Set<String> failed = new HashSet<String>();
		for (Map.Entry<String, DeploymentListener> entry : new HashMap<String, DeploymentListener>(listeners).entrySet()) {
			try {
				entry.getValue().unDeployed(bundleName, hash);
			} catch (Throwable t2) {
				failed.add(entry.getKey());
			}
		}
		removeListeners(failed);
	}
	
	private void removeListeners(Set<String> failed) {
		for (String key : failed) {
			listeners.remove(key);
		}
	}
	public void addDeploymentListener(DeploymentListener listener) {
		listeners.put(listener.getId(), listener);
	}
	
	public void removeDeploymentListener(DeploymentListener listener) {
		listeners.remove(listener.getId());
	}
	
	
	public void undeploy(String bundleId, String hash) {
		log(String.format("LS_EVENT:Undeploy :- undeploy trying to remove bundle[%s] Hash[%s]", bundleId, hash));
		Deployed deployed = spaceService.findById(Deployed.class, hash);
		if (deployed == null) {
			log("WARN Didnt find DeployedBundle:" + bundleId);
			List<Deployed> deployedItems = spaceService.findObjects(Deployed.class, "", false, -1);
			for (Deployed deployed2 : deployedItems) {
				log(String.format("WARN: Deployed file[%s] hash[%s]", deployed2.name, deployed2.hash));
			}
			bundleHandler.undeployBundle(bundleId);
			return;
		}
		
		boolean isLocatedOnSystemBundleDir = new File(systemBundleDir, getBundleName(bundleId) + ".bundle").exists();
		File bundleDir = isLocatedOnSystemBundleDir ? systemBundleDir : appBundleDir;
		String bundleDescriptorPath = bundleDescriptorPath(bundleDir, bundleId);
		writeDEPLOYEDFile(bundleDescriptorPath, false);
		
		bundleHandler.undeployBundle(bundleId);
		spaceService.remove(Deployed.class, hash);
		log(String.format("LS_EVENT:RemovedDeployment - removed deployment for[%s]", bundleId));
		fireUndeployed(bundleId, hash);
	}
	
	private synchronized boolean shouldDeploy(String hash, String bundleId, String bundlePath) {
		Deployed deployed = spaceService.findById(Deployed.class, hash);
		if (deployed != null && !isDeployedFlag(bundlePath)) {
				deployed = null;
		}
		if (deployed == null) {
			log("Create Deployed:" + bundleId + "/" + hash);
			spaceService.store(new Deployed(hash, bundleId), -1);
			return true;
		}
		return false;
	}
	
	private boolean isDeployedFlag(String bundleDescriptorPath) {
		File deployedFile = new File(new File(bundleDescriptorPath).getParent(),"DEPLOYED");
		return deployedFile.exists();
	}

	public String bundleDescriptorPath(File bundleDir, String bundleId) {
//		int lastDash = bundleId.lastIndexOf("-");
//		if (lastDash == -1) lastDash = bundleId.length();
//		String firstTry = bundleDir.getPath() + "/" + bundleId + "/" + bundleId.substring(0, lastDash) + ".bundle";
//		if (new File(firstTry).exists()) return firstTry;
		
		// fall back to find anything with bundle
		String[] list = new File(bundleDir.getPath() + "/" + bundleId).list(new FilenameFilter(){ 
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".bundle");
			}
		} );
		if (list.length >  0 || list != null) return bundleDir.getPath() + "/" + bundleId + "/" + list[0];

        auditLogger.emit("Failed","BundleFileNotFound path:" +bundleDir.getPath() + "/" + bundleId);
        auditLogger.emit("BundleZip","Invalid Structure -  path:" +bundleDir.getPath() + "/" + bundleId);

		return null;//throw new RuntimeException("Failed to Find *.bundle for BundleId:" + bundleDir.getAbsolutePath() + " id:" + bundleId + " from PATH: " + bundleDir.getAbsolutePath() + "/" + bundleId);
	}

	public String workingDirectory(File bundleDir, String bundleName) {
		String defaultBundleDir = bundleDir.getName() + "/" + bundleName;
		return defaultBundleDir;
	}
	
	public static void main(String[] args) {
		try {
			BundleDeploymentService.boot(args[0]);
		} catch (Throwable t) {
			LOGGER.error(t.toString(), t);
			throw new RuntimeException(t.toString() ,t);
		}
	}
	
	public static DeploymentService getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
		return SpaceServiceImpl.getRemoteService(whoAmI, DeploymentService.class, lookupSpace, proxyFactory, BundleDeploymentService.NAME, true, false);
	}

	
	public static BundleDeploymentService boot(String lookupAddress) throws Exception {
		return BundleDeploymentService.run(lookupAddress);
	}
	
	public static BundleDeploymentService run(String lookupAddress) throws URISyntaxException {
		try {
			return runMe(lookupAddress);
		} catch (Throwable t) {
			return runMe(lookupAddress);
		}
	}
	
	private static BundleDeploymentService runMe(String lookupAddress) {
		ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();
		LookupSpace lookup = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(),"BundleDS");
		
		BundleHandler bundleHandler = new BundleHandlerImpl(SpaceServiceImpl.getRemoteService("BundleDeployService", BundleSpace.class, lookup, mapperFactory.getProxyFactory(), BundleSpace.NAME, true, false));
		SpaceServiceImpl spaceService = new SpaceServiceImpl(lookup, mapperFactory, DeploymentService.NAME, mapperFactory.getScheduler(), true, false, true);
		BundleDeploymentService bundleDeploymentService = new BundleDeploymentService(spaceService, bundleHandler, mapperFactory.getExecutor());
		bundleDeploymentService.start();
		return bundleDeploymentService;
	}
}
