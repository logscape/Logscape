package com.liquidlabs.replicator.download;

import java.io.File;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.replicator.service.ReplicationServiceImpl;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.deployment.DeploymentService;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.BundleUnpacker;
import com.liquidlabs.vso.lookup.LookupSpace;

public class BundleDeployer implements DownloadListener {

	private final ResourceAgent agent;
	private static final Logger LOGGER = Logger.getLogger(BundleDeployer.class);
	private final DeploymentService deploymentService;
	private BundleUnpacker unpacker = new BundleUnpacker(new File(VSOProperties.getSystemBundleDir()), new File(VSOProperties.getDeployedBundleDir()));

	public BundleDeployer(ResourceAgent agent, DeploymentService deploymentService, File bundleDir) {
		this.agent = agent;
		this.deploymentService = deploymentService;
	}

	
	/**
	 * We should not auto-deploy files cause they are downloaded!
	 */
	@Deprecated 
	public void downloadComplete(File download, String hash) {
		if (download.getName().startsWith("."))
			return;
		if (unpacker.isBundle(download)) {
			Bundle theBundle = unpacker.unpack(download, false);
			LOGGER.info(String.format("%s - %s has been installed", ReplicationServiceImpl.TAG, theBundle.getId()));
			agent.addDeployedBundle(theBundle.getId(), theBundle.getReleaseDate());

			// only auto expand & deploy nonSystem bundles
			// cause system bundles are running and the files will be
			// locked - so need to wait for reboot where the agent auto deploys
			// them
			if (theBundle.isAutoStart() && !theBundle.isSystem()) {
				deploy(theBundle.getId(), hash, theBundle.isSystem());
			}
		} else {
			LOGGER.info(String.format("%s - %s has been downloaded, Exists:" + download.exists(), ReplicationServiceImpl.TAG, download.getName()));
		}
	}

	private void deploy(String bundleId, String hash, boolean isSystem) {
		deploymentService.deploy(bundleId, hash, isSystem);
		LOGGER.info(String.format("%s - %s has been deployed", ReplicationServiceImpl.TAG, bundleId));
	}

	public static DownloadListener create(ResourceAgent agent, LookupSpace lookup, ProxyFactory proxyFactory, String bundleDir)
			throws URISyntaxException {
		DeploymentService service = SpaceServiceImpl.getRemoteService("DeployService", DeploymentService.class, lookup, proxyFactory,
				DeploymentService.NAME, true, false);
		return new BundleDeployer(agent, service, new File(bundleDir));
	}

	public void downloadRemoved(File bundleFile) {
		String bundleName = unpacker.getBundleName(bundleFile.getName());
		try {
			unpacker.deleteExpandedBundleDir(bundleName);
		} catch (Throwable t) {
			LOGGER.warn("Failed to delete expanded bundle:" + t.toString());
		}
		agent.removeBundle(bundleName);
	}

}
