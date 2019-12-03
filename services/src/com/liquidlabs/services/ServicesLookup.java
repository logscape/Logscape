package com.liquidlabs.services;

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.AdminSpaceImpl;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.AggSpaceImpl;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.LogSpaceImpl;
import com.liquidlabs.replicator.service.ReplicationService;
import com.liquidlabs.replicator.service.Uploader;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.deployment.DeploymentService;
import com.liquidlabs.vso.deployment.bundle.BundleSpace;
import com.liquidlabs.vso.deployment.bundle.BundleSpaceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;
import org.apache.log4j.Logger;
import org.eclipse.jetty.deploy.DeploymentManager;
import org.eclipse.jetty.webapp.WebAppContext;


/**
 * Common Class to pass proxy handles between Servlet using the WAC
 * Disable by setting -Dtest.mode = true
 *
 * @author neil
 */

/**
 * Created by neil.avery on 12/03/2016.
 */
public class ServicesLookup {

    static final Logger LOGGER = Logger.getLogger(ServicesLookup.class);
    private static ServicesLookup INSTANCE;

    private ProxyFactory proxyFactory;
    private LookupSpace lookupSpace;
    private LogSpace logSpace;
    private TransportFactory transportFactory;
    private AggSpace aggSpace;
    private AdminSpace adminSpace;
    private BundleSpace bundleSpace;
    private ResourceSpace resourceSpace;
    private ReplicationService replicationService;
    private DeploymentService deploymentService;
    private Uploader uploader;

    private ServicesLookup(VSOProperties.ports port) throws UnknownHostException, URISyntaxException {
        if (Boolean.getBoolean("test.mode") == true) return;


        Integer dashboardVScapePort = Integer.getInteger("dashboard.vscape.port", NetworkUtils.determinePort(VSOProperties.getPort(port)));
        java.util.concurrent.ExecutorService executor = ExecutorService.newDynamicThreadPool("worker", "Dsh-TRPRT");


        transportFactory = new TransportFactoryImpl(executor, "servlet");
        transportFactory.start();
        proxyFactory = new ProxyFactoryImpl(transportFactory, dashboardVScapePort, executor, "WebApp");
        proxyFactory.start();
        LOGGER.info("Loading LookUpspace");
        lookupSpace = LookupSpaceImpl.getRemoteService(VSOProperties.getLookupAddress(), proxyFactory, "WebAppLookup");

        LOGGER.info("Loading ResourceSpace");
        resourceSpace = ResourceSpaceImpl.getRemoteService("WebAppAdminSpace", lookupSpace, proxyFactory);

        LOGGER.info("Loading AdminSpace");
        adminSpace = AdminSpaceImpl.getRemoteService("WebAppAdminSpace", lookupSpace, proxyFactory);

        LOGGER.info("Loading BundleSpace");
        bundleSpace = BundleSpaceImpl.getRemoteService("WebAppAdminSpace", lookupSpace, proxyFactory);

        LOGGER.info("Loading ReplicationService");
        replicationService = SpaceServiceImpl.getRemoteService("WebAppAdminSpace", ReplicationService.class, lookupSpace, proxyFactory, ReplicationService.NAME, false, false);

        LOGGER.info("Loading Deployment");
        deploymentService = SpaceServiceImpl.getRemoteService("WebAppAdminSpace", DeploymentService.class, lookupSpace, proxyFactory, DeploymentService.NAME, false, false);

        LOGGER.info("Loading LogSpace");
        logSpace = LogSpaceImpl.getRemoteService("WebAppLogSpace", lookupSpace, proxyFactory, false);

        LOGGER.info("Loading AggSpace");
        aggSpace = AggSpaceImpl.getRemoteService("WebAppAggSpace", lookupSpace, proxyFactory);

//        String mgmtId = resourceSpace.findResourceIdsBy("type contains Management AND resourceId contains -0").get(0);
        LOGGER.info("Loading Uploader");
        uploader = proxyFactory.getRemoteService(Uploader.NAME, Uploader.class, System.getProperty("vs.agent.address", "stcp://localhost:11003"));

    }
    public synchronized static ServicesLookup getInstance(VSOProperties.ports port) {


        if (INSTANCE == null) {
            try {
                INSTANCE = new ServicesLookup(port);
            } catch (UnknownHostException e) {
                System.out.println("ServletServices:" + e);
                e.printStackTrace();
            } catch (URISyntaxException e) {
                System.out.println("ServletServices:" + e);
                e.printStackTrace();
            }
        }
        return INSTANCE;
    }

    public void init(WebAppContext wac) {
        if (Boolean.getBoolean("test.mode") == true) return;
        wac.setAttribute(LogSpace.NAME, logSpace);
        wac.setAttribute(LookupSpace.NAME, lookupSpace);
        wac.setAttribute("ProxyFactory", proxyFactory);
    }

    public void init(DeploymentManager dm) {
        if (Boolean.getBoolean("test.mode") == true) return;
        dm.setContextAttribute(LogSpace.NAME, logSpace);
        dm.setContextAttribute(LookupSpace.NAME, lookupSpace);
        dm.setContextAttribute(AggSpace.NAME, aggSpace);
        dm.setContextAttribute("ProxyFactory", proxyFactory);
    }

    public ProxyFactory getProxyFactory() {
        return proxyFactory;
    }

    public LookupSpace getLookupSpace() {
        return lookupSpace;
    }

    public AggSpace getAggSpace() {
        return aggSpace;
    }

    public AdminSpace getAdminSpace() {
        while (adminSpace == null) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return adminSpace;
    }

    public BundleSpace getBundleSpace() {
        return bundleSpace;
    }

    public ResourceSpace getResourceSpace() {
        return resourceSpace;
    }

    public ReplicationService getReplicationService() {
        return replicationService;
    }

    public Uploader getUploader() {
        return uploader;
    }

    public DeploymentService getDeploymentService() { return deploymentService;  }

    public LogSpace getLogSpace() {
        int count = 0;
        while (logSpace == null && count++ < 30) {
            try {
                System.out.println("Waiting on LogSpace");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return logSpace;
    }



    public void shutdown() {
        proxyFactory.stop();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {

        }
        transportFactory.stop();
    }
}
