package com.liquidlabs.vso;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.common.*;
import com.liquidlabs.vso.deployment.bundle.*;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.*;

import com.liquidlabs.common.LifeCycle.State;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceAgentImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.deployment.BundleDeploymentService;
import com.liquidlabs.vso.deployment.DeploymentService;
import com.liquidlabs.vso.deployment.bundle.Bundle.Status;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;
import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAllocatorImpl;
import com.liquidlabs.vso.work.WorkAssignment;

public class VSOMain {
	static final Logger LOGGER = Logger.getLogger(VSOMain.class);
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "VSOMain");

    // -boot 8080  or stcp://lookuphost:8080
	public static void main(String[] args) throws NumberFormatException, Exception {	
		try {
            auditLogger.emit("boot", new Date().toString());
            cleanupTmpDir();

			writeToStatus("VSOMain ENTRY");
			writeToStatus("PID:" + ManagementFactory.getRuntimeMXBean().getName());
            writeToStatus("OperatingSystem:" + System.getProperty("os.name").toUpperCase() + " / " + System.getProperty("os.version"));
			writeToStatus("VSOMain starting...:" + Arrays.toString(args));
			printPropertiesToStdout();
			
			String resourceType = VSOProperties.getResourceType();
			
			boolean bootManagement = resourceType.contains("Management");
			int port = VSOProperties.getLookupPort();
            if (args.length == 0) {
                writeToStatus("FATAL: Cannot run with NoArgs. Run logscape.[bat/sh]");
                return;
            }

            URI lookupAddress = new URI(args[0]);

			int profiles = 1;
			writeToStatus("VSOMain boot Type:" + resourceType + " host:" + NetworkUtils.getIPAddress());
			
			while (NetworkUtils.getIPAddress() == null) {
				writeToStatus("Waiting for Nic:" + NetworkUtils.useNWIF);
				Thread.sleep(30 * 1000);
			}

			if (bootManagement) {
				writeToStatus("\r\n\r\n STARTING *MANAGER* 6 profiles\r\n\r\n");
				profiles = 6;
				writeToStatus("Wait 30 seconds for the DASHBOARD on PORT:" + System.getProperty("web.app.port"));
			} else {
				writeToStatus("\r\n\r\n STARTING AGENT Role:" + VSOProperties.getResourceType() + " 1 profile\r\n\r\n");
				profiles = 1;
				if (lookupAddress.toString().contains("localhost")){
					writeToStatus("\r\n\r\n WARNING - AGENT has ManagementHost as 'stcp://localhost' - this Agent will fail unless on the same host!\r\n ** After editing setup.conf ensure you run configure(.sh/bat) **");
				}
			}
			writeToStatus("VSOMain lookup:" + lookupAddress + " PORT:" + port);
			String host = lookupAddress.getHost();
			if (host == null) {
				System.err.println("The Management URL is not valid[" + lookupAddress + "]");
				writeToStatus("The Given ManagementURL is not valid[" + lookupAddress + "]");
				System.exit(-1);
			}
			resolveHost(host);
			// setup so the client network connections can be resolved when SO_ADDR REUSE DOESNT WORK


			Log4JLevelChanger.apply(args);
			ProxyFactory proxyFactory = getProxyFactory(VSOProperties.getAgentBasePort());
			
			listenForShutdown();

            auditLogger.emit("bootAgent", VSOProperties.getResourceType());
			
			if(bootManagement) {
				boot(lookupAddress.toString(), proxyFactory, profiles);
			} else {
				agent(lookupAddress.toString(), profiles, proxyFactory);
			}
		} catch (Throwable t) {
			System.out.println("Failed:" + t);
			t.printStackTrace();
			writeToStatus("FAILED:" + t);

            // this was introduced because everynow and then we see a JVM running with a non-daemon thread being created
            // it happens randomly. Being non-daemon VSOMain hangs...
            Thread.sleep(10 * 1000);
            writeToStatus("Calling System.exit():" + t);
            System.exit(0);

		} finally {
			writeToStatus("STOPPED");
		}
	}

    public static ResourceAgentImpl startAgent(String lookupAddress, int profiles, ProxyFactory proxyFactory) throws URISyntaxException, InterruptedException, UnknownHostException {

        writeToStatus("Agent: Starting AGENT Type:" + VSOProperties.getResourceType() );
		LookupSpace lookupSpace = null;

		// when the system is bounced we prefer services to start on the Management host
		
		writeToStatus("BOOT SEQUENCE WAIT");
		String context = "_HA_VSOAgent";
		String failoverAddress = getFailoverAddress();
		if (getManagerAddress() != null) lookupAddress = getManagerAddress();


        boolean started = false;
		while (!started) {
			LOGGER.info("Agent: boot sequence - wait for Management@" + lookupAddress);
			boolean connected = false;
			int count = 0;
			
			/**
			 * Get Valid handle to LUSpace
			 */
            Integer waitForFailoverCount = Integer.getInteger("failover.wait.count", 10);

			while (!connected) {
                writeToStatus("Connection Attempt:" + count + " WaitForFailoverCount:" + waitForFailoverCount);
				String ping = null;
				try {
					lookupSpace = LookupSpaceImpl.getLookRemoteSimple(lookupAddress, proxyFactory, context);
					ping = lookupSpace.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
                    connected = true;
					System.err.println("Result of pinging Management Node:" + ping);
					LOGGER.info("Agent: ping:" + ping);
				} catch (Throwable t) {

					Thread.sleep(5000);


                    if (count++ > waitForFailoverCount && failoverAddress != null) {

                        System.err.println("Failover:" + count);
                        writeToStatus("Attempting Failover Lookup.ping @" + failoverAddress);
                        lookupSpace = LookupSpaceImpl.getLookRemoteSimple(failoverAddress, proxyFactory, context);
                        ping = lookupSpace.ping(VSOProperties.getResourceType(), System.currentTimeMillis());
                        // success - flip to using the failover node
                        LOGGER.warn("**** USING FAILOVER NODE **** Booting from" + failoverAddress + " ping time:" + ping);
                        writeToStatus("Connected To failover:" + failoverAddress);
                        connected = true;
                        //lookupAddress = failoverAddress;
                    }
                }
				try {
					Thread.sleep(Integer.getInteger("boot.pause.secs", 10) * 1000);
					if (count++ % 10 == 0 && count > 0) {
                        writeToStatus("Waiting to connect to Management@" + lookupAddress);
						Thread.sleep(Integer.getInteger("vsomain.pause.secs", 30) * 1000);
					}
					
				} catch (Throwable e) {
				}
			}
			writeToStatus("BOOT SEQUENCE START");
			LOGGER.info("Agent: boot - init network services");
			if (failoverAddress != null) {
				lookupAddress = lookupAddress + "," + failoverAddress;
			}

            try {
				NetworkUtils.resetValues();

				WorkAllocator workAllocator = WorkAllocatorImpl.getRemoteService("StartAgent", lookupSpace, proxyFactory);
				ResourceSpace resourceSpace = ResourceSpaceImpl.getRemoteService("StartAgent", lookupSpace, proxyFactory);
				DeploymentService bundleDeploymentService = BundleDeploymentService.getRemoteService("StartAgent", lookupSpace, proxyFactory);
		
				JmxHtmlServerImpl jmxHtmlServer = new JmxHtmlServerImpl(VSOProperties.getJMXPort(VSOProperties.ports.SHARED), true);
				jmxHtmlServer.start();

				final ResourceAgentImpl agent = new ResourceAgentImpl(workAllocator, resourceSpace, bundleDeploymentService, lookupSpace, proxyFactory, lookupAddress, jmxHtmlServer.getURL(), profiles);
				proxyFactory.registerMethodReceiver(ResourceAgent.NAME, agent);
				agent.start();
				
				LOGGER.info("Agent: started");
				writeToStatus("BOOT SEQUENCE COMPLETE");
				writeToStatus("BOOT SEQUENCE RUNNING");
				
				Runtime.getRuntime().addShutdownHook(new Thread() {
					public void run() {
						writeToStatus("STOPPED");
						System.out.println("Agent shutdown hook called");
						agent.stop();
					}
				});
				started = true;
				return agent;
			} catch (Throwable t) {
				writeToStatus("REBOOT");
				LOGGER.info("Agent: Revert to boot again - Failed to startAgent:" + t.getMessage(), t);
				Thread.sleep(1000);
			}
		}
		writeToStatus("FAILED");
		// should never happen
		System.exit(-10);
		return null;
	}
	private static void boot(String lookupAddress, ProxyFactory proxyFactory, int profiles) throws UnknownHostException, URISyntaxException, InterruptedException {

        auditLogger.emit("bootManagement", new Date().toString());
		writeToStatus("BOOT SEQUENCE INIT");
		JmxHtmlServerImpl jmxHtmlServer = new JmxHtmlServerImpl(VSOProperties.getJMXPort(VSOProperties.ports.LOOKUP), true);
		jmxHtmlServer.start();
		

        if (getManagerAddress() != null) lookupAddress = getManagerAddress();

        LOGGER.info("BOOT Start - Creating BOOT Agent, Address:" + lookupAddress);

        agent = new ResourceAgentImpl(lookupAddress, proxyFactory, jmxHtmlServer.getURL(), profiles);
		
		agent.setBuildInfo();
		
		agent.setStatus(LifeCycle.State.STARTED);
		
		LOGGER.info("BOOT Start - Loading Boot bundle:" + bundleName());


		proxyFactory.registerMethodReceiver(ResourceAgent.NAME, agent);

		Bundle bootBundle = new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle(bundleName());
		List<Service> services = bootBundle.getServices();
        List<WorkAssignment> assignments = new ArrayList<WorkAssignment>();
		LOGGER.info("BOOT Start - [BootSequence]");
		for (Service service : services) {
			final WorkAssignment workAssignment = WorkAssignment.fromService(service, agent.getId());
			workAssignment.setWorkingDirectory(".");
			assignments.add(workAssignment);
					try {
						LOGGER.info("[BootSequence] Starting:" + workAssignment.getId());
                        auditLogger.emit("startWork", workAssignment.getId());
						agent.start(workAssignment);
					} catch (Throwable t) {
						LOGGER.error(t);
					}
			if (workAssignment.getId().contains("Lookup")) {
				Thread.sleep(1000);
			}
		}

		writeToStatus("BOOT SEQUENCE COMPLETE");
		LOGGER.info("BOOT Finished - [BootSequence] core services running");
		
		LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, proxyFactory,"VSOMainLU");
		WorkAllocator workAllocator = WorkAllocatorImpl.getRemoteService("VSOMainWA", lookupSpace, proxyFactory);
		ResourceSpace resourceSpace = ResourceSpaceImpl.getRemoteService("VSOMainRS", lookupSpace, proxyFactory);
		DeploymentService deploymentService = BundleDeploymentService.getRemoteService("StartAgent", lookupSpace, proxyFactory);
		
		registerBootAllocationCounts(proxyFactory, agent, bootBundle, assignments,	lookupSpace, workAllocator, resourceSpace, agent.getId());
		
		writeToStatus("BOOT RUNNING");
		
		writeToStatus("AGENT STARTING");
		
		agent.booted(lookupSpace, workAllocator, resourceSpace, deploymentService);
		writeToStatus("AGENT STARTED");
		writeToStatus("DEPLOY OTHERS");
        auditLogger.emit("startWork", "Others[]");
		LOGGER.info("BOOT Starting Other Bundles (replicator, logscape, dashboard, etc");
		deploymentService.deployAllBundles();
		
		scheduleFileCopyOfSysBundles(proxyFactory);

		agent.go();
	}
	private static void scheduleFileCopyOfSysBundles(ProxyFactory proxyFactory) {
		final String[] files = new String[] { "boot.zip", "replicator-1.0.zip", "vs-log-1.0.zip","lib-1.0.zip" };
		ScheduledExecutorService scheduler = proxyFactory.getScheduler();
		scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
                int count = 0;
				for (String file : files) {
					try {
                        File dest = new File(VSOProperties.getWebAppRootDir() + "/bundles/" + file);
                        // dont do the copy if the files exists and we have been running for a bit and the file is old...
                        if (dest.exists() && count++ > 100) {
                            if (dest.lastModified() < new DateTime().minusMinutes(10).getMillis()) return;
                        }
                        dest.getParentFile().mkdirs();
                        FileUtil.copyFile(new File("downloads/" + file), dest);
					} catch (Throwable t) {
                        LOGGER.warn(new File(".").getAbsolutePath() + " App.zip CopyFailed", t);
					}
				}
			}
		}, 10, 10, TimeUnit.MINUTES);
	}



	private static String bundleName() {
        return VSOProperties.isHaEnabled() ? "boot-forked-1.0.bundle" : "boot-1.0.bundle";
    }

    private static void registerBootAllocationCounts(ProxyFactory proxyFactory,	final ResourceAgent resourceAgent, Bundle bootBundle,
			List<WorkAssignment> assignments, LookupSpace lookupSpace, WorkAllocator workAllocator, ResourceSpace resourceSpace, String resourceId) throws URISyntaxException {
		BundleSpace bundleSpace = BundleSpaceImpl.getRemoteService("VSOMainB", lookupSpace, proxyFactory);
		try {
			String hostname = NetworkUtils.getHostname();
			String id = "BOOT_REQUEST" + hostname +"-";
			bootBundle.setStatus(Status.ACTIVE);
            BundleHandlerImpl bundleHandler = new BundleHandlerImpl(bundleSpace);
            bundleHandler.install(bootBundle);
            ResourceProfile resourceProfile = resourceAgent.getProfiles().get(0);

            for (WorkAssignment workAssignment : assignments) {
				workAssignment.setStatus(State.RUNNING);
				workAssignment.decrementAllocationsOutstanding();

                workAllocator.saveWorkAssignment(workAssignment, resourceProfile.getResourceId());
                bundleSpace.requestServiceStart(id + workAssignment.getServiceName(), workAssignment);
			}
			resourceSpace.assignResources(id + "_X", java.util.Arrays.asList(resourceId), "BOOT_LOOKUP" + hostname, 1000, "LookupSpace", -1);
			
		} catch (Exception e) {
			LOGGER.error(e);
		}
	}
	
	private static boolean checkUsage(String[] args) {
		if ((args.length == 3 && !args[0].equals("-boot"))
				|| (args.length == 2 && args[0].equals("-boot")
				|| args.length <= 1) ) {
			usage();
		}
		return "-boot".equals(args[0]);
	}

    private static void cleanupTmpDir() {
        String property = System.getProperty("java.io.tmpdir");
        if (new File(property).getAbsolutePath().contains("logscape")) {
            writeToStatus("Cleaning java.io.tmpdir:" + property);
            File file = new File(property);
            deleteDir(file);
            file.mkdirs();
        }    }

    static   DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
    private static void writeToStatus(String string) {
        try {
            String statusMsg = formatter.print(DateTimeUtils.currentTimeMillis()) + " " + string;
            System.out.println(statusMsg);
            FileOutputStream fos = new FileOutputStream("status.txt", true);
            fos.write((statusMsg + "\n").getBytes());
            LOGGER.info(statusMsg);
            fos.close();
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }
    static void resolveHost(String endPoint) {
        try {
            URI uri = new URI(endPoint);
            InetAddress.getAllByName(uri.getHost());
        } catch (Throwable t) {
            writeToStatus("FAILED TO RESOLVE Host:" + endPoint);
            throw new RuntimeException("Failed to resolveHost:" + endPoint, t);
        }
    }

    private static void printPropertiesToStdout() {
        Properties properties = System.getProperties();
        Set<Object> keySet2 = properties.keySet();
        List<String> keySet = new ArrayList<String>();
        for (Object object : keySet2) {
            if (object instanceof String) keySet.add((String) object);
        }

        Collections.sort(keySet);
        for (Object object : keySet) {
            System.out.println(object + " - " + properties.getProperty(object.toString()).toString());
        }
    }


    // yes its a bit crappy but we need to listen for shutdown
    static ResourceAgentImpl agent = null;
    private static void agent(String lookup, int profiles, ProxyFactory proxyFactory) throws UnknownHostException, URISyntaxException, InterruptedException {
        agent = startAgent(lookup, profiles, proxyFactory);
        agent.go();
    }

    private static ProxyFactory getProxyFactory(int bootPort) throws UnknownHostException, URISyntaxException{

        ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();
        return mapperFactory.getProxyFactory();
    }

    private static void listenForShutdown() {
        Thread t = new Thread(new Runnable(){
            public void run() {
                try {
                    System.out.println("Use 'bye' or 'exit' to stop the agent");
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    while(true) {
                        try {
                            String readLine = reader.readLine();
                            if (readLine != null && ("exit".equals(readLine.toLowerCase()) || "bye".equals(readLine.toLowerCase()))) {
                                writeToStatus("STOPPED");
                                System.err.println(" VSOMain....terminating VSO due to exit msg from hook");
                                LOGGER.info("\r\n\r\n\r\n******************* TERMINATING VSO DUE TO EXIT MESSAGE FROM BOOTSTRAPPER");
                                if (agent != null) agent.stop();
                                LOGGER.info("\r\n\r\n\r\n******************* sysexit");
                                System.exit(0);
                            }
                        } catch (IOException e) {
                            Thread.sleep(50);
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.error("ProcessHook error", ex);
                }

            }});
        t.setName("VSO-stdin-ShutdownListener");
        t.setDaemon(true);
        t.start();
    }

    private static void usage() {
		System.err.println("com.liquidlabs.vso.VSOMain [-boot] stcp://lookup.host:port numProfiles");
		System.exit(1);
	}
	public static int deleteDir(File dir) {
		int count = 0;
		int failed = 0;
		LOGGER.debug("Delete:" + dir);
		if (dir != null && dir.exists()) {
			File[] files = dir.listFiles();
			if (files!= null) {
				for (File file : files) {
					if (file.isDirectory()) {
						LOGGER.debug(count + " DeleteDir => " + file.getName());
						count += FileUtil.deleteDir(file);
					}
					else {

						boolean delete = file.delete();

						if (delete) {
							LOGGER.debug(count + " DeleteFile:" + file.getName());
							count++;
						} else {
							LOGGER.debug(count + " DeleteFile:" + file.getName() + " LOCKED");
							failed++;
						}
					}
				}
			}
			count++;
			LOGGER.debug(count+ " DeleteDIR:" + dir);
			dir.delete();
		}
		if (failed != 0) {
			LOGGER.debug("Failed!: " + failed);
			return failed * -1;
		}
		return count;
	}


    public static String getManagerAddress() {
		return getAddress("manager.address");
	}
	public static String getFailoverAddress() {
		return getAddress("failover.address");
	}

	public static String getAddress(String property) {
        // now try the failover address if there is one on the file system
        File file = new File("downloads/connection.properties");
        if (file.exists()) {
            try {
                Properties props = new Properties();
				FileInputStream fis = new FileInputStream(file);
				props.load(fis);
				fis.close();
                return props.getProperty(property);

            } catch (Throwable e) {
                LOGGER.info(e.toString());
            }
        }

        return null;
    }
}
