package com.liquidlabs.vso.deployment;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceAgentImpl;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.resource.ResourceSpace;

public class SystemBouncer {

	private static final Logger LOGGER = Logger.getLogger(SystemBouncer.class);

	private boolean bounced;
	private final long waitPeriod;
	private final ResourceSpace resourceSpace;
	private final ProxyFactory proxyFactory;

	public SystemBouncer(long waitPeriod, ResourceSpace resourceSpace, ProxyFactory proxyFactory) {
		this.waitPeriod = waitPeriod;
		this.resourceSpace = resourceSpace;
		this.proxyFactory = proxyFactory;
	}

	public void bounce(final ResourceAgent agent) {
		bounced = true;
		bounceAgents(agent);
	}

	private void bounceAgents(ResourceAgent myAgent) {
		List<String> resourceIds = resourceSpace.findResourceIdsBy("id == 0");
		Collections.sort(resourceIds);
		int failed = 0;
		final String myResourceId = myAgent.getProfiles().get(0).getResourceId();

		LOGGER.info("Going to bounce Agents:" + resourceIds.size());
		System.err.println(new Date() + " Going to bounce Agents:" + resourceIds.size());

		// tell all resources to bounce at the same time for quicker reboot
		java.util.concurrent.ExecutorService executor = ExecutorService.newFixedThreadPool(10, "sys-bouncer");
		final AtomicInteger count = new AtomicInteger();
		for (final String id : resourceIds) {
				try {
					
					final ResourceProfile profile = resourceSpace.getResourceDetails(id);
					if (!profile.getResourceId().equals(myResourceId) )
						executor.execute(new Runnable() {
							public void run() {
								try {
									LOGGER.info(count.getAndIncrement() + " Bouncing:" + profile.getResourceId() + " ep:" + profile.getEndPoint());
									ResourceAgentImpl.getRemoteService(proxyFactory, profile.getEndPoint()).bounce(true);
									Thread.sleep(1000);
								} catch (Throwable t) {
									LOGGER.warn(t.toString(), t);
								}
							}
						});
				} catch (Throwable t) {
					failed++;
				}
		}
		try {
			LOGGER.info("Waiting for Agent bounce:" + resourceIds.size() + " failed count:" + failed);
			executor.shutdown();
			boolean completed = executor.awaitTermination(resourceIds.size() * 1000, TimeUnit.SECONDS);
			if (!completed) {
				LOGGER.info("Still tasks completing....");
				Thread.sleep(resourceIds.size() * 1000);
			}
		} catch (InterruptedException e) {
			LOGGER.error(e);
		}
		bounceMe(myAgent);
	}

	private void bounceMe(ResourceAgent myAgent) {
		try {
			LOGGER.info("Pause before bounce");
			System.err.println(new Date() + " Pause before bouncing");
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
		}
		LOGGER.info("bouncing myself");
		try {
			myAgent.bounce(false);
		} catch (Exception e) {
			LOGGER.error(e);
		}
		System.err.println(new Date() + " done - bouncing");
		
	}

}
