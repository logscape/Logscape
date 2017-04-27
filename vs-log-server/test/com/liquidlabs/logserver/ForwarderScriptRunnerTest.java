package com.liquidlabs.logserver;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogConfiguration;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.addressing.AddressHandler.RefreshAddrs;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.EmbeddedServiceManager;
import com.liquidlabs.vso.deployment.ScriptForker;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.BundleSerializer;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.lookup.LookupSpace;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ForwarderScriptRunnerTest {

	final ScriptForker scriptForker = new ScriptForker();
	private LookupSpace lookup;
	private ProxyFactory proxyFactory;
	private LogSpace logSpace;
	private AggSpace aggSpace;
	private LogServer logServer;

	@Test
	public void shouldRunTheScript() throws Exception {

		lookup = mock(LookupSpace.class);
		proxyFactory = mock(ProxyFactory.class);
		logSpace = mock(LogSpace.class);
		aggSpace = mock(AggSpace.class);
		logServer = mock(LogServer.class);

		Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread thread, Throwable throwable) {

            }
        };

        when(lookup.getServiceAddresses(anyString(), anyString(), false)).thenReturn(new String[]{"stcp://localhost:1223"});
        when(proxyFactory.getRemoteService(anyString(), Matchers.any(Class.class), any(String[].class), Matchers.<AddressUpdater>any(AddressUpdater.class), Matchers.<RefreshAddrs>any())).thenReturn(logSpace).thenReturn(aggSpace).thenReturn(logServer);
        when(proxyFactory.getAddress()).thenReturn(new URI("stcp://localhost:1234"));
        when(proxyFactory.getEndPoint()).thenReturn("stcp://localhost:1234");
        when(logSpace.getConfiguration(anyString())).thenReturn(new LogConfiguration());

		Bundle loadBundle = new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle("vs-log-server.bundle");
		Service service = loadBundle.getService("LogForwarder");
		assertNotNull(service);

		String script = service.getScript();
		System.out.println("Running:" + script);

		Map<String, Object> variables= new HashMap<String, Object>();
		variables.put("lookupSpace", lookup);
		variables.put("proxyFactory", proxyFactory);
		variables.put("agentPort", 1234);
		variables.put("resourceId", "resourceId");
		variables.put("serviceManager", new EmbeddedServiceManager(""));
		variables.put("exceptionHandler", exceptionHandler);
		scriptForker.runString(script, "", variables, System.class.getClassLoader(),"workId");

		Thread.sleep(3 * 1000);

	}

}
