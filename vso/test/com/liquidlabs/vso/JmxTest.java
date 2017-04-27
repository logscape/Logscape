package com.liquidlabs.vso;

import org.junit.Test;

import com.liquidlabs.vso.agent.JmxScriptRunner;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JmxTest {

	@Test
	public void shouldQueryStuff() throws Exception {

		try {

            System.setProperty("sun.rmi.transport.tcp.responseTimeout", "1000");

            String url = "localhost:8890";
            if (url.split(":").length == 2){
                url = "service:jmx:rmi:///jndi/rmi://" + url + "/jmxrmi";

            }

            System.out.println("URL:" + url);

            JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url));


//			String mempool1 = "java.lang:type=MemoryPool,name=Par Eden Space@Usage";
//			String mempool2 = "java.lang:type=MemoryPool,name=Par Survivor Space@Usage";
//			String mempool3 = "java.lang:type=MemoryPool,name=CMS Old Gen@Usage";
//			String mem = "java.lang:type=Memory@HeapMemoryUsage";
			String memnH = "java.lang:type=Memory@NonHeapMemoryUsage";
			String threads = "java.lang:type=Threading@PeakThreadCount";
			String thread1 = "java.lang:type=Threading@DaemonThreadCount";
			String thread2 = "java.lang:type=Threading@ThreadCount";
			String os = "java.lang:type=OperatingSystem,*@Name@Arch@AvailableProcessors@ProcessCpuTime@TotalPhysicalMemorySize@FreePhysicalMemorySize";
			String resourceCpu = "com.liquidlabs.vscape.agent.Profile:type=Admin@CpuTime@CpuModel";
//			String resourceCpu = "com.liquidlabs.vscape.agent.Profile:type=Admin";

			String queries[] = new String[] { 
//					mempool1, mempool2, mempool3,
//					mem, 
//					memnH, 
//					threads, thread2, 
					os,
//					resourceCpu
					};
			String jmxUrl = "service:jmx:rmi:///jndi/rmi://localhost:8989/jmxrmi";

			new JmxScriptRunner(System.out, false).runJmxQueries(queries,
					jmxUrl, null);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

}
