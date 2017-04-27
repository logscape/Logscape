package com.liquidlabs.common.jmx;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.management.*;

import com.sun.jdmk.comm.AuthInfo;
import org.apache.log4j.Logger;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.NetworkUtils;

public class JmxHtmlServerImpl implements JmxHtmlServer {
	
	private static final Logger LOGGER = Logger.getLogger(JmxHtmlServer.class);
	
	private static final String MBEAN_NAME = "adaptor:type=html";
	private com.sun.jdmk.comm.HtmlAdaptorServer htmlAdaptor;
	private int port;
	

	LifeCycle.State status = LifeCycle.State.STOPPED;
	private String hostname;

	public static String locateHttpUrL(){
		final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName objectName;
		try {
			objectName = new ObjectName(MBEAN_NAME);
			Object protocol = mbs.getAttribute(objectName, "Protocol");
			Object host = mbs.getAttribute(objectName, "Host");
			Object port = mbs.getAttribute(objectName, "Port");
			if (port != null) {
				
			}
			if (protocol.equals("html")) protocol = "http";
			return protocol + "://" + NetworkUtils.getIPAddress() + ":" + port + "/#host=" + host;
		} catch (Exception e) {
			LOGGER.info(e.getMessage());
		}
		return "";
	}
	public JmxHtmlServerImpl(int port, boolean findPortOnClash) {

		if (findPortOnClash) {
			port = NetworkUtils.determinePort(port);
		}
		hostname = NetworkUtils.getIPAddress();
		
		create(port, MBEAN_NAME);
		
		this.port = port;
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				JmxHtmlServerImpl.this.stop();
			}
		});
        LOGGER.info("Http Management on JMXPort:" + getURL() );
	}
	public void start() {
		htmlAdaptor.start();
		status = LifeCycle.State.RUNNING;
	}
	public void stop() {
		if (status == LifeCycle.State.RUNNING) {
			status = LifeCycle.State.STOPPED;
			htmlAdaptor.stop();
		}
	}
	public String getURL(){
		return "http://" + hostname + ":" + port;
	}
	public com.sun.jdmk.comm.HtmlAdaptorServer create(final int htmlAdaptorPort, final String adaptorMBeanName) {

        // see if we need to
        String property = System.getProperty("com.sun.management.jmxremote.port");
        if (property == null) {
//            loadJMXMBeanServer(htmlAdaptorPort);
        }

        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		htmlAdaptor = new com.sun.jdmk.comm.HtmlAdaptorServer(htmlAdaptorPort);
        htmlAdaptor.addUserAuthenticationInfo(new AuthInfo(System.getProperty("jmx.user","sysadmin"), System.getProperty("jmx.pwd","ll4bs")));
		registerProvidedMBean(htmlAdaptor, adaptorMBeanName);
        new MBeanServerInfoJMX();
		return htmlAdaptor;
	}

    //        System.setProperty("com.sun.management.jmxremote.port", Integer.toString(htmlAdaptorPort +1));
//        System.setProperty("com.sun.management.jmxremote.ssl","false");
//        System.setProperty("com.sun.management.jmxremote.authenticate","false");
//        MBeanServer mbsCustom= MBeanServerFactory.createMBeanServer();
//        public static String loadJMXAgent(int port) throws IOException,

//                AttachNotSupportedException, AgentLoadException,
//                AgentInitializationException {


//    private void loadJMXMBeanServer(int port) {
//        try {
//            String name = ManagementFactory.getRuntimeMXBean().getName();
//            VirtualMachine vm = VirtualMachine.attach(name.substring(0, name.indexOf('@')));
//
//            String lca = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
//            if (lca == null) {
//                Path p = Paths.get(System.getProperty("java.home")).normalize();
//                if (!"jre".equals(p.getName(p.getNameCount() - 1).toString().toLowerCase())) {
//                    p = p.resolve("jre");
//                }
//                File f = p.resolve("lib").resolve("management-agent.jar").toFile();
//                if (!f.exists()) {
//                    throw new IOException("Management agent not found");
//                }
//                String options = String.format("com.sun.management.jmxremote.port=%d, " +
//                        "com.sun.management.jmxremote.authenticate=false, " +
//                        "com.sun.management.jmxremote.ssl=false", port);
//                vm.loadAgent(f.getCanonicalPath(), options);
//                lca = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
//            }
//            vm.detach();
//            System.out.printf(lca);
//        } catch (Throwable t) {
//            t.printStackTrace();;
//        }
//    }

    public int getPort() {
		return port;
	}

	public  void registerProvidedMBean(final Object objectToBeRegisteredAsMBean, final String nameForMBean) {
		final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {
			mbs.registerMBean(objectToBeRegisteredAsMBean, new ObjectName(nameForMBean));
		} catch (MalformedObjectNameException badMBeanNameEx) {
			System.err.println("ERROR trying to create ObjectName with " + nameForMBean + ":\n" + badMBeanNameEx.getMessage());
		} catch (MBeanRegistrationException badMBeanRegistrationEx) {
			System.err.println("ERROR trying to register MBean " + nameForMBean + ":\n" + badMBeanRegistrationEx.getMessage());
		} catch (NotCompliantMBeanException nonCompliantEx) {
			System.err.println("ERROR: " + nameForMBean + " is not a compliant MBean.:\n" + nonCompliantEx.getMessage());
		} catch (InstanceAlreadyExistsException redundantMBeanEx) {
			System.err.println("WARN: MBean instance " + nameForMBean + " already exists:\n" + redundantMBeanEx.getMessage());
		}
	}

}
