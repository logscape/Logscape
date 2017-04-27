package com.logscape.play

import com.liquidlabs.services.ServicesLookup
import com.liquidlabs.vso.VSOProperties
import com.liquidlabs.vso.VSOProperties.ports
import org.eclipse.jetty.server.{Connector, Server}
import sys.SystemProperties
import org.eclipse.jetty.webapp.WebAppContext
import com.liquidlabs.dashboard.server.{DashboardProperties}
import org.fusesource.scalate.util.Logging
import org.eclipse.jetty.server.nio.SelectChannelConnector
import com.liquidlabs.common.jmx.JmxHtmlServerImpl
import javax.management.MBeanServer
import java.lang.management.ManagementFactory
import org.eclipse.jetty.jmx.MBeanContainer

object PlayIdeJetty extends Logging{
  def main(args: Array[String]) {
    info("Starting Jetty")
    System.setProperty("dashboard.vscape.port","11100")

    // needed to find the ssl certs
    System.setProperty("vscape.home","../master/build/logscape/")






    val properties = new SystemProperties
    val port = properties.getOrElse("port", "8888")
    val jmxServer: JmxHtmlServerImpl = new JmxHtmlServerImpl(VSOProperties.getJMXPort(ports.DASHBOARD), true)
    jmxServer.start

    DashboardProperties.setHttpPort(port)
    DashboardProperties.setHttpsPort("8443")
    DashboardProperties.setHttpsUrl("https://localhost:8443")
    DashboardProperties.setHttpUrl("http://localhost:"+port)
    val server = new Server(Integer.valueOf(port))
    val connectors:Array[Connector] = server.getConnectors
    val connector:SelectChannelConnector = connectors(0).asInstanceOf[SelectChannelConnector]
    val headerSize:Int = connector.getRequestHeaderSize
    connector.setRequestHeaderSize(headerSize * 4)
    val bufferSize:Int = connector.getRequestBufferSize
    connector.setRequestBufferSize(bufferSize * 4)

    val context: WebAppContext = new WebAppContext()



    context.setResourceBase("src/main/webapp")
    context.setDescriptor("src/main/webapp/WEB-INF")
    context.setContextPath("/play")
    context.setDefaultsDescriptor("./webdefault.xml")
    context.setParentLoaderPriority(true)


    if (args.length == 0) {
      ServicesLookup.getInstance(ports.DASHBOARD).init(context)
    }









    server.setHandler(context)

    val mBeanServer = ManagementFactory.getPlatformMBeanServer();
    val mBeanContainer = new MBeanContainer(mBeanServer);
    server.getContainer().addEventListener(mBeanContainer);
    mBeanContainer.start();


    server.start()
    info("Started")
    val foo: AnyRef = new AnyRef
    foo synchronized {
      foo.wait()
    }

  }
}
