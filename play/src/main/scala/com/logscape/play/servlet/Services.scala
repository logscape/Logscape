package com.logscape.play.servlet

import com.liquidlabs.services.ServicesLookup
import com.liquidlabs.vso.VSOProperties.ports
import com.logscape.play.search.RealSearcher
import com.logscape.play.replay.{MapDbEventsDatabase, NativeEventsDatabase, EventsDatabase, ChronicleMapEventsDatabase}
import com.liquidlabs.common.concurrent.ExecutorService
import com.liquidlabs.vso.VSOProperties

trait Services {
  val startTime = VSOProperties.startTime;
  lazy val servletServices = ServicesLookup.getInstance(ports.DASHBOARD)
  lazy val logSpace = servletServices.getLogSpace
  lazy val uploader = servletServices.getUploader
  lazy val aggSpace= servletServices.getAggSpace
  lazy val adminSpace = servletServices.getAdminSpace
  lazy val bundleSpace = servletServices.getBundleSpace
  lazy val resourceSpace = servletServices.getResourceSpace
  lazy val deploymentService = servletServices.getDeploymentService
  lazy val replicationService = servletServices.getReplicationService
  lazy val searcher = new RealSearcher(aggSpace, logSpace)

  lazy val scheduler = SingeltonDb.scheduler
  lazy val eventsDatabase = SingeltonDb.db

  lazy val proxyFactory = servletServices.getProxyFactory

}

object SingeltonDb {

  def getDb : EventsDatabase = {
    val impl  = System.getProperty("event.db", "chronicle");
    if (impl.equals("mapdb") ) {
      new MapDbEventsDatabase(new java.io.File(com.liquidlabs.log.LogProperties.getDashboardEventsDB), scheduler, ServicesLookup.getInstance(ports.DASHBOARD).getLogSpace)
    } else if (impl.equals("chronicle") ) {
      new ChronicleMapEventsDatabase(new java.io.File(com.liquidlabs.log.LogProperties.getDashboardEventsDB), scheduler, ServicesLookup.getInstance(ports.DASHBOARD).getLogSpace)
    } else {
      new NativeEventsDatabase(new java.io.File(com.liquidlabs.log.LogProperties.getDashboardEventsDB), scheduler, ServicesLookup.getInstance(ports.DASHBOARD).getLogSpace)
    }
  }

  val scheduler = ExecutorService.newScheduledThreadPool(5, "services")
  val db = getDb
}