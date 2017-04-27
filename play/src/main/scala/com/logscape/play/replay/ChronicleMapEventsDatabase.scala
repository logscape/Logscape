package com.logscape.play.replay

import com.liquidlabs.log.search.{TimeUID, ReplayEvent}
import com.liquidlabs.log.space.LogSpace
import com.liquidlabs.vso.agent.metrics.DefaultOSGetter
import scala.collection.mutable
import java.util.concurrent.{TimeUnit, ScheduledExecutorService}
import org.fusesource.scalate.util.Logging
import com.liquidlabs.log.fields.{FieldSets, FieldSet}
import com.liquidlabs.log.LogProperties
import org.joda.time.DateTime
import scala._
import java.io.{PrintWriter, BufferedOutputStream, FileOutputStream}
import java.{lang, util}
import com.liquidlabs.common.DateUtil
import jregex.Pattern
import com.liquidlabs.dashboard.server.DashboardProperties
import net.openhft.chronicle.map.ChronicleMapBuilder

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 01/05/2013
 * Time: 13:23
 * To change this template use File | Settings | File Templates.
 */
class ChronicleMapEventsDatabase(parentDir:java.io.File, scheduler:ScheduledExecutorService, logSpace: LogSpace) extends MapEventsDatabase(parentDir, scheduler, logSpace) {

  val mem = new DefaultOSGetter().getAvailMemMb

  var DB_SIZE = java.lang.Long.getLong("db.replay.event.store", 50 * 1000);
  if (mem > 4000) {
      DB_SIZE = java.lang.Long.getLong("db.replay.event.store", 100 * 1000);
  }
  if (mem > 8000) {
    DB_SIZE = java.lang.Long.getLong("db.replay.event.store", 200 * 1000);
  }

  info("Creating ChronicleMapEventsDatabase: MAP_SIZE:" + DB_SIZE + " AvailMem:" + mem)
  println("Creating ChronicleMapEventsDatabase: MAP_SIZE:" + DB_SIZE + " AvailMem:" + mem)

  val eventsDB = ChronicleMapBuilder.of(classOf[TimeUID],classOf[ReplayEvent]).entries(DB_SIZE).averageValueSize(1 * 1024).create()

  override def getDb(request:String) = {
    val requestId = fixRequestId(request)
    dbMap.get(requestId) match {
      case None =>  {
        var adb = new ChronicleMapPerRequestSessionDb(requestId, scheduler, eventsDB)
        dbMap.put(requestId, adb)
        scheduleCleanup(requestId)
        adb
      }
      case Some(adb) => adb
    }

  }
}
