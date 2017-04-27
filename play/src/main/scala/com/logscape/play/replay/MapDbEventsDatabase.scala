package com.logscape.play.replay

import com.liquidlabs.log.search.{TimeUID, ReplayEvent}
import com.liquidlabs.log.space.LogSpace
import com.logscape.play.replay.MapDbPerRequestSessionDb
import scala.collection.mutable
import scala.collection.mutable.Set
import org.mapdb.{BTreeMap, DB, DBMaker}
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

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 01/05/2013
 * Time: 13:23
 * To change this template use File | Settings | File Templates.
 */
class MapDbEventsDatabase(parentDir:java.io.File, scheduler:ScheduledExecutorService, logSpace: LogSpace) extends MapEventsDatabase(parentDir, scheduler, logSpace) {


  println("Creating MapDbEventsDatabase")

  new java.io.File(parentDir, "DbEvents").delete()
  new java.io.File(parentDir, "DbEvents.p").delete()

  val db: DB = DBMaker.newFileDB(new java.io.File(parentDir, "DbEvents")).closeOnJvmShutdown().transactionDisable().cacheWeakRefEnable().mmapFileEnableIfSupported().deleteFilesAfterClose().make()


  scheduler.scheduleWithFixedDelay(new Runnable() {
    def run() {
      // delete event db files which are older than 2 days
      val files:Array[java.io.File] = parentDir.listFiles()
      files.iterator.toList.foreach(f => {
        if (f.lastModified() < new DateTime().minusDays(2).getMillis) {
          f.delete();
        }
      })

    }
  }, 1,2, TimeUnit.HOURS)



  override def getDb(request:String) = {
    val requestId = fixRequestId(request)
    dbMap.get(requestId) match {
      case None =>  {
        var adb = new MapDbPerRequestSessionDb(requestId, db, scheduler)
        dbMap.put(requestId, adb)
        scheduleCleanup(requestId)
        adb
      }
      case Some(adb) => adb
    }

  }
}
