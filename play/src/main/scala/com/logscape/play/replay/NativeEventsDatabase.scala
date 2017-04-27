package com.logscape.play.replay

import com.liquidlabs.log.space.LogSpace
import org.mapdb.{DB, DBMaker}
import java.util.concurrent.{TimeUnit, ScheduledExecutorService}
import org.joda.time.DateTime
import scala._

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 01/05/2013
 * Time: 13:23
 * To change this template use File | Settings | File Templates.
 */
class NativeEventsDatabase(parentDir:java.io.File, scheduler:ScheduledExecutorService, logSpace: LogSpace) extends MapEventsDatabase(parentDir, scheduler, logSpace) {


  println("Creating MapDbEventsDatabase")

  new java.io.File(parentDir, "DbEvents").delete()
  new java.io.File(parentDir, "DbEvents.p").delete()

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
        var adb = new NativePerRequestSessionDb(requestId, scheduler)
        dbMap.put(requestId, adb)
        scheduleCleanup(requestId)
        adb
      }
      case Some(adb) => adb
    }

  }
}
