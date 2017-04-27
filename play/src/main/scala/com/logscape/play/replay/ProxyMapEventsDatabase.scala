package com.logscape.play.replay

import java.io.{BufferedOutputStream, FileOutputStream, PrintWriter}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.{lang, util}

import com.liquidlabs.common.DateUtil
import com.liquidlabs.dashboard.server.DashboardProperties
import com.liquidlabs.log.LogProperties
import com.liquidlabs.log.fields.{FieldSet, FieldSets}
import com.liquidlabs.log.search.{ReplayEvent, TimeUID}
import com.liquidlabs.log.space.LogSpace
import jregex.Pattern
import org.fusesource.scalate.util.Logging
import org.joda.time.DateTime

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 01/05/2013
 * Time: 13:23
 * To change this template use File | Settings | File Templates.
 */
class ProxyMapEventsDatabase(parentDir:java.io.File, scheduler:ScheduledExecutorService, logSpace: LogSpace) extends Logging with EventsDatabase {

  val dbMap = new mutable.HashMap[String, ProxyPerRequestSessionDb]()

  def printQ = LogProperties.getPrintQ(Integer.parseInt(DashboardProperties.getHttpsPort))

  println("Creating MapEventsDatabase")
  info("MkDir PrintQ" + new java.io.File(printQ).getAbsolutePath)
  new java.io.File(printQ).mkdirs()
  parentDir.mkdirs()

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


  def scheduleCleanup(requestId: String) {
    scheduler.schedule(new Runnable() {
      def run() {
        val adb = dbMap.remove(requestId)
        if (adb.isDefined) {
          adb.get.close
        }

      }
    }, 59, TimeUnit.MINUTES)
  }

  def getDb(request:String) : ProxyPerRequestSessionDb = {
    throw new RuntimeException("Override me")
  }
  def isClosed(request:String): Boolean = {
    //    val requestId = fixRequestId(request)
    //    val db = getDb(requestId)
    //    (db == null || db.isClosed)
    false;
  }
  def addEvents(request:String, events: java.util.List[ReplayEvent])  {
    val requestId = fixRequestId(request)
    getDb(requestId).addEvents(events)
  }
  def addEvent(request: String, event:ReplayEvent) {
    val requestId = fixRequestId(request)
    getDb(requestId).addEvents(util.Arrays.asList(event))
  }

  def setFieldSet(request:String, fieldSet:FieldSet) {
    getDb(fixRequestId(request)).addFieldSet(fieldSet)
  }

  def getEventAsJson(requestId:String, eventIdString: String, baseURL: String) : String = {
    val db = getDb(requestId);
    val eventId = TimeUID.fromString(eventIdString);
    val event = db.eventsMap.get(eventId)
    val fs = getFieldSetForEvent(event.getDefaultField(FieldSet.DEF_FIELDS._type), db)
    if (event != null) return event.toJsonEventView(fs, baseURL)
    ""
  }

  private def getFieldSetForEvent(fieldSetId:String, db:ProxyPerRequestSessionDb) : FieldSet = {
    var result = db.fieldSetMap.get(fieldSetId)
    if (result == null) {
      val fs = logSpace.getFieldSet(fieldSetId)
      if (fs != null) {
        result = fs;
        db.fieldSetMap.put(fieldSetId, result)
      } else {
        result = FieldSets.getBasicFieldSet
      }
    }
    return result.copy()
  }
  def getFieldSet(request:String) : FieldSet = {
    val requestId = fixRequestId(request)

    val sessionDb: ProxyPerRequestSessionDb = getDb(requestId)
    if (sessionDb.fieldSetMap.size() > 0) {
      sessionDb.fieldSetMap.values().iterator().next();
    } else {
      warn("FieldSet not found:" + requestId)
      FieldSets.getBasicFieldSet
    }
  }

  def close(request:String) {
    try {

      val requestId = fixRequestId(request)

      if (!new java.io.File(parentDir, requestId).exists()) return;
      info("Closing:" + requestId)
      getDb(requestId).close

    } catch {
      case t:Throwable => {
        error("Failed to Close DB:" + t, t)
      }
    }
  }
  def delete(request:String) {
    val requestId = fixRequestId(request)
    info("Delete:" + requestId)
    removeCsv(requestId)
    getDb(requestId).delete
    dbMap.remove(requestId)
    scheduler.schedule(new Runnable() {
      def run(){
        val files:Array[java.io.File] = parentDir.listFiles()
        files.iterator.toList.foreach(f => {
          if (f.getName().contains(requestId)) {
            f.delete();
          }
        })
      }
    }, 1, TimeUnit.MINUTES);


  }


  def getPage(request:String, start:Int, length:Int, filter:String, sortColumn:Integer, sortField:String, ascending:Boolean, attemptMode:ReplayEvent.Mode, baseUrl: String, fromMs: Long, toMs: Long) =  {
    val requestId = fixRequestId(request)

    var mode = attemptMode;
    val db = getDb(requestId)
    val builder: StringBuilder = mutable.StringBuilder.newBuilder
    var totalSize:Int = 0
    var displayRecords = 0;
    builder += '['

    if (db != null) {
      // val keySet: util.NavigableSet[Long] = db.keySet()
      val fs = getFieldSet(requestId).copy()

      val events = db.events


      val iterator = DbSorter.getIterator(events, sortColumn, sortField, ascending, fs, mode, fromMs, toMs)
      var i = 0
      var hits = 0

      totalSize = db.totalSize.intValue()

      var mm : jregex.Matcher = null;
      if (filter.contains(".*")) {
        mm = new Pattern(filter).matcher();
      }

      while(iterator.hasNext) {
        i +=1

        var str:String = "";
        val next = iterator.next();

        val event: ReplayEvent = events.get(next)
        if (event.isSubstringMatch(filter)) {
          displayRecords += 1

          if (hits < length && i >= start) {
            if (event != null) {
              try {

                str = event.toJson(i, null, getFieldSetForEvent(event.getDefaultField(FieldSet.DEF_FIELDS._type), db), mode, baseUrl, mm)
                hits += 1
              } catch {
                case e: Throwable => e.printStackTrace()
              }
            }

          }
          if (str != null) {
            if (i >= start && hits < length && str.length > 0){
              if (builder.length > 1 && builder.charAt(builder.length-1) != ',') builder += ','
              builder.append(str)

            }
          }
        }
      }
      if (filter == null || filter.length == 0) displayRecords = totalSize


    }
    builder += ']'
    (totalSize, displayRecords, builder.toString())
  }
  def removeCsv(request:String) {
    new java.io.File(printQ + request + ".csv").delete()

  }

  def fixRequestId(request:String) = {
    request.replaceAll(":","").replaceAll("/","_")
  }
  def getDate() = {
    DateUtil.shortDateFormat.print(System.currentTimeMillis)
  }

  def dumpCsvWriter(request:String, writer:PrintWriter): Unit = {
    val requestId = fixRequestId(request)
    writer.write(dumpCsv(request, requestId + ".csv"))
    writer.flush

  }
  def dumpCsv(request:String, file:String): String = {

    val filename = printQ + "/" + getDate + "/" + file;

    val requestId = fixRequestId(request)

    val events = getDb(requestId).events
    if (events.size() == 0) {
      return filename
    };
    var fs = getFieldSet(requestId)
    if (fs == null) fs = FieldSets.getBasicFieldSet
    val iterator = events.keySet().iterator()
    new java.io.File(filename).getParentFile.mkdirs()
    info(" CSV Dump:" + new java.io.File(filename).getAbsolutePath + " items: " + getDb(requestId).totalSize)

    val fos = new BufferedOutputStream(new FileOutputStream(filename));
    fos.write("host, path, filename, _tag, _type, time, lineNumber, msg\n".getBytes)
    var written = 0
    try {
      while (iterator.hasNext){

        val event = events.get(iterator.next())
        fos.write(event.toCSV(fs).getBytes)
        written = written + 1
      }
    } catch {
      case t : Throwable => {
        error("Failed to write CSV Event:" + t, t)
      }
    }

    fos.close();
    scheduler.schedule(new Runnable() {
      def run() {
        try {
          new java.io.File(filename).delete()
        } catch {
          case _ : Throwable => {
            info("Delete File Failed:" + filename)
          }
        }

      }
    }, 15, TimeUnit.MINUTES);
    "reports/" + getDate + "/" + file;
  }

  def commit(requestId:String) {
    //db.commit();

  }
}
