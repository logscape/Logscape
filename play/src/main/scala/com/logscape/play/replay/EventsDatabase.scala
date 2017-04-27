package com.logscape.play.replay

import com.liquidlabs.log.search.ReplayEvent
import com.liquidlabs.log.fields.FieldSet
import java.io.PrintWriter

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 09/11/15
 * Time: 08:23
 * To change this template use File | Settings | File Templates.
 */
trait EventsDatabase {


  def close(request:String)
  def delete(request:String)
  def isClosed(request:String): Boolean
  def addEvents(request:String, events: java.util.List[ReplayEvent])
  def addEvent(request: String, event:ReplayEvent)
  def setFieldSet(request:String, fieldSet:FieldSet)
  def getEventAsJson(requestId:String, eventIdString: String, baseURL: String) : String
  def dumpCsvWriter(request:String, writer:PrintWriter): Unit
  def getPage(request:String, start:Int, length:Int, filter:String, sortColumn:Integer, sortField:String, ascending:Boolean, attemptMode:ReplayEvent.Mode, baseUrl: String, fromMs: Long, toMs: Long) : Tuple3[Int, Int, String]
}
