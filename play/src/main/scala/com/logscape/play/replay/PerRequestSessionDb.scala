package com.logscape.play.replay

import com.liquidlabs.log.search.{TimeUID, ReplayEvent}
import com.liquidlabs.log.fields.FieldSet
import java.util
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 09/11/15
 * Time: 17:42
 * To change this template use File | Settings | File Templates.
 */
trait PerRequestSessionDb {
  def addEvents(events: java.util.List[ReplayEvent])
  def events : java.util.Map[TimeUID, ReplayEvent]
  def addFieldSet(fieldSet: FieldSet)
  def eventsMap : java.util.Map[TimeUID, ReplayEvent]
  def fieldSetMap : java.util.Map[String, FieldSet]
  def totalSize : AtomicInteger
  def delete
  def close

}
