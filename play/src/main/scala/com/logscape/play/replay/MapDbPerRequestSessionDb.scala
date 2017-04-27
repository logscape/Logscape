package com.logscape.play.replay

import org.mapdb.{BTreeKeySerializer, BTreeMap, Fun, DB}
import org.fusesource.scalate.util.Logging
import scala.collection.JavaConversions._
import com.liquidlabs.log.LogProperties
import java.util.concurrent.atomic.AtomicInteger

import com.liquidlabs.log.search.{TimeUIDKeySerializer, TimeUID, ReplayEvent}
import com.liquidlabs.log.fields.FieldSet
import javolution.util.FastMap
import java.util.concurrent.{TimeUnit, ScheduledExecutorService}
import gnu.trove.set.hash.TLongHashSet

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 27/06/14
 * Time: 21:12
 * To change this template use File | Settings | File Templates.
 */
class MapDbPerRequestSessionDb(requestId:String, db:DB, scheduler:ScheduledExecutorService) extends Logging with PerRequestSessionDb  {
  private val maker = db.createTreeMap(requestId)//.keySerializer(new TimeUIDKeySerializer)
  val eventsMap : BTreeMap[TimeUID, ReplayEvent]   = maker.makeOrGet[TimeUID, ReplayEvent]
  val totalSize =  new AtomicInteger
  val hostsMap = new FastMap[String, AtomicInteger].shared()
  val fieldSetMap = new FastMap[String, FieldSet].shared()
  val maxStored = LogProperties.getMaxEventsStoreOnServer
  def HOST_LIMIT = LogProperties.getMaxEventsPerHostSource

  val incoming = new java.util.concurrent.LinkedBlockingDeque[java.util.List[ReplayEvent]]();

  val populateMapDB = new Runnable() {
    def run() {

      val events = incoming.poll(50, TimeUnit.MILLISECONDS);
      if (events != null) {}

        events.listIterator.foreach(
          (event: (ReplayEvent)) => {
            if (totalSize.get() <= maxStored) {
              val hostname =      event.getDefaultField(FieldSet.DEF_FIELDS._host)
              var hhh:AtomicInteger = hostsMap.get(hostname)
              if (hhh == null) {
                hostsMap.put(hostname, new AtomicInteger)
                hhh = hostsMap.get(hostname)
              }
              if (hhh.incrementAndGet < HOST_LIMIT) {
                event.setSubscriber("");
                eventsMap.put(event.getId, event)
                totalSize.incrementAndGet();
              }
            }

          })
      }
  }

  scheduler.scheduleWithFixedDelay(populateMapDB, 10, 10, TimeUnit.MILLISECONDS);



  def addFieldSet(fieldSet:FieldSet) {
       fieldSetMap.put(fieldSet.getId, fieldSet)
  }
  def close() {
    delete

  }
  def events() = {
    eventsMap
  }

  def addEvents(events: java.util.List[ReplayEvent])  {
    incoming.add(events);
  }
  def delete {

    scheduler.submit(new Runnable() {
      def run() {
        try {
          eventsMap.clear
          fieldSetMap.clear
          hostsMap.clear
          db.delete(requestId)

        } catch {
          case e : Exception =>  error("Failed to cleanup DB:" + requestId);

        }

      }
    })

  }

}
