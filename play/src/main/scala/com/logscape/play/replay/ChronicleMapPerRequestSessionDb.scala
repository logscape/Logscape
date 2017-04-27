package com.logscape.play.replay

import org.mapdb.{BTreeMap, DB}
import org.fusesource.scalate.util.Logging
import scala.collection.JavaConversions._
import com.liquidlabs.log.LogProperties
import java.util.concurrent.atomic.AtomicInteger

import com.liquidlabs.log.search.{TimeUID, ReplayEvent}
import com.liquidlabs.log.fields.FieldSet
import javolution.util.FastMap
import java.util.concurrent.ScheduledExecutorService
import net.openhft.chronicle.map.{ChronicleMap, ChronicleMapBuilder}
import com.liquidlabs.log.space.agg.NavgableMap

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 27/06/14
 * Time: 21:12
 * To change this template use File | Settings | File Templates.
 */
class ChronicleMapPerRequestSessionDb(requestId:String, scheduler:ScheduledExecutorService, chronoMap: ChronicleMap[TimeUID, ReplayEvent]) extends Logging with PerRequestSessionDb {

  val A1_MILLION_ENTRIES = 10 * 100 * 1000;

  val eventsMap : NavgableMap[TimeUID, ReplayEvent] = new NavgableMap[TimeUID, ReplayEvent](chronoMap);
  val totalSize =  new AtomicInteger
  val hostsMap = new FastMap[String, AtomicInteger].shared()
  val fieldSetMap = new FastMap[String, FieldSet].shared()
  val maxStored = A1_MILLION_ENTRIES;//LogProperties.getMaxEventsStoreOnServer
  var isFull = false

  def HOST_LIMIT = LogProperties.getMaxEventsPerHostSource

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

    if (events.size() == 0) return
    if (isFull) return
      events.toList.foreach(
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
              try {
                eventsMap.put(event.getId, event)
              } catch{
                case ise: IllegalStateException => {
                  warn("ChronicleMap is full.")
                  isFull = true
                };
              }

              totalSize.incrementAndGet();
            }
        }

    })

  }
  def delete {

    scheduler.submit(new Runnable() {
      def run() {
        try {
          eventsMap.clear
          fieldSetMap.clear
          hostsMap.clear
          eventsMap.close

        } catch {
          case e : Exception =>  error("Failed to cleanup DB:" + requestId);

        }

      }
    })

  }

}
