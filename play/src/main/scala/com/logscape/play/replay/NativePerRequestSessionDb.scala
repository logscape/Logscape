package com.logscape.play.replay

import org.mapdb.{BTreeMap, DB}
import org.fusesource.scalate.util.Logging
import scala.collection.JavaConversions._
import com.liquidlabs.log.LogProperties
import java.util.concurrent.atomic.AtomicInteger

import com.liquidlabs.log.search.{TimeUID, ReplayEvent}
import com.liquidlabs.log.fields.FieldSet
import javolution.util.FastMap
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService}
import com.liquidlabs.log.space.agg.SimpleNavgableMap

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 27/06/14
 * Time: 21:12
 * To change this template use File | Settings | File Templates.
 */
class NativePerRequestSessionDb(requestId:String,  scheduler:ScheduledExecutorService) extends Logging with PerRequestSessionDb  {

  val valueCache : java.util.Map[String,String] = new ConcurrentHashMap[String, String]();
  val eventsMap : java.util.Map[TimeUID, ReplayEvent]   = new SimpleNavgableMap[TimeUID, ReplayEvent](new ConcurrentHashMap[TimeUID,ReplayEvent]())
  val totalSize =  new AtomicInteger
  val hostsMap = new FastMap[String, AtomicInteger].shared()
  val fieldSetMap = new FastMap[String, FieldSet].shared()
  val maxStored = LogProperties.getMaxEventsStoreOnServer
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
    val task = new Runnable() {
      def run() {
        if (events.size() == 0) return
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
                event.cacheValues(valueCache);
                eventsMap.put(event.getId, event)
                totalSize.incrementAndGet();
              }
            }

          })
      }
    }
    task.run();

  }
  def delete {

    scheduler.submit(new Runnable() {
      def run() {
        try {
          eventsMap.clear
          fieldSetMap.clear
          hostsMap.clear
          valueCache.clear

        } catch {
          case e : Exception =>  error("Failed to cleanup DB:" + requestId);

        }

      }
    })

  }

}
