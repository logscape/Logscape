package com.logscape.play.event

import com.liquidlabs.log.space.{LogReplayHandler, LogSpace}
import com.liquidlabs.log.search.{Bucket, ReplayEvent}
import com.logscape.play.Json._
import com.logscape.play.model.{InitEvents, Facets, Progress, MultiPlot, Histogram, Plot, Facet}
import com.liquidlabs.common.{StringUtil, ExceptionUtil}
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{Seconds, DateTime}
import org.fusesource.scalate.util.Logging
import scala.collection.JavaConversions._
import scala._
import collection.immutable.TreeMap
import java.util
import com.liquidlabs.log.space.agg.HistoManager
import com.liquidlabs.log.fields.{FieldItem, FieldSet, FieldSets}
import com.liquidlabs.log.space.agg.ClientHistoItem
import com.liquidlabs.log.space.agg.ClientHistoItem.SeriesValue
import java.util.concurrent.{ScheduledFuture, ScheduledExecutorService, TimeUnit}
import com.logscape.play.replay.{EventsDatabase}
import org.apache.log4j.Logger
import java.text.NumberFormat
import com.liquidlabs.admin.User
import com.liquidlabs.log.search.functions.CountFaster
import com.logscape.disco.kv.RulesKeyValueExtractor
import com.logscape.disco.indexer.Pair

class ReplayHandler(connection: String => Unit, requestId: String, user:User, auditLogger:Logger,  searchName: String, histogram: Histogram,
                    histogramManager: HistoManager, logSpace: LogSpace, scheduler:ScheduledExecutorService,
                    db:EventsDatabase, uuid:String) extends LogReplayHandler with Logging {

  def getId = requestId
  def theHistogram : Histogram = histogram
  val started : DateTime = new DateTime()

  var summary: Bucket = null
  @volatile
  var sendingFacets: Facets = null

  @volatile
  var sendingHisto:  util.List[ClientHistoItem]  = null

  @volatile
  var initEvent:InitEvents = null


  @volatile
  var cancelled =false

  auditLogger.info("event:SEARCH_START " + user.ndc + " search:" + searchName + " against:" +requestId);

  val futures:scala.collection.mutable.HashMap[String, ScheduledFuture[_]] = new scala.collection.mutable.HashMap[String, ScheduledFuture[_]]()

  scheduleFirst("facets" ,1L ,facets)
  scheduleFirst("histos" ,1L ,histos)
  scheduleFirst("sendEvents", 3L, initEvents)

  futures.put("status", scheduler.scheduleWithFixedDelay(new Runnable() {
    def run() {
      status(provider, subscriber, msg);

    }
  }, 3, 1, TimeUnit.SECONDS));


  // init the db events tree
  db.addEvents(requestId, new util.ArrayList[ReplayEvent])

  def scheduleFirst(name:String, interval:Long, func: () => Long) {
    val future: ScheduledFuture[_] = scheduler.schedule(scheduled(name, func), interval, TimeUnit.SECONDS)
    addFuture(name, future)
  }

  def scheduled(name: String, runMe : () => Long) : Runnable = {
    new Runnable() {
      def run() {
        val me: Long = runMe()
        if(!cancelled){
          val schedule: ScheduledFuture[_] = scheduler.schedule(scheduled(name, runMe), me, TimeUnit.SECONDS)
          addFuture(name, schedule)
        }
      }
    }
  }
  def addFuture(name:String, future:ScheduledFuture[_]) {
    val f:Option[ScheduledFuture[_]] = futures.get(name)
    if (f.isDefined ) {
      f.get.cancel(false)
    };
    futures.put(name, future);
  }

  var facetsSent = 0

  def facets() : Long = {
    if(sendingFacets != null) {
      send(sendingFacets)
      sendingFacets = null
      facetsSent += 1
    }
    if(facetsSent < 5) 1L else 3L
  }

  var histosSent = 0

  def histos() : Long = {
    if(sendingHisto != null) {
      sendChartData(sendingHisto)
      sendingHisto = null
      histosSent += 1
    }
    if(histosSent < 5) 1L else 2L
  }

  var initEventsSent = 0
  def initEvents() : Long = {
    if (initEvent == null && completeReceived && initEventsSent > 3) {
      return 10L
    };
    if (initEvent != null) {
      send(initEvent)
      initEvent = null
      initEventsSent +=1
      if(initEventsSent < 3) 3L else 8L
    } else {
      3L
    }


  }

  def close() {
    cancelled = true
    futures.values.foreach((value:ScheduledFuture[_]) => value.cancel(false))
  }

  def handle(event: ReplayEvent) {
  }

  def send(thing: AnyRef) {
    val msg = toJson(thing)
    connection(msg)
  }


  def handle(event: Bucket) {}

  var gotSum = 0
  def handleSummary(inSummary: Bucket) {
    if (cancelled) {
      println(new DateTime() + " CANCELLED");
      return
    };

    lastEvent = System.currentTimeMillis

    synchronized {
      lastEvent = System.currentTimeMillis

      gotSum += 1

      //println(new DateTime() + " GOT:" + gotSum + " " +  inSummary.getAggregateResult(new CountFaster("_agent","_agent").toStringId, false))

      if (this.summary == null) {
        this.summary = inSummary
      } else {
        histogramManager.handle(summary, inSummary, 1, false, false)
        summary.convertFuncResults(false)
        summary.incrementAggCount()
      }
      //println(new DateTime() + " BUILT:" + gotSum + " " +  summary.getAggregateResult(new CountFaster("_agent","_agent").toStringId, false))


      var fieldSetType = FieldSets.getBasicFieldSet.getId
      val searchTypes : java.util.Map[_,_]  = summary.getAggregateResult(new CountFaster("_type","_type").toStringId, false)
      if (searchTypes != null) {
        if (searchTypes.size() == 1) {
          fieldSetType = searchTypes.keySet().iterator().next().asInstanceOf[String]
        }
      }

      val fieldSet:FieldSet = logSpace.getFieldSet(fieldSetType).copy()
      fieldSet.addDefaultFields(fieldSetType, "", "", "", "", "", "", 0, false)
      val sending : java.util.List[FieldItem] = FieldItem.convert(summary.getAggregateResults, fieldSet, fieldSetType, true)

      summary.getAggregateResults().clear()


      def foo(items: List[FieldItem]) : List[Facet] = {
        if (items.isEmpty) Nil
        else Facet(items.head.fieldName, items.head.funct, Integer.parseInt(items.head.total), items.head.keyValues.toList, items.head.dynamic) :: foo(items.tail)
      }
sendingFacets = Facets(foo(sending.toList.filter((p:FieldItem) => p.total != null)), uuid)
    }
  }


  def handle(providerId: String, subscriber: String, size: Int, netHisto: java.util.Map[String, AnyRef]): Int = {
    if (cancelled) return 1
    lastEvent = System.currentTimeMillis
    synchronized {
      if (!cancelled) {
        val incoming: util.List[util.Map[String, Bucket]] = netHisto.get("histo").asInstanceOf[util.List[util.Map[String, Bucket]]]
        sendingHisto = histogram.handle(incoming);
      }
    }
    1
  }


  private def generateStream(index: Int, list: List[Map[String, Bucket]]): Stream[(Int, Map[String, Bucket])] = {
    if (list.isEmpty) Stream.empty
    else (index, list.head) #:: generateStream(index + 1, list.tail)
  }


  private def sendChartData(histo: util.List[ClientHistoItem]) {
    def points(index: Int, histoItems:List[ClientHistoItem]):List[Plot] =  {
      if (histoItems.isEmpty) List.empty
      else {
        if (histoItems.head.meta != null) histoItems.head.meta.toJSon()

        val treeMap: TreeMap[String, SeriesValue] = TreeMap(histoItems.head.series.toMap.toArray: _*)
        points(index+1, histoItems.tail) ::: treeMap.flatMap((entry:(String,ClientHistoItem.SeriesValue)) =>
          List(Plot(entry._2.queryPos, histoItems.head.getStartTime/1000, index, entry._2.label, StringUtil.roundDouble(entry._2.value), histoItems.head.meta))).toList
      }
    }

    send(MultiPlot(points(0, histo.toList), uuid))
  }


  def time(value: Long, formatter: DateTimeFormatter) = {
    new DateTime(value).toString(formatter)
  }



  def handle(events: java.util.List[ReplayEvent]) = {
    try {
      if (!cancelled){
          lastEvent = System.currentTimeMillis

          db.addEvents(requestId, events)

          val event = events.get(0)
          val fieldSet:FieldSet = logSpace.getFieldSet(event.fieldSetId()).copy()

          db.setFieldSet(requestId, fieldSet)
          if (initEvent == null) {
            fieldSet.getFields(event.getRawData)
            fieldSet.addDefaultFields(event.getDefaultFields);
            val summary:util.List[String] = fieldSet.getFieldNames(false, true, true, false, true)
            if (!summary.get(0).equals("time")) summary.prepend("time")
            val fields: List[String] = getAllFields(fieldSet, events).toList
            val summary2:util.List[String] = fixSummaryFields(summary, fields);
            initEvent = InitEvents(fixRequestId(requestId), summary2, fields, uuid)
          }
      }


    } catch {
      case e: Exception => {
        warn(e.toString,e)
        warn(ExceptionUtil.stringFromStack(e,-1));

        // terminate request
      }

    }
    100;
  }
  def fixSummaryFields(summary:util.List[String], allFields:util.List[String]) : util.List[String] = {
    val fieldNames = new util.ArrayList[String];
    val head = new util.ArrayList[String];
    val tail = new util.ArrayList[String];

    for (x : String <- summary) {
      if (x.startsWith("_")) tail.add(x)
      else head.add(x)
    }



    for (x :String <- allFields) {
      if (x.startsWith("_")) {
        if (!tail.contains(x)) tail.add(0, x)
      } else {
        if (!head.contains(x)) head.add(x)

      }

    }
    fieldNames.addAll(head)
    fieldNames.addAll(tail)


    return fieldNames

  }
  def fixRequestId(request:String) = {
    request.replaceAll(":","").replaceAll("/","_")
  }

  def getAllFields(fs: FieldSet, events: java.util.List[ReplayEvent]) : util.ArrayList[String] = {
    val fieldNames = new util.ArrayList[String];


    val typeCount : util.HashSet[String] = new util.HashSet[String];

    for (event : ReplayEvent <- events) {
      typeCount.add(event.getDefaultField(FieldSet.DEF_FIELDS._type));
    }
    for (event : ReplayEvent <- events) {
      fs.getFields(event.getRawData, -1, -1, event.getTime);
      val eventFieldNames = event.getFieldNames(fs, java.lang.Boolean.valueOf(typeCount.size() == 1))
      for (x <- eventFieldNames) {
        if (!fieldNames.contains(x))   {
          fieldNames.add(x)
        }
      }

    }
    return fieldNames

  }

  var lastStatus = System.currentTimeMillis
  var lastEvent = System.currentTimeMillis
  var completeReceived = false;
  var provider ="";
  var subscriber = "";
  var msg = "";

  def status(provider: String, subscriber: String, msg: String): Int = {
    if (provider == "") return 1;
    this.provider = provider;
    this.subscriber = subscriber;
    this.msg = msg;
    if (cancelled || completeReceived) {
      info("STATUS:" + provider + " :" + msg)
//        println(new DateTime + " Status-Canclled:" + msg + " sub:" + subscriber)
        initEvents()
        histos()
        facets()
        close()
      return 1
    };
    lastEvent = System.currentTimeMillis

    //println(new DateTime + " Status:" + msg)

    val s = Seconds.secondsBetween(started, new DateTime()).getSeconds

    var unit = "s"
    var sString = s + "";
    if (s > 120) {
      sString = (s / 60) + "." + (s % 60)
      unit = "m";
    }


    if (msg.contains("Complete")) {
      completeReceived = true

      def elapsedMs = lastStatus - started.getMillis;

      def kve = new RulesKeyValueExtractor()
      def kvs = kve.getFields(msg)
      var tot = "0"
      kvs.toList.foreach( (f:(Pair)) => {
        if (f.key == "Total") tot = f.value
      })

      auditLogger.info("event:SEARCH_COMPLETE " + user.ndc + "  events:" + tot + " elapsedMs:" + elapsedMs + " search:" + searchName + " durationMins:" + histogram.durationMins());
      def totString = NumberFormat.getInstance().format(Integer.parseInt(tot))

      send(Progress("Complete, Events: " +  totString + " (" + sString + unit + ")", uuid))

      scheduler.schedule(new Runnable() {
        def run() {
          close()
        }
      }, 5, TimeUnit.SECONDS)

    } else {
      lastStatus = System.currentTimeMillis
      send(Progress(msg + " (" + sString + unit + ")", uuid))
    }

    if (msg.contains("Complete") || msg.contains("Expired")) {
      this.provider = "";
      this.subscriber = "";
    }
    return 1;
  }
}
