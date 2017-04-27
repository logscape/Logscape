package com.logscape.play.servlet

import scala.collection.JavaConversions._
import com.logscape.play.event.ReplayHandler
import java.util.Arrays
import org.joda.time.DateTime
import com.logscape.play.model.{SearchName, SearchErrors, Chart, Histogram, SearchNames, Search}
import com.liquidlabs.log.space.{ReplayType, Replay, LogRequest}
import com.logscape.play.Json._
import com.liquidlabs.log.{LogProperties, LogRequestBuilder}
import com.liquidlabs.log.space.agg.HistoManager
import com.liquidlabs.admin._
import java.util
import com.liquidlabs.common.UID
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import org.apache.log4j.Logger
import com.liquidlabs.vso.resource.BloomMatcher
import scala.collection.mutable.ListBuffer


class SearchWebSocket(clientId: String, user: User, clientIp:String) extends PlayWebSocket with Services {
  def uid = user.username
  val maxReplayItems = LogProperties.getMaxEventsRequested
  val outstandingRequests = collection.mutable.Map[String, (String, ReplayHandler)]()
  def auditLogger = Logger.getLogger("AuditLogger")


  def processMessage(event: String, payload: Map[String, Any], json: String, uuid: String): Option[String] = {
    def actions():Option[String] = {
      event match {
        case "closeConnection" => {
          cancelSearch(uuid, true)
          closeConnection()
          listSearches(uuid)
        }
        case "listSearches" => listSearches(uuid)
        case "cancelSearch" => cancelSearch(uuid, false)
        case "openSearch" => openSearch(payload("searchName").toString, uuid)
        case "deleteSearch" => deleteSearch(payload("searchName").toString)
        case "saveSearch" => saveSearch(payload("searchName").toString, payload("terms").asInstanceOf[java.util.ArrayList[String]], uuid)
        case "extendSearch" => extendSearch(uuid, payload("from").asInstanceOf[Long],  payload("to").asInstanceOf[Long])
        case "search" => executeSearch(uuid, payload("name").toString, payload("terms").asInstanceOf[util.List[String]], payload("page").asInstanceOf[Int], payload("from").asInstanceOf[Long], payload("to").asInstanceOf[Long],
          payload("summaryRequired").asInstanceOf[Boolean], payload("eventsRequired").asInstanceOf[Boolean], clientIp)

      }
    }
    val requiredPerm = event match {
      case "deleteSearch" => Permission.Write
      case "saveSearch" => Permission.Write
      case _ => Permission.Read
    }
    executeWithPermission(requiredPerm, user.getPermissions, event, uuid, actions)
  }

  private def listSearches(uuid: String) = Some(toJson(SearchNames(logSpace.listSearches(user).toList, uuid)))

  private def openSearch(name: String, uuid: String) = {
    val searchName = name.substring(name.indexOf(":") + 1).replaceAll("%20", " ")
    val s: com.liquidlabs.log.space.Search = logSpace.getSearch(searchName, user)
    if (s == null) {
      error("Failed to load search: " + searchName)
      None
    } else Some(toJson(Search(searchName, s.patternFilter.toList, uuid)))
  }

  private def saveSearch(name: String, terms: java.util.ArrayList[String], uuid:String) = {
    auditLogger.info("event:SAVE_SEARCH " + user.ndc + " against:" + name)
    val searchName = logSpace.saveSearch(new com.liquidlabs.log.space.Search(name, "ls-play", terms, "", Arrays.asList(1), 60, ""), user)
    if (!name.equals(searchName)) {
      Some(toJson(SearchName(searchName, uuid)))
    } else {
      None
    }


  }

  private def deleteSearch(name: String) = {
    auditLogger.info("event:DELETE_SEARCH " + user.ndc + " against:" + name)
    logSpace.deleteSearch(name, user)
    None
  }


  private def cancelSearch(uuid: String, removeEvents:Boolean) = {
    val lastRequest = outstandingRequests.getOrElse(uuid, ("NONE", null))
    auditLogger.info("event:CANCEL_SEARCH " + user.ndc + " against:" + lastRequest._1)

    scheduler.submit(new Runnable() {
      def run() {
        logSpace.cancel(lastRequest._1)
        if (removeEvents) {
          eventsDatabase.close(lastRequest._1)
          eventsDatabase.delete(lastRequest._1)
        }
        if (lastRequest._2 != null) {
          lastRequest._2.close()
          proxyFactory.unregisterMethodReceiver(lastRequest._2)

        }
      }
    })


    if (outstandingRequests.size > 10) {
      var removeIds = new ListBuffer[String]
      outstandingRequests.foreach {
        entry =>
          var replayHandler = entry._2._2
          //          if (replayHandler.lastStatus < System.currentTimeMillis() - 5 * 60 * 1000) {
          if (replayHandler.cancelled ||
            replayHandler.lastEvent < System.currentTimeMillis() - 5 * 60 * 1000) {
            removeIds += entry._1
            info("Cancel:" + entry._2._1)
            logSpace.cancel(entry._2._1)
            entry._2._2.close()
            proxyFactory.unregisterMethodReceiver(entry._2._2)
          }

      }

      removeIds.foreach( entry  => outstandingRequests -= entry  )

    }
    None
  }

  private def extendSearch(uuid: String, fromTimeL: Long, toTimeL: Long) : Option[String] = {
    // copy the set of request and give a new ID and fromTime, toTime
    info("ExtendingSearch:" + new DateTime(fromTimeL) + "-" + new DateTime(toTimeL))
    if (fromTimeL == toTimeL) {
      return None
    }

    val replayHandler : ReplayHandler = outstandingRequests.getOrElse(uuid, ("NONE", null))._2
    val histogram : Histogram = replayHandler.theHistogram

    val logRequests = histogram.requestMap.values();
    // val requestId = logRequests.iterator().next().subscriber() + "-extend-" + fromTimeL;
    val logRequestsExtend : util.List[LogRequest] = new util.ArrayList[LogRequest]
    val  builder = new LogRequestBuilder
    logRequests.foreach(request => {
      val copy : LogRequest = request.copy(fromTimeL, toTimeL);
      copy.setBucketCount(builder.getBuckets(copy.queries(), fromTimeL, toTimeL, builder.getBucketMultiplier(copy.queries())))
      copy.setSubmittedTime(System.currentTimeMillis());
      logRequestsExtend.add(copy)
    })

    logRequestsExtend.foreach(request => searcher.executeSimpleSearch(request, replayHandler))
    None
  }

  val requests = new AtomicLong
  private def executeSearch(uuid: String, name:String, terms: util.List[String], page: Int, fromTimeL: Long, toTimeL: Long, summaryRequired: Boolean, eventsRequired: Boolean, clientIp: String) :Option[String] = {
    cancelSearch(uuid,true)

    val requestId: String = (clientId + "-" + user.username() + "-" + name + "-" + requests.getAndIncrement()).replaceAll(" ", "").replaceAll(":","_")

    val fromTime: DateTime = new DateTime(fromTimeL)
    val toTime: DateTime = new DateTime(toTimeL)


    val dg: DataGroup = adminSpace.getDataGroup(user.getDataGroup, true)

    var bloom: BloomMatcher = null;

    if (dg != null) {
      user.setDataGroup(dg)
      bloom = resourceSpace.expandGroupIntoBloomFilter(dg.getResourceGroup, LogRequestBuilder.getResourceGroup(terms));
    } else {
      error("Didnt find DataGroup:" + user.getDataGroup)
    }
    val logRequests = buildRequest(fromTime, toTime, terms, requestId, summaryRequired, eventsRequired, clientIp, bloom)


    auditLogger.info("event:REQUEST " + user.ndc + " against:" + logRequests)
    val syntaxErrors = logRequests.toList.filter((r:LogRequest) => r.hasErrors).map((request:LogRequest) => request.getErrors).flatten

    if(!syntaxErrors.isEmpty) {
      return Some(toJson(SearchErrors(syntaxErrors, uuid)))
    }

    val histogram = new Histogram(logRequests, logSpace.fieldSets().toList)

    val json: String = toJson(Chart(histogram.startTimeSec, histogram.intervalTimeSec, histogram.bucketCount, uuid))
    connection.sendMessage(json)


    def sendIt(msg: String) = {
      if (!connection.isOpen) {
        //          replayHandler.cancelled = true
        logSpace.cancel(requestId)
      } else {
        connection.sendMessage(msg)
      }

    }

    val replayHandler = new ReplayHandler(sendIt, requestId, user, auditLogger, name.replaceAll("\\s+","_"), histogram, new HistoManager(), logSpace, scheduler, eventsDatabase, uuid)
    logRequests.foreach(request => searcher.executeSimpleSearch(request, replayHandler))

    outstandingRequests.put(uuid, (requestId, replayHandler))
    None
  }


  def buildRequest(fromTime: DateTime, toTime: DateTime, terms: util.List[String], requestId: String, collectSummary: Boolean, eventsRequired: Boolean, clientIp:String, bloom:BloomMatcher): util.List[LogRequest] = {

    // extract ofset terms
    val results = new util.ArrayList[LogRequest]()
    val normalTerms = new util.ArrayList[String]()
    val pos: AtomicInteger = new AtomicInteger();
    terms.foreach(term => if (term.contains("offset(")) {
      val logRequest = new LogRequestBuilder().getLogRequest(requestId + "Offset" + pos.getAndIncrement, util.Arrays.asList(term), "", fromTime.getMillis, toTime.getMillis);
      setAttributes(logRequest, clientIp, eventsRequired, collectSummary)
      logRequest.setHosts(bloom)
      results.append(logRequest)
    } else {
      normalTerms.add(term);
    })
    if (normalTerms.size() > 0) {
      val logRequest = new LogRequestBuilder().getLogRequest(requestId, normalTerms, "", fromTime.getMillis, toTime.getMillis);
      setAttributes(logRequest, clientIp, eventsRequired, collectSummary)
      logRequest.setHosts(bloom)
      results.prepend(logRequest)
    }

    info("Running:" + results)
    results
  }


  def setAttributes(logRequest: LogRequest, clientIp: String, eventsRequired: Boolean, collectSummary: Boolean) {
    logRequest.setSearch(true)
    logRequest.setSummaryRequired(true)
    logRequest.addContext("clientIp", clientIp);
    if (eventsRequired) logRequest.setReplay(new Replay(ReplayType.END, maxReplayItems))
    else logRequest.setReplay(new Replay(ReplayType.END, 0))
    logRequest.setUser(user)
    logRequest.setSummaryRequired(collectSummary)
  }

  def closeConnection() {
    info("Close Connection")
    outstandingRequests.foreach {
      entry =>
        info("Cancel:" + entry._2._1)
        logSpace.cancel(entry._2._1)
        entry._2._2.close()
        proxyFactory.unregisterMethodReceiver(entry._2._2)
    }
  }

}


