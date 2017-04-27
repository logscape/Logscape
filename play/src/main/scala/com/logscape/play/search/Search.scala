package com.logscape.play.search

import com.liquidlabs.log.space._
import org.fusesource.scalate.util.Logging
import org.joda.time.DateTime
import com.liquidlabs.log.search.ReplayEvent
import java.util.Date
import com.logscape.play.event.ReplayHandler
import com.logscape.play.model.Plot
import util.Random
import com.liquidlabs.log.space.agg.Meta

trait Searcher {
  def executeSimpleSearch(request: LogRequest, replayHandler: ReplayHandler)
}

class RealSearcher(aggSpace: AggSpace, logSpace: LogSpace) extends Searcher with Logging {
  def executeSimpleSearch(request: LogRequest, replayHandler: ReplayHandler) {
    request.setSubmittedTime(System.currentTimeMillis())
    aggSpace.search(request, replayHandler.getId, replayHandler)
    logSpace.executeRequest(request)
  }
}

class FakeSearcher extends Searcher {
  val rand = new Random
  def executeSimpleSearch(request: LogRequest, replayHandler: ReplayHandler) {
    request.getStartTime
    val startingAt = new DateTime()
//    replayHandler.send(Chart(startingAt.getMillis / 1000, 60, 60))
    (0 until 60).foreach((f:Int) => replayHandler.send(Plot(0,startingAt.plusMinutes(f).getMillis / 1000,f,"", rand.nextInt(100), new Meta())))
    (0 until 10).foreach((f:(Int)) => send(f, replayHandler))
  }

  def send(i:Int, replayHandler:LogReplayHandler) {
    val rawData = new Date() + " line:" + i + " stuff and stuff"
    val event  =  new ReplayEvent("sourceUri", 0, 0, 0, "subscriber", new DateTime().getMillis, rawData)
    event.setDefaultFieldValues("type", "host", "file", "path", "tag", "agentType","", "0")
    replayHandler.handle(event)
  }
}
