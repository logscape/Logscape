package com.logscape.play.model

import com.liquidlabs.log.space.LogRequest
import org.fusesource.scalate.util.Logging
import com.liquidlabs.log.search.Bucket
import com.liquidlabs.log.space.agg.{HistogramHandler, HistoManager, ClientHistoAssembler, ClientHistoItem}
import java.util
import scala.collection.JavaConversions._

import java.util.HashMap

class Histogram(requests:util.List[LogRequest], fieldSets:List[com.liquidlabs.log.fields.FieldSet]) extends Logging  {

  val histogramManager = new HistoManager()
  val histogramHandlerA = new HistogramHandler()

  val histogram: util.List[util.Map[String, Bucket]] = histogramManager.newHistogram(requests.get(0))
  val requestMap: util.Map[String, LogRequest] = new HashMap[String, LogRequest]()

  requests.foreach( request => {
      requestMap.put(request.subscriber, request)
  })

  /**
   * Used as part of the initial json setup
   */
  var startTimeSec:Int = (histogram.get(0).valuesIterator.next.getStart() / 1000).toInt
  var nextTimeSec:Int = startTimeSec + requests.get(0).getBucketWidthSecs
  var bucketCount:Int = requests.get(0).getBucketCount;
//  if (histogram.size() > 1) nextTimeSec = (histogram.get(1).valuesIterator.next.getStart() / 1000).toInt
  var intervalTimeSec:Int = nextTimeSec - startTimeSec;


  def handle(incoming:util.List[util.Map[String, Bucket]] ): util.List[ClientHistoItem] = {
    val incomingSubscriber = incoming.get(0).values().iterator().next().subscriber()
    val request:LogRequest = requestMap.get(incomingSubscriber)
    val offsetValueMS = request.getOffsetValueMs

    offsetHistogram(incoming, offsetValueMS)

      histogramManager.handle(histogram,incoming,request)
      histogramManager.updateFunctionResults(histogram, false)

      val results : util.List[ClientHistoItem] = getHistogramForClient(request, histogram)
      histogramManager.clearFunctionResults(histogram);
      //results.foreach((ci:(ClientHistoItem)) => ci.fromUTC())
      return results
  }

  def getHistogramForClient(request:LogRequest, histogram:util.List[util.Map[String, Bucket]]) : util.List[ClientHistoItem] = {
		
		// if we have -LIVE- AND it is single bucket request.. - then it would have been turned into a 60 bucket histogram to support scrolling
//		if (request.subscriber().contains("-LIVE-") && request.queries().get(0).fullPattern().contains("buckets(1)")){
//			// we have to aggregate this histogram into a single bucket one.
//			HistoManager histoManager = new HistoManager();
//			LogRequest copy = request.copy();
//			copy.setBucketCount(1);
//			val newHistogram = histoManager.newHistogram(copy);
//			
//			request.removeCopy(copy);
//			histoManager.handle(newHistogram, histogram, request);
//			histogram = newHistogram;
//		}
		
		
		val functionTags = new util.HashMap[Integer,util.Set[String]]();
		val subscriber = request.subscriber();

    // too many can kill the heap
    synchronized {
      val rawResult = new ClientHistoAssembler().getClientHistoFromRawHisto(request, histogram, functionTags, fieldSets);
      histogramHandlerA.getHistogramForClient(subscriber, request, rawResult, functionTags, fieldSets);
    }
	}
  def offsetHistogram(histogram:util.List[util.Map[String, Bucket]], offsetMs:Long) {
      if (offsetMs != 0) {
        histogram.foreach( (item : util.Map[String, Bucket]) => {
          item.values().foreach( (bucket: Bucket) => {
              bucket.adjust(offsetMs * -1)
          })
        })
      }
  }
  def durationMins() : Long = {
    return (histogram.last.values().iterator().next().getEnd - histogram.get(0).values().iterator().next().getStart)/(60 * 1000);

  }
}
