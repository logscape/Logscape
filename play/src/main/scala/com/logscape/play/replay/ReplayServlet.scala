package com.logscape.play.replay

import java.net.URL
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.liquidlabs.common.ExceptionUtil
import com.liquidlabs.common.net.URI
import com.liquidlabs.dashboard.server.DashboardProperties
import com.liquidlabs.log.search.ReplayEvent
import com.logscape.play.servlet.Services
import org.fusesource.scalate.util.Logging


class ReplayServlet extends HttpServlet with Logging with Services {

  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    val eStart = System.currentTimeMillis();
    try {
      val slickGrid = request.getParameter("isSlickGrid")
      if (slickGrid != null) {
        return handleSlickGrid(request, response);
      }
      val isExportToCsv: String = request.getParameter("exportToCsv");
      val eventId: String = request.getParameter("requestEventJson");
      if (isExportToCsv != null) {
        exportToCsv(request, response)
      } else if (eventId != null) {
        getEventJson(eventId, request, response)
      } else {
        getPage(request, response)
      }
    } catch {
      case e: Exception => {
        warn(e.toString,e)
        warn(ExceptionUtil.stringFromStack(e,-1));
        println(ExceptionUtil.stringFromStack(e,-1))
      }

    } finally {
     // info("Request ElapsedMs:" + (System.currentTimeMillis()  - eStart))
    }
  }
  def getEventJson(eventId: String, request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val requestId: String = request.getParameter("requestId")
    val baseUrl = getBaseURL(request.getRequestURL.toString)
    val json = eventsDatabase.getEventAsJson(requestId, eventId, baseUrl)
    response.getWriter.write(json);

  }


  def getPage(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val requestId: String = request.getParameter("requestId")
    val start = request.getParameter("iDisplayStart")
    val length = request.getParameter("iDisplayLength")
    val filter = request.getParameter("sSearch")
    val sortCol = request.getParameter("iSortCol_0")
    val sortDir = request.getParameter("sSortDir_0")
    val sortField = request.getParameter("mDataProp_" + sortCol)
    val mode = ReplayEvent.Mode.valueOf(request.getParameter("mode")) // raw/events
    val fromMs = request.getParameter("fromMs") // raw/events
    val toMs = request.getParameter("toMs") // raw/events

    val baseUrl = getBaseURL(request.getRequestURL.toString)



    val records = eventsDatabase.getPage(requestId, start.toInt, length.toInt, filter, sortCol.toInt, sortField, sortDir == "asc", mode, baseUrl, fromMs.toLong, toMs.toLong)
    response.setCharacterEncoding(System.getProperty("file.encoding"))
    response.getWriter.write("{ \"sEcho\": " + request.getParameter("sEcho") + ",\n")
    response.getWriter.write(" \"iTotalRecords\": " + records._1 + ",\n")
    response.getWriter.write(" \"iTotalDisplayRecords\": " + records._2 + ",\n")
    response.getWriter.write(" \"aaData\": " + records._3 + "}")
    response.getWriter.flush()
  }

  def exportToCsv(request: HttpServletRequest, response: HttpServletResponse) = {
    val requestId: String = request.getParameter("requestId")
    eventsDatabase.dumpCsvWriter(requestId, response.getWriter)
  }
  def handleSlickGrid(request: HttpServletRequest, response: HttpServletResponse) = {
    val requestId: String = request.getParameter("requestId")
    val mode = ReplayEvent.Mode.valueOf(request.getParameter("mode")) // raw/events
    val baseUrl = getBaseURL(request.getRequestURL.toString)
    val records  = eventsDatabase.getPage(requestId, 0, Integer.MAX_VALUE, "", -1, "", true, mode, baseUrl, 0, Long.MaxValue)
    response.getWriter.write("{ \"data\": " +records._3 + "}");
  }

  def getBaseURL(url:String) = {
    if(!DashboardProperties.isProxyTailers) ""
    else {
      val baseUrl = new URI(getUrl(url))

      val host = new URL(url).getHost
      val base = new URI(baseUrl.getScheme, baseUrl.getUserInfo, host, baseUrl.getPort, baseUrl.getPath, baseUrl.getQuery, baseUrl.getFragment)
      base.toString
    }

  }

  // check here if proxying is on
  def getUrl(url:String) =
    if(!DashboardProperties.isProxyTailers) ""
    else url match {
        case x if x.startsWith("https") => DashboardProperties.getHttpsUrl + "/play/proxy?url="
        case x if x.startsWith("http") => DashboardProperties.getHttpUrl + "/play/proxy?url="
  }
}
