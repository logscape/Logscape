package com.logscape.play.servlet

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.fusesource.scalate.util.Logging

class ProxyServlet extends HttpServlet with Logging{

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    val client = new HttpClient
    val url = req.getParameter("url") match {
      case x if x.contains("?") => x + "&proxied=true"
      case x => x + "?proxied=true"
    }
    info("Proxying request=" + url);

    val get = new GetMethod(url.replaceAll("\\s+","%20"))
    client.executeMethod(get)
    val requestStream = get.getResponseBodyAsStream
    get.getResponseHeaders.foreach(header => resp.setHeader(header.getName, header.getValue))
    val outputStream = resp.getOutputStream
    val buffer = new Array[Byte](8096)
    Iterator continually requestStream.read(buffer) takeWhile (_ != -1) foreach(outputStream.write(buffer,0, _))
    try {
      requestStream.close()
    }
    catch {
      case unknown : Throwable => debug("whateva", unknown)
    }
  }
}
