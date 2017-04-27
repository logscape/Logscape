package com.logscape.play.servlet

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import org.fusesource.scalate.util.Logging
import com.logscape.play.jmx.Jmx
import com.logscape.play.Json._
import com.codahale.jerkson.Json._

class JmxServlet extends HttpServlet with Logging{

  System.setProperty("sun.rmi.transport.tcp.responseTimeout", "1000");

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {

    def toParams(json:String) = {
      parse[Array[Any]](json)
    }

    val url: String = req.getParameter("url")
    val beanName: String = req.getParameter("bean")
    var jmx:Jmx = null
    try{
      jmx = Jmx(url)
      resp.getOutputStream.print(toJson(req.getPathInfo match {
        case "/attribute" => jmx.getAttributeValue(beanName, req.getParameter("attribute"))
        case "/operation" => jmx.getOperation(beanName, req.getParameter("operation"))
        case "/invoke" => jmx.invokeOperation(beanName, req.getParameter("operation"),toParams(req.getParameter("params")))
        case _ => "UnknownOperation"
      }))

    } catch {
      case e:Exception => {
        //e.printStackTrace
        resp.sendError(777, "JMX Request Endpoint:" + url + "/" + beanName + " Failed [Check your URL] Message:" + e.toString)
      }
    } finally {
      if(jmx != null) jmx.close()
    }
  }
}
