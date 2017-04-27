package com.logscape.play.servlet

import javax.servlet.http.{HttpSession, HttpServletResponse, HttpServletRequest, HttpServlet}
import com.liquidlabs.admin.User
import com.logscape.play.websocket.PullConnection
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConversions._

trait FallbackServlet extends HttpServlet {

  def webSocket(user: User, clientId: String, clientIp:String): PlayWebSocket

  val fallback = "fallbackMessages"

  def connectionKey(path: String) = fallback + path + "/connection"

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val session: HttpSession = req.getSession(false)
    if (session != null) {
      val user: User = session.getAttribute("uid").asInstanceOf[User]
      val clientId = req.getRemoteHost + ":" + req.getRemotePort

      val webSocketKey = fallback + req.getServletPath + "/fallbackSocket"
      if (session.getAttribute(webSocketKey) == null) {
        val socket = webSocket(user, clientId, req.getRemoteAddr)
        val pullConnection = new PullConnection(new ConcurrentLinkedQueue[String]())
        socket.onOpen(pullConnection)
        session.setAttribute(webSocketKey, socket)
        session.setAttribute(connectionKey(req.getServletPath), pullConnection)
      }
      val socket = session.getAttribute(webSocketKey).asInstanceOf[PlayWebSocket]
      val map: java.util.Map[String, Array[String]] = req.getParameterMap
      socket.onMessage(map("params")(0))
    }
  }

  /**
   * Poll comes in over GET
   */
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    val session: HttpSession = req.getSession(false)
    val msgKey = connectionKey(req.getServletPath)
    if (session != null && session.getAttribute(msgKey) != null) {
      val connection = session.getAttribute(msgKey).asInstanceOf[PullConnection]
      val sendMe = connection.poll()
      if (sendMe != null) {
        val w = resp.getWriter
        w.write(sendMe)
        w.flush
      }
    }
  }
}
