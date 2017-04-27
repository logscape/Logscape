package com.logscape.play.servlet

import javax.servlet.http.{HttpSessionEvent, HttpSessionListener}
import com.logscape.play.websocket.PullConnection
import org.fusesource.scalate.util.Logging
import com.liquidlabs.admin.User

class TheSessionListener extends HttpSessionListener with Logging{
  def sessionCreated(p1: HttpSessionEvent) {
    val user = theUser(p1)
    if (user == null) {
      //error("No USER IN SESSION")
    } else {
      info("Session created got user: " + user)
    }

  }


  private def theUser(p1: HttpSessionEvent): User = {
    p1.getSession.getAttribute("uid").asInstanceOf[User]
  }

  def sessionDestroyed(p1: HttpSessionEvent) {
    val session = p1.getSession
    val names = session.getAttributeNames
    info("Session destroyed for user: " + theUser(p1))
    while (names.hasMoreElements) {
      val x = names.nextElement()
      x match {
        case a if x.endsWith("/connection") => session.getAttribute(x).asInstanceOf[PullConnection].close()
        case b if x.endsWith("/fallbackSocket") => session.getAttribute(x).asInstanceOf[PlayWebSocket].onClose(1, "Session expired")
        case _ => {}
      }

    }
  }
}
