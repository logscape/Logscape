package com.logscape.play.servlet

import javax.servlet.http.{HttpSession, HttpServletRequest}
import org.fusesource.scalate.util.Logging
import com.liquidlabs.admin.User
import org.eclipse.jetty.websocket.WebSocket

trait PlayWebSocketServlet extends Logging {

    def createWebSocket(user: User, clientId:String, session:HttpSession, clientIp:String) : WebSocket.OnTextMessage

    def doWebSocketConnect(httpRequest: HttpServletRequest, p2: String) = {
      val clientId = httpRequest.getRemoteHost.replace("%","") + ":" + httpRequest.getRemotePort
      info("WebSocket connected with clientId: " + clientId)// + " S:"+ httpRequest.getSession.toString)
      val user:User = httpRequest.getSession.getAttribute("uid").asInstanceOf[User]
      val attr = httpRequest.getSession.getAttribute("clientIp")
      if (attr == null)  error("Session doesnt have a client IP - bailing out")
      val clientIp:String = attr.asInstanceOf[String].replace("%","")
      if (user == null) error("Holy crap batman the user is null!")
      info("WebSocket connected with user: " + user.username)
      createWebSocket(user, clientId, httpRequest.getSession, clientIp)
    }


}
