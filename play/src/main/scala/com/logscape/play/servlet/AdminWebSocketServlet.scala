package com.logscape.play.servlet

import java.io.File

import com.liquidlabs.common.file.FileUtil
import org.eclipse.jetty.websocket.WebSocketServlet
import org.eclipse.jetty.websocket.WebSocket.OnTextMessage
import com.logscape.play.deployment.{Users, Agents, Deployment}
import com.liquidlabs.admin.User
import javax.servlet.http.HttpSession

class AdminWebSocketServlet extends WebSocketServlet with PlayWebSocketServlet with Services {
  def createWebSocket(user: User, clientId: String, session:HttpSession, clientIp:String): OnTextMessage = {

    val adminSocket = new AdminWebSocket(clientId, logSpace, user,
            new Deployment(user),
            new Agents(resourceSpace, proxyFactory),
            new Users(user, adminSpace), proxyFactory, resourceSpace,adminSpace)
    session.setAttribute("adminWebSocket", adminSocket)
    adminSocket
  }
}
