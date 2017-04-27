package com.logscape.play.servlet

import org.eclipse.jetty.websocket.WebSocketServlet
import org.eclipse.jetty.websocket.WebSocket.OnTextMessage
import com.liquidlabs.admin.User
import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpSession}

class SearchWebSocketServlet extends WebSocketServlet with PlayWebSocketServlet with Services {
  def createWebSocket(user: User, clientId: String, session:HttpSession, clientIp:String): OnTextMessage = {
    val socket = new SearchWebSocket(clientId, user, clientIp)
    session.setAttribute("searchWebSocket", socket)
    socket
  }

}
