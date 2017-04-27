package com.logscape.play.servlet

import org.eclipse.jetty.websocket.WebSocketServlet
import org.eclipse.jetty.websocket.WebSocket.OnTextMessage
import com.logscape.play.deployment.Deployment
import com.liquidlabs.admin.User
import javax.servlet.http.HttpSession

class WorkspaceWebSocketServlet extends WebSocketServlet with PlayWebSocketServlet with Services {
  def createWebSocket(user: User, clientId: String, session:HttpSession, clientIp:String): OnTextMessage =
    new WorkspaceWebSocket(clientId, user, logSpace)
}
