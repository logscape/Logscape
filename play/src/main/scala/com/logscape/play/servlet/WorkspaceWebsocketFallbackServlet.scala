package com.logscape.play.servlet

import org.fusesource.scalate.util.Logging
import com.liquidlabs.admin.User

class WorkspaceWebsocketFallbackServlet extends FallbackServlet with Logging with Services {
  def webSocket(user: User, clientId: String, clientIp:String): PlayWebSocket = new WorkspaceWebSocket(clientId, user, logSpace)
}
