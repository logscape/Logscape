package com.logscape.play.servlet

import org.fusesource.scalate.util.Logging
import com.liquidlabs.admin.User
import com.logscape.play.deployment.{Users, Agents, Deployment}

class AdminWebsocketFallbackServlet extends FallbackServlet with Logging with Services {
  def webSocket(user: User, clientId: String, clientIp:String): PlayWebSocket = new AdminWebSocket(clientId, logSpace, user,
              new Deployment(user),
              new Agents(resourceSpace, proxyFactory),
              new Users(user, adminSpace), proxyFactory, resourceSpace, adminSpace)
}
