package com.logscape.play.servlet

import com.liquidlabs.admin.User
import org.fusesource.scalate.util.Logging

class SearchFallbackServlet extends FallbackServlet with Logging with Services {

  override def webSocket(user: User, clientId : String, clientIp: String) = new SearchWebSocket(clientId, user, clientIp)

}
