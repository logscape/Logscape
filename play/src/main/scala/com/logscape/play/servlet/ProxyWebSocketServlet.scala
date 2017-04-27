package com.logscape.play.servlet

import org.eclipse.jetty.websocket.{WebSocket, WebSocketServlet}
import javax.servlet.http.HttpServletRequest
import com.logscape.play.websocket.ProxyWebSocket

class ProxyWebSocketServlet extends WebSocketServlet{
  def doWebSocketConnect(p1: HttpServletRequest, p2: String): WebSocket = new ProxyWebSocket
}
