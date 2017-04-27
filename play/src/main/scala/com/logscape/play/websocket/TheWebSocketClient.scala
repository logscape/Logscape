package com.logscape.play.websocket

import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import java.net.URI
import org.eclipse.jetty.websocket.WebSocket.Connection
import org.java_websocket.drafts.Draft_17
import org.fusesource.scalate.util.Logging

class TheWebSocketClient(uri: URI, connection:Connection, file:String) extends WebSocketClient(uri, new Draft_17) with Logging{
  def onOpen(p1: ServerHandshake) {
      send("{file:\"" + file + "\"}")
  }

  def onMessage(p1: String) {
      connection.sendMessage(p1)
  }

  def onClose(p1: Int, p2: String, p3: Boolean) {
    info("clientwebsocketclosed:"  + p2)
    connection.close()
  }

  def onError(p1: Exception) {
    error("Error in WebSocket proxy:" + p1)
  }
}
