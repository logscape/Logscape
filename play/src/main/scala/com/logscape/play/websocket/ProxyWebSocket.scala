package com.logscape.play.websocket

import org.eclipse.jetty.websocket.WebSocket
import org.eclipse.jetty.websocket.WebSocket.Connection
import org.fusesource.scalate.util.Logging
import com.codahale.jerkson.Json._
import com.logscape.play.model.Tail
import java.net.URI

class ProxyWebSocket extends WebSocket.OnTextMessage with Logging{
  @volatile
  var connection:Connection = null
  var client: TheWebSocketClient = null

  def onMessage(message: String) {
    debug("json_received:" + message)
    if (message.contains("\"event\":\"ping\"")) {
      debug("proxy was pinged")
    } else {
      val d = "data\":";
      val what = message.substring(message.indexOf(d) + d.length(), message.length -1)
      val tailMessage = parse[Tail](what)

      client = new TheWebSocketClient(new URI(tailMessage.url), connection, tailMessage.file)
      client.connect()
      info("proxy_tailing:" + tailMessage.url + tailMessage.file)
    }
  }

  def onOpen(p1: Connection) {
    this.connection = p1
  }

  def onClose(p1: Int, p2: String) {
    client.onClose(p1,"closed by web client",true)
  }
}
