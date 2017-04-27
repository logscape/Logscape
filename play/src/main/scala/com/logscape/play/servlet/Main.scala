package com.logscape.play.servlet

import com.logscape.play.websocket.TheWebSocketClient
import java.net.URI

object Main {
  def main(args: Array[String]) {
    val client = new TheWebSocketClient(new URI("ws://localhost:10004"), null, "/Users/damian/development/java/logscape/run/logscape/work/agent.log")
    client.connect()
   }
}
