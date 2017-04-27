package com.logscape.play.websocket

import org.eclipse.jetty.websocket.WebSocket.Connection
import com.logscape.play.servlet.FallbackMessages
import java.util

class PullConnection(messageQ : util.Queue[String]) extends Connection {

  @volatile
  var open = true

  def sendMessage(p1: String) {
    messageQ.add(p1)
  }


  def poll() = messageQ.poll()

  def close(p1: Int, p2: String) {}

  def close() {
    open = false
    messageQ.clear()
  }

  def getProtocol: String = ""

  def disconnect() {}

  def isOpen: Boolean = open

  // whatever - we probably don't care
  def sendMessage(p1: Array[Byte], p2: Int, p3: Int) {}

  def setMaxIdleTime(p1: Int) {}

  def setMaxTextMessageSize(p1: Int) {}

  def setMaxBinaryMessageSize(p1: Int) {}

  def getMaxIdleTime: Int = 0

  def getMaxTextMessageSize: Int = 0

  def getMaxBinaryMessageSize: Int = 0
}
