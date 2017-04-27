package com.logscape.play.servlet

import org.eclipse.jetty.websocket.WebSocket
import org.fusesource.scalate.util.Logging
import org.eclipse.jetty.websocket.WebSocket.Connection
import com.codahale.jerkson.Json._
import scala.collection.JavaConversions._
import com.liquidlabs.common.ExceptionUtil
import com.liquidlabs.admin.Permission
import com.logscape.play.Json._
import scala.Some
import com.logscape.play.model.UnAuthorised

trait PlayWebSocket extends WebSocket.OnTextMessage with Logging {
  var connection: Connection = null
  val event:String = "event"

  def processMessage(event:String, payload:Map[String, Any], json:String, uuid:String):Option[String]
  def closeConnection()

  def onMessage(json: String) {
    try {
      debug(json)
      val request = parse[Map[String, Any]](json)
      val payload = request("data").asInstanceOf[java.util.HashMap[String, Any]].toMap
      if(request(event).equals("ping")) ping
      else doProcessMessage(request(event).asInstanceOf[String], payload, json)
    } catch {
      case npe: NullPointerException => {
        error("NullPtr Json:" + json, npe)
        error("Stack:" + ExceptionUtil.stringFromStack(npe,-1))
      }
      case unknown : Throwable => {
        unknown.printStackTrace()
        error("Exception happened JSON:" + json, unknown)
        error("Stack:" + ExceptionUtil.stringFromStack(unknown,-1))
      }
    }
  }

  def executeWithPermission(required:Permission, userPerms:Permission, action:String, uuid:String, function: () => Option[String]) = {
    if(userPerms.hasPermission(required)) function()
    else Some(toJson(UnAuthorised(required.getName, action, uuid)))
  }

  private def ping {
    debug("ping")
  }
  private def doProcessMessage(event:String, payload:Map[String, Any], json:String) {
    processMessage(event, payload, json, payload("uuid").asInstanceOf[String]) match {
      case Some(data) => connection.sendMessage(data)
      case None => debug("Processed " + event)
    }
  }



  def onOpen(connection: Connection) {
    this.connection = connection
  }

  def onClose(code: Int, message: String) {
    debug("Connection closed with message: " + message + " and code: " + code)
    closeConnection()
    this.connection = null
  }

}
