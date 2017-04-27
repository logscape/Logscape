package com.logscape.play.servlet

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 03/07/2013
 * Time: 08:30
 * To change this template use File | Settings | File Templates.
 */
class FallbackMessages {
  val response : java.util.concurrent.ConcurrentHashMap[String,String] = new java.util.concurrent.ConcurrentHashMap[String,String]()
  val push : java.util.concurrent.ConcurrentLinkedQueue[String] = new java.util.concurrent.ConcurrentLinkedQueue[String]()


}
