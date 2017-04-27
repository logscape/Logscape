package com.logscape.play.servlet

object Stuff {

  def search(request:Request) = Map("title" -> "Search", "name" -> "JOhn")


  def index(request:Request) = Map("title" -> "Index")

  def keepAlive(request:Request) = Map[String, Any]()
}
