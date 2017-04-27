package com.logscape.play

import com.codahale.jerkson.Json._
import org.fusesource.scalate.util.Logging

object Json extends Logging {
  def toJson(message: Any) = {
    generate(message)
  }

}
