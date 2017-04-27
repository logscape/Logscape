package com.logscape.play

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.codahale.jerkson.Json._
import scala.collection.JavaConversions._
import java.util
import com.logscape.play.jmx.Jmx
import com.logscape.play.Json.toJson

@RunWith(classOf[JUnitRunner])
class SomeTest extends FunSuite {
    test("some shit") {
      val json = "{\"event\":\"saveAlert\",\"data\":{\"name\":\"Damian\",\"search\":\"Demo\",\"schedule\":\"* * * * *\",\"dataGroup\":\"admin\",\"enabled\":true,\"realTime\":true,\"actions\":{\"EmailAction\":{\"from\":\"\",\"to\":\"\",\"subject\":\"\",\"message\":\"\"},\"LogAction\":{},\"ScriptAction\":{\"value\":\"1\"},\"FileAction\":{\"value\":\"\"}},\"trigger\":{\"NumericTrigger\":{\"value\":\"\"},\"ExpressionTrigger\":{\"value\":\"\"},\"CorrelationTrigger\":{\"timeWindow\":\"\",\"corrType\":\"sequence\",\"eventValue\":\"\",\"correlationField\":\"\",\"correlationKey\":\"\"}}}}"
      val map = parse[Map[String, Any]](json)
      val data  = map("data").asInstanceOf[util.LinkedHashMap[String, Any]].toMap
      val actions  = data("actions").asInstanceOf[util.LinkedHashMap[String,util.LinkedHashMap[String,Any]]].toMap
      val trigger  = data("trigger").asInstanceOf[util.LinkedHashMap[String, util.LinkedHashMap[String,Any]]].toMap
      val numeric = trigger("NumericTrigger")
      println("Script:" + actions("ScriptAction").get("value"))
      println(actions)
      println(trigger)
    }

    val url = "service:jmx:rmi:///jndi/rmi://localhost:8989/jmxrmi"
    test("jmx") {
      val results = Jmx(url).listAttributes("com.liquidlabs.vspace.Space:id=AdminSpace-SPACE")
      println(toJson(results))
    }

    test("get attribute value") {
      println(toJson(Jmx(url).getAttributeValue("com.liquidlabs.vspace.Space:id=AdminSpace-SPACE", "MaxResultValue")))
    }

    test("get operations") {
      val operations: Any = Jmx(url).getOperations("com.liquidlabs.vspace.Space:id=AdminSpace-SPACE")
      println(toJson(operations))
    }

    test("get operation") {
      val operations: Any = Jmx(url).getOperation("com.liquidlabs.vspace.Space:id=AdminSpace-SPACE", "setMaxResultValue")
     println(toJson(operations))
    }

  test("invoke operation") {
     println(toJson(Jmx(url).invokeOperation("com.liquidlabs.vspace.Space:id=AdminSpace-SPACE", "setMaxResultValue",Array(2048))))
   }
}
