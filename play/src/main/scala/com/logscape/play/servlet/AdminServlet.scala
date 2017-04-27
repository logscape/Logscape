package com.logscape.play.servlet

import javax.servlet.http.{HttpSession, HttpServletResponse, HttpServletRequest, HttpServlet}

import com.liquidlabs.admin.{DataGroup, User}
import com.liquidlabs.log.LogRequestBuilder
import com.liquidlabs.log.explore.DirListToJson
import com.liquidlabs.vso.resource.BloomMatcher
import com.logscape.play.servlet.Services
import org.fusesource.scalate.util.Logging
import com.codahale.jerkson.Json._
import org.json.{JSONArray, JSONObject}
import scala.collection.JavaConversions._
/**
 * Created by neil on 21/03/16.
 */
class AdminServlet extends HttpServlet with Logging with Services  {
  override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {

    var session : HttpSession = request.getSession(true)

    val uid = session.getAttribute("uid")

    val raw = request.getParameter("requestObject")
    val requestObj = parse[Map[String, Any]](raw)

    // convert request to json
    val payload = requestObj("data").asInstanceOf[java.util.HashMap[String, Any]].toMap

    println(raw)





  }
  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {

    var session: HttpSession = request.getSession(true)

    val uid = session.getAttribute("uid").asInstanceOf[User]

    val is = request.getInputStream
    val content = new Array[Byte](is.available());
    is.readLine(content, 0, content.length)
    val raw = new String(content)
    val requestObj = parse[Map[String, Any]](raw)

    // convert request to json
    val target = requestObj("target").asInstanceOf[String]
    val action = requestObj("action").asInstanceOf[String]
    val params = requestObj("params").asInstanceOf[java.util.HashMap[String, Any]].toMap


    target match {
      case "explore" => handleExplore(uid, action, params, response)
      case _ => warn("unknown")
    }

  }

  def handleExplore(uid: User, action: String, params: Map[String, Any], response: HttpServletResponse) = {
    val dg: DataGroup = adminSpace.getDataGroup(uid.getDataGroup, true)
    var bloom: BloomMatcher = null;

    if (dg != null) {
      uid.setDataGroup(dg)
      bloom = resourceSpace.expandGroupIntoBloomFilter(dg.getResourceGroup, "");
    } else {
      error("Didnt find DataGroup:" + uid.getDataGroup)
    }
    action match {
      case "hosts" => exploreHosts(uid, params, response)
      case "dirTree" => exploreDirTree(uid, params, response)
      case "contents" => exploreContents(uid, params, response)
      case _ => warn("unknown")
    }
    response.getWriter.flush()

  }
  def exploreContents(uid:User, params: Map[String, Any], response: HttpServletResponse): Unit = {
    response.getWriter.println("{\n\t\"line\": 100,\n\t\"data\": \"line 1\\nline2\\nline3\\nline4\\nstuff\\nmore stuff\"\n}")

  }
  def exploreHosts(uid:User, params: Map[String, Any], response: HttpServletResponse): Unit = {
    val hosts = servletServices.getLogSpace.exploreHosts(uid)
    response.getWriter.println("[");
    for (x <- hosts) {
      // needs to be proper array
      response.getWriter.println("\"" + x + "\",");
    }
    response.getWriter.println("\"" + "-" + "\"");
    response.getWriter.println("]");
  }
  def exploreDirTree(uid:User, params: Map[String, Any], response: HttpServletResponse): Unit = {
    val host = params("host").asInstanceOf[String]
    val dirs1 = servletServices.getLogSpace.exploreDirs(uid, host)

    val jsonObject = new DirListToJson().convert(dirs1, host);

    response.getWriter.println(jsonObject.toString)


    // similar to this:http://stackoverflow.com/questions/11194287/convert-a-directory-structure-in-the-filesystem-to-json-with-node-js
    /**
     * [{
	"name": "C:",
	"path": "C:",
	"dirs": [{
		"name": "work",
		"path": "c:\\work",
		"files": ["file1.log", "file2.log", "file3.log"]
	}, {
		"name": "home",
		"path": "c:\\home",
		"files": ["file4.log", "file5.log", "file6.log"]
	}, {
		"name": "dev",
		"path": "c:\\dev",
		"files": ["file7"],
		"dirs": [{
			"name": "work",
			"path": "c:\\dev\\work",
			"files": ["dev1.log", "dev2.log", "dev3.log"]
		}]
	}]
}, {
	"name": "D:",
	"path": "D:",
	"dirs": [{
		"name": "access",
		"path": "D:\\access",
		"files": ["acc1.log", "acc2.log", "acc3.log"]
	}]
}]
     */
//    val dirs = "[{\n\t\"name\": \"C:\",\n\t\"path\": \"C:\",\n\t\"dirs\": [{\n\t\t\"name\": \"work\",\n\t\t\"path\": \"c:\\\\work\",\n\t\t\"files\": [\"file1.log\", \"file2.log\", \"file3.log\"]\n\t}, {\n\t\t\"name\": \"home\",\n\t\t\"path\": \"c:\\\\home\",\n\t\t\"files\": [\"file4.log\", \"file5.log\", \"file6.log\"]\n\t}, {\n\t\t\"name\": \"dev\",\n\t\t\"path\": \"c:\\\\dev\",\n\t\t\"files\": [\"file7\"],\n\t\t\"dirs\": [{\n\t\t\t\"name\": \"work\",\n\t\t\t\"path\": \"c:\\\\dev\\\\work\",\n\t\t\t\"files\": [\"dev1.log\", \"dev2.log\", \"dev3.log\"]\n\t\t}]\n\t}]\n}, {\n\t\"name\": \"D:\",\n\t\"path\": \"D:\",\n\t\"dirs\": [{\n\t\t\"name\": \"access\",\n\t\t\"path\": \"D:\\\\access\",\n\t\t\"files\": [\"acc1.log\", \"acc2.log\", \"acc3.log\"]\n\t}]\n}]";
//    response.getWriter.println(dirs)
  }


}
