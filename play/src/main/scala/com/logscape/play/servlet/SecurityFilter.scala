package com.logscape.play.servlet

import javax.servlet.http.{HttpServletResponse, HttpSession, HttpServletRequest}
import javax.servlet.FilterConfig
import org.fusesource.scalate.util.Logging
import org.apache.log4j.Logger
import scala.collection.JavaConversions._


class SecurityFilter extends java.lang.Object with javax.servlet.Filter with Logging with Services {

  def auditLogger = Logger.getLogger("AuditLogger")

  var filterConfig : FilterConfig    = null

  def init(afilterConfig : FilterConfig)  {
    filterConfig = afilterConfig
    //LOGIN_ACTION_URI = fConfig.getInitParameter("loginActionURI");
  }

  auditLogger.info("event:SYSTEM_BOOT");


  def destroy() {
  }

  def doFilter(req : javax.servlet.ServletRequest, res : javax.servlet.ServletResponse, chain : javax.servlet.FilterChain) : scala.Unit = {

    if (req.isInstanceOf[HttpServletRequest]) {

      val request : HttpServletRequest= req.asInstanceOf[javax.servlet.http.HttpServletRequest]
      val response : HttpServletResponse =  res.asInstanceOf[javax.servlet.http.HttpServletResponse]
      var session : HttpSession = request.getSession(true)

      def acceptEula(path:String) = path.contains("accept-eula")

      if(acceptEula(request.getRequestURI)){
        adminSpace.acceptEula()
        auditLogger.info("event:EULA_ACCEPTED ip:" + request.getRemoteAddr + " / " + request.getRemoteHost)
      }

      if (skipit(request.getRequestURI)){
        chain.doFilter(request, response)
        return
      }


      // Auth user on link http://localhost:8888/play/?Workspace=Home&user=sysadmin&client=printServer#
      val client = request.getParameter("client")
      val user = request.getParameter("user")
      if (client != null && client.equals("printServer")) {
        if (user != null) {
          var format = request.getParameter("format").asInstanceOf[String]
          if (format == null || format.length == 0) format = "png"
          var id = request.getParameter("clientId").asInstanceOf[String]
          if (id == null || id.length == 0) id = System.currentTimeMillis + ""

          session = request.getSession(true);

          def user1  = adminSpace.getUser(user)
          user1.ndc = "user:" +user + "," + client + " format:" + format + " clientId:" + id  +" clientIp:" + req.getRemoteAddr + ",";
          request.getSession.setAttribute("uid", user1)
          auditLogger.info("event:LOGIN_AUTO user:" + user1.username());
        }

      }
      if (session.getAttribute("uid") == null) {
        info("Session is NULL, sessionId: " + session.getId + ", [Did not find uid attr],  Redirecting to Login:" + request.getRequestURI + " ClientPort:" + req.getRemotePort )
        // doesnt work properly....
//        val rd : RequestDispatcher  = request.getRequestDispatcher("error");
//        rd.forward(request, response);
        // this does work and gets the template rendered
        request.setAttribute("error", false)


        val parameterList = request.getParameterMap.flatMap{ case (k, v) => v.headOption.map((k, _))}

        def addRedirect(uri:String) = {
          (!"/play/".equals(uri) && !"/play/?".equals(uri) && !"/play/login".equals(uri)) || !parameterList.isEmpty
        }

        if(addRedirect(request.getRequestURI)) {
          val sb = new StringBuilder
          sb.append("/play/?")

          val workspaceName = parameterList.getOrElse("Workspace", "")
          if(workspaceName != ""){
            sb.append("Workspace=").append(workspaceName)
          }

          val filterValue = parameterList.getOrElse("filter" , "")
          if(filterValue.toString != ""){
            sb.append("&filter=").append(filterValue)
          }


          //parameterMap.foreach((e:(String, Array[String])) => sb.append(e._1).append("=").append(e._2(0)))
          session.setAttribute("redirectTo", sb.toString())
        }
        response.sendRedirect("/play/login")

      } else {
        debug("Session Id: " + session.getId + " last accessed: " + session.getLastAccessedTime + " max inactive interval: " + session.getMaxInactiveInterval)
        chain.doFilter(request, response)
      }
    }
  }
  def skipit(path:String) : Boolean = {
    path.contains("lang") || path.contains("play/jmx") || path.contains("upload") || path.contains("-ws") || path.contains("image") || path.contains("javascript") || path.contains("font") || path.contains("css") || path.contains(".js") || path.contains("play/error")  || path.contains("/play/replay") || path.contains("/play/keep-alive") || path.contains("login") || path.contains("eula")
  }

}

