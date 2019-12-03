package com.logscape.play.servlet

import javax.servlet.http.{HttpSession, HttpServlet, HttpServletRequest, HttpServletResponse}
import javax.servlet.RequestDispatcher
import com.liquidlabs.services.ServicesLookup
import com.liquidlabs.vso.VSOProperties.ports
import org.fusesource.scalate.util.Logging
import Stuff._
import org.apache.log4j.Logger
import com.liquidlabs.admin.User
import java.lang.Throwable
import scala.Throwable

class LoginServlet extends HttpServlet with Logging
{
  def auditLogger = Logger.getLogger("AuditLogger")
  /**
   * Map of Path -> function(request):
   */
  lazy val routes: Map[String, (Request) => Map[String, Any]] = Map("/search" -> search, "/index" -> index, "/play/keep-alive" -> keepAlive)
  lazy val adminSpace = ServicesLookup.getInstance(ports.DASHBOARD).getAdminSpace

  private def simplePath(path:String) = path match {
    case null => "/index"
    case "/" => "/index"
    case _  => path
  }

  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    val path: String = simplePath(request.getPathInfo)

    val templateValues: Map[String, Any] = routes(path)(new Request(request))

    templateValues.foreach((x:(String, Any)) => request.setAttribute(x._1, x._2))

    val dispatcher: RequestDispatcher = request.getRequestDispatcher(path + ".jade")
    dispatcher.forward(request, response)
  }

  val UUID: String = "uid"

  val REDIRECT: String = "redirectTo"

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    val session = req.getSession(true)
    val loggedIn = session.getAttribute(UUID) != null

    if (req.getRequestURI == "/play/dologin") {
      val user = req.getParameter("Username")
      val pwd = req.getParameter("Password")
      if (loggedIn) {
        info("Already Logged in... USER:" + user)
      }
      // Get client's IP address
      val clientIp = req.getRemoteAddr()
      val clientHost= req.getRemoteHost()

      if (authorise(user, pwd)) {
        val user1: User = adminSpace.getUser(user)
        req.getSession.setAttribute("clientIp", clientIp)
        user1.ndc = "user:" +user  +" clientIp:" + clientIp + ","
        info("Login User:" + user + " Perm:" + user1.getPermissions)
        session.setAttribute(UUID, user1)

        storeUserSession(session, user);


        val redirTo = session.getAttribute(REDIRECT).asInstanceOf[String]
        val reqHeader = req.getRequestURL.toString().replace(req.getRequestURI,"")

       if(redirTo == null || redirTo.equals("/play/?")) {
          resp.sendRedirect(reqHeader + "/play/?Workspace=user.Home#")
        }
        else resp.sendRedirect(reqHeader + redirTo)

        session.removeAttribute(REDIRECT)
        auditLogger.info("event:LOGIN " + user1.ndc + "header:" + reqHeader + " status:Success clientHost:" + clientHost + " clientIp:" + clientIp + " sessionId:" + session.getId + " port:" + req.getRemotePort + " requestedSessionId:" + req.getRequestedSessionId);

      } else {
        auditLogger.info("event:LOGIN user:" + user + " status:Failure clientHost:" + clientHost + " clientIp:" + clientIp);
        req.setAttribute("error", true)
        redirToLogin(req, resp, user, clientIp, clientHost)
      }
      return
    }
      super.doPost(req, resp)
  }

  def redirToLogin(req: HttpServletRequest, resp: HttpServletResponse, user: String, clientIp: String, clientHost: String): Unit = {
    try {
      val dispatcher: RequestDispatcher = req.getRequestDispatcher("login.jade")
      dispatcher.forward(req, resp)
    } catch {
      case _: Throwable => {
        val value = "event:LOGIN user:" + user + " status:Failure clientHost:" + clientHost + " clientIp:" + clientIp + " Ex:" + _
        auditLogger.error(value);
      }
    }
  }

  def storeUserSession(session:HttpSession, userId:String) = {
    var user = new com.logscape.portal.User(userId, "0000", "", 9999,  9999, 9999, true)
    session.setAttribute("session", new com.logscape.portal.Session(userId, user))
  }


  def authorise(user:String, pwd:String):Boolean = {
    info("Authenticate:" + user)
    adminSpace.authenticate(user, pwd)
  }

  def isAuthorised(user:String, pwd:String):Boolean = {
    return user != null && pwd != null && pwd.length > 2
  }
}
