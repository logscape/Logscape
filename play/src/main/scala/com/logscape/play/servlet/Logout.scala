package com.logscape.play.servlet

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

class Logout extends HttpServlet {
  override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
    val session = req.getSession(false)
    session.removeAttribute("uid")
    session.invalidate()
    resp.sendRedirect("/play/login")
  }
}
