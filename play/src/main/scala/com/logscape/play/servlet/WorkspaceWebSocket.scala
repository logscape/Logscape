package com.logscape.play.servlet

import scala.collection.JavaConversions._
import com.logscape.play.model.{WorkspaceDeleted, WorkspaceSaved, Workspace, WorkspaceNames}
import com.liquidlabs.log.space.{SearchSet, LogSpace}
import com.logscape.play.Json._
import com.liquidlabs.admin._
import java.util
import org.apache.log4j.Logger


class WorkspaceWebSocket(clientId: String, user: User, logSpace: LogSpace) extends PlayWebSocket {
  def uid = user.username
  def auditLogger = Logger.getLogger("AuditLogger")

  def processMessage(event: String, payload: Map[String, Any], json:String, uuid:String): Option[String] = {
    def actions():Option[String] = {
      Some(event match {
        case "listWorkspaces" => listWorkspaces(uuid)
        case "openWorkspace" => openWorkspace(payload("workspaceName").toString, uuid)
        case "deleteWorkspace" => deleteWorkspace(payload("workspaceName").toString, uuid)
        case "saveWorkspace" => saveWorkspace(payload("workspaceName").toString, toJson(payload("content")),uuid)
      })
    }
    val requiredPerm = event match {
      case "deleteWorkspace" => Permission.Write
      case "saveWorkspace" => Permission.Write
      case _ => Permission.Read
    }
    executeWithPermission(requiredPerm, user.getPermissions, event, uuid, actions)
  }

  private def listWorkspaces(uuid:String) = {
    val results = logSpace.listWorkSpaces(user).toSet ++ logSpace.listDashboardNames(new util.HashSet[String]()).toSet
    toJson(WorkspaceNames(results.toList.sortWith(_.toLowerCase < _.toLowerCase), uuid))
  }

  private def saveWorkspace(name: String, content: String, uuid:String) = {
    auditLogger.info("event:SAVE_WORKSPACE " + user.ndc + " against:" + name)
    val aname = logSpace.saveWorkspace(name, content, user)
    toJson(WorkspaceSaved(aname, uuid))
  }

  private def deleteWorkspace(name: String, uuid:String) = {
    auditLogger.info("event:DELETE_WORKSPACE " +  user.ndc+ " against:" + name)
    info("Delete Workspace:" + name)
    logSpace.deleteWorkspace(name, user)
    toJson(WorkspaceDeleted(name, uuid))
  }

  private def openWorkspace(name: String, uuid:String) = {
    auditLogger.info("event:OPEN_WORKSPACE " +  user.ndc + " against:" + name)
    val goodname = name.replaceAll("%20", " ")
    var workspace = logSpace.getWorkspace(goodname, user)
    if (workspace != null) {
      toJson(new Workspace(workspace.name, workspace.content, uuid))
    } else {
        val db: SearchSet = logSpace.getDashboard(goodname)
        if (db != null) {
          val content = db.toJSON(logSpace)
          toJson(new Workspace(db.getName, content, uuid))
        } else {
          "{}"
        }
    }


  }
  def closeConnection() {
  }


}
