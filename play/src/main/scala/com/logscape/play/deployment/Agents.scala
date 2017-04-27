package com.logscape.play.deployment

import com.liquidlabs.vso.resource.{ResourceGroup, ResourceSpace}
import org.fusesource.scalate.util.Logging
import java.util
import com.logscape.play.model.{ServerGroup, AgentLostList, AgentKVList, AgentKVs, AgentList, AgentSummary, DeployedFile}
import com.liquidlabs.vso.agent.{ResourceAgent, ResourceProfile}
import scala.collection.JavaConversions._
import scala._
import com.liquidlabs.transport.proxy.ProxyFactory
import com.liquidlabs.common.ExceptionUtil
import com.logscape.play.model

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 10/04/2013
 * Time: 12:41
 * To change this template use File | Settings | File Templates.
 */
class Agents (resourceSpace:ResourceSpace, proxyFactory:ProxyFactory) extends Logging {

  def  list(query:String, property:String, uuid:String): AgentList = {
    AgentList(loadResourceList(query, property), loadGroups, uuid)
  }

  private def loadGroups:List[ServerGroup] = {
    val map: scala.collection.immutable.List[model.ServerGroup] = resourceSpace.getResourceGroups.toList.map(r => ServerGroup(r.getName, r.getResourceSelection, r.getDescription))
    map
  }

  private def loadResourceList(query: String, property: String): List[AgentSummary] = {
    val resourceIds: util.List[String] = resourceSpace.findResourceIdsBy(query)
    var items: List[AgentSummary] = List[AgentSummary]()
    resourceIds.toList.foreach((id: String) => {
      val resource: ResourceProfile = resourceSpace.getResourceDetails(id)
      if (resource != null && items.length < 1000) {
        val value: String = resource.getFieldValue(property)
        val os: String = resource.getOsName + ":" + resource.getOsVersion
        items = items ::: List(AgentSummary(id, resource.getStartTime, os,
          resource.getIpAddress, resource.getCpuModel,
          resource.getCpuUtilisation, resource.getPhysicalMemTotalMB, resource.getCoreCount,
          resource.getMflops, resource.getType, value))
      }
    })
    items
  }

  def listKVs(resourceId:String, uuid:String): AgentKVList = {
    val resources:util.List[ResourceProfile] = resourceSpace.findResourceProfilesBy("resourceId equals " + resourceId);
    var kvs:List[AgentKVs] = List[AgentKVs]()
    if (resources.size == 1) {
      val resource:ResourceProfile = resources.get(0)
      var fields:util.List[String] = resource.getFields()
      var i = 0;
      while (i < fields.length) {
        val v1:String = resource.getFieldValue(fields.get(i))
        if (i+1 < fields.length) {
          val v2:String = resource.getFieldValue(fields.get(i +1))
          kvs = kvs:::List(AgentKVs(fields.get(i),v1, fields.get(i+1),v2))
        } else {
          kvs = kvs:::List(AgentKVs(fields.get(i),v1, "",""))
        }


        i = i + 2
      }
      //for(i <- 1 until fields.length) {
      //}
    }
    AgentKVList(kvs, uuid)
  }
  def bounceAgent(resourceId:String, uuid:String) {
    val profile:ResourceProfile = resourceSpace.getResourceDetails(resourceId)
    try {
      proxyFactory.getRemoteService(ResourceAgent.NAME, classOf[ResourceAgent], profile.getEndPoint()).bounce(true)
    } catch {
      case e: Exception => {
        warn(e.toString,e)
        warn(ExceptionUtil.stringFromStack(e,-1));

        // terminate request
      }
    }
    //return "resource:" + resourceId + " bounced";
  }
  def getLostAgents(uuid:String): AgentLostList = {
    val lostResources = new java.util.ArrayList[String](resourceSpace.getLostResources)
    return AgentLostList(lostResources.toList, uuid)
  }
  def clearLostAgents(uuid:String): String = {
    resourceSpace.clearLostAgents()
    return "Lost Agents cleared"
  }


}
