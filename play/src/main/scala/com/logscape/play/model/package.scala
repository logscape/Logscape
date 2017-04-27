package com.logscape.play

import com.liquidlabs.log.space.agg.Meta
import java.util
import com.liquidlabs.log.fields.KeyValue

package object model {

  //  case class SearchResponse(data:String, file:String, filePath:String, host:String, ip:String, time:String, date:String, lineNumber:Int, fields:util.LinkedHashMap[String,String], summary:util.List[String], event:String="message")
  //  case class Events(items:List[SearchResponse], event:String="messages")
  case class InitEvents(requestId:String, summary:util.List[String],  fields:List[String], uuid:String, event:String="initEvents")

  case class Plot(queryIndex: Int, time:Long, index:Int, name:String, hits:Double, meta:Meta, event:String = "point")
  case class Progress(message:String, uuid:String, event:String = "progress")
  case class MultiPlot(points:List[Plot], uuid:String, event:String = "multiPoint")
  case class Chart(x0:Long, xStep:Int, steps:Int, uuid:String, event:String="chart")
  case class Facets(facets:List[Facet], uuid:String, event:String="facets")
  case class Facet(name:String, func:String, count:Int,children:List[KeyValue],dynamic:Boolean, event:String="facet")
  case class SearchNames(names:List[String],uuid:String, event:String="searchList")
  case class Search(name:String, terms:List[String],uuid:String, event:String="search")
  case class SearchName(name:String,uuid:String, event:String="searchName")
  case class SearchErrors(errors:List[String],uuid:String, event:String="searchErrors")
  case class WorkspaceNames(names:List[String], uuid:String, event:String="workspaceList")
  case class Workspace(name:String, content:String,uuid:String,event:String="workspace")
  case class WorkspaceSaved(name:String, uuid:String, event:String="workspaceSaved")
  case class WorkspaceDeleted(name:String, uuid:String, event:String="workspaceDeleted")


  case class LicenseItem(issued:String, products:String, email:String, company:String, dv:Int, expires:String, centralIndexers:Int, localIndexers:Int)
  case class LicenseList(list:List[LicenseItem], uuid:String, event:String="licenseList")

  case class HostList(list:List[String], uuid:String, event:String="hostList")
  case class DirItem(name:String,fileType:String, path:String)

  case class DirList(list:List[DirItem], uuid:String, event:String="dirList")


  case class DataSource(id:String, host:String, tag:String, dir:String, fileMask:String, timeFormat:String, ttl:Int, breakRule:String, rollEnabled:Boolean, discoveryEnabled:Boolean, grokItEnabled:Boolean, systemFieldsEnabled:Boolean, archivingRules:String, volumeGb:Double, uuid:String, event:String="dataSourceCreated")
  case class DataSourceList(sources:List[DataSource], uuid:String, event:String="dataSourceList")
  case class DataSourceListResults(sources:List[DataSource], uuid:String, event:String="searchDsListResults")
  case class DataSourceDeleted(id:String, event:String="dataSourceDeleted")
  case class DeployedFile(name:String, time:String, size:String, status:String, downloaded:Int)
  case class DeployedFilesList(msg:String,list:List[DeployedFile],uuid:String,event:String="deployedFilesList")

  case class TimeTesterResults(text:String, uuid:String, event:String="timeTesterResults")

  case class AgentSummary(agentId:String,startTime:String,os:String,ipAddress:String,cpuModel:String,cpu:Int,mem:Int,core:Int,mflops:Int,role:String,property:String)

  case class ServerGroup(name:String, resourceSelection:String, description:String)
  case class AgentList(list:List[AgentSummary],groups:List[ServerGroup],uuid:String, event:String="agentList")

  case class RuntimeInfo(upTime:String, startTime:String, buildId:String, username:String, role:String, permissions:Int, agents:Int, indexers:Int, totalVolume:String, uuid:String, event:String="runtimeInfo")

  case class AgentKVs(key1:String, value1:String, key2:String, value2:String)
  case class AgentKVList(list:List[AgentKVs],uuid:String, event:String="agentKVList")
  case class AgentLostList(list:List[String],uuid:String, event:String="lostAgentsList")

  case class UserModel(userId:String, role:String, pwd:String, email:String, apps:String, dataGroup:String, permissions:Int, logo:String, lastMod:String, uuid:String, event:String="userModel")
  case class UserSummary(userId:String,email:String, apps:String, dataGroups:String, permissions:Int, lastMod:String, role:String)
  case class UserList(list:List[UserSummary],uuid:String, event:String="userList")

  case class DGroup(name:String,include:String, exclude:String, children:String, enabled:Boolean, resourceGroup:String)
  case class DGroupList(list:List[DGroup],uuid:String, event:String="dGroupList")
  case class DGroupResult(message:String,uuid:String, event:String="evaluateDGroupResult")


  case class DirectoryServicesConfig(currentModel:String, baseCN:String,userCN:String,groupCN:String,providerURL:String,sysUser:String,sysCreds:String,userFilter:String,adminFilter:String, uuid:String, event:String="setSecurityConfig")
  case class DirectoryServicesConfigTest(result:String,uuid:String,event:String="testSecurityConfigOutput")
  case class ChangeSecurityModelOutput(result:String,uuid:String, event:String="changeSecurityModelOutput")

  case class FieldSetList(names:List[String], uuid:String, event:String="dataTypeList")
  case class Field(groupId:Int, name:String, funct:String,visible:Boolean, summary:Boolean, indexed:Boolean, srcField:String, synthExpr:String, desc:String)
  case class FieldSet(name:String, fields:List[Field], dir:String, file:String, priority:Int, sample:String, expression:String, uuid:String, event:String="gotDataType")
  case class FieldSetTestResults(json:String, uuid:String, event:String="testDataTypeResults")
  case class FieldSetBenchResults(json:String, uuid:String, event:String="benchDataTypeResults")
  case class ExpressionBenchResults(json:String, uuid:String, event:String="evaluateDataTypeExpressionResults")

  // used for search stuff
  case class CutDownFieldSet(name:String, fields:List[String])
  case class AllCutDownFieldSets(fieldSets:List[CutDownFieldSet], uuid:String, event:String="dataTypes")


  trait AlertAction {def name:String}
  case class EmailAction(from:String, to:String, subject:String, message:String, name:String="email") extends AlertAction
  case class ReportAction(value:String, name:String="report") extends AlertAction
  case class ScriptAction(value:String, name:String="script") extends AlertAction
  case class LogAction(value:String, name:String="log") extends AlertAction
  case class FileAction(value:String, name:String="file") extends AlertAction

  trait AlertTrigger {
    def name:String
    def value:String
  }

  case class NumericTrigger(value:String, name:String="numeric") extends AlertTrigger
  case class ExpressionTrigger(value:String, name:String="expression") extends AlertTrigger
  case class CorrelationTrigger(value:String, timeWindow:String, corrType:String, eventValue:String, correlationField:String, correlationKey:String, name:String="correlation") extends AlertTrigger

  case class Alert(name:String, schedule:String, trigger:AlertTrigger, search:String, lastFired:String, actions:List[AlertAction], dataGroup:String, enabled:Boolean, realTime:Boolean, lastRun:String, webSocketPort:String, feedScript:String)
  case class AlertingData(searchNames:List[String], alert:List[Alert], dataGroups:DGroupList, uuid:String, event:String="alertingData")
  case class AlertingEvents(events:List[String], uuid:String, event:String="alertingEvents")
  case class Ok(value:String, event:String="ok")

  case class EmailConfig(username:String,password:String, server:String, uuid:String, event:String="setEmailSetup")
  case class EmailConfigTest(message:String, uuid:String, event:String="testEmailSetupResults")
  case class Tail(url:String, file:String, uuid:String)

  case class UnAuthorised(permName:String,action:String, uuid:String, event:String="unauthorised")
  case class Error(action:String, uuid:String, event:String="error")

  //jmx
  case class UnknownBean(bean:String, event:String="unknown-bean")
  case class AttributeDescription(name:String, theType:String)
  case class Parameter(name:String, theType:String)
  case class AttributeValue(bean:String, attribute:String, value:Any)
  case class InvokeResult(bean:String, op:String, value:Any)
  case class OperationDescription(name:String, description:String, returnType:String, params:List[Parameter])
  case class BeanAttributes(beanName:String, attributes:List[AttributeDescription], event:String="jmx-attributes")
  case class BeanOperations(beanName:String, operations:List[OperationDescription], event:String="jmx-operations")
}
