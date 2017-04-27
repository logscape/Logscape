package com.logscape.play.servlet

import java.io.{File, FileReader, BufferedReader}
import java.util.concurrent.ScheduledExecutorService

import com.liquidlabs.common.file.FileUtil
import com.liquidlabs.log.alert.correlate.CorrEventFeedAssembler
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import com.logscape.play.model._
import com.liquidlabs.log.space.LogSpace
import com.logscape.play.Json._
import com.liquidlabs.admin._
import com.codahale.jerkson.Json._
import com.logscape.play.deployment.{Users, Agents, Deployment}
import com.liquidlabs.log.fields.field.{FieldDTO, FieldI}
import com.liquidlabs.log.fields.{DelegatingFieldGenerator, FieldSetUtil}
import org.json.XML
import com.liquidlabs.common.regex.RegExpUtil
import java.util
import com.liquidlabs.common.{DateUtil, ExceptionUtil, StringUtil}
import com.liquidlabs.log.alert.Schedule
import scala.Some
import com.liquidlabs.vso.agent.ResourceProfile
import com.liquidlabs.log.AgentLogServiceAdmin
import com.liquidlabs.log.index.FileItem
import com.liquidlabs.transport.proxy.ProxyFactory
import com.liquidlabs.vso.resource.{ResourceGroup, ResourceSpace}
import com.liquidlabs.log.roll.{ContentBasedSorter, NullFileSorter}
import java.net.URI
import java.util.Date
import com.liquidlabs.common.file.raf.BreakRuleUtil
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeUtils
import com.liquidlabs.log.util.DateTimeExtractor


class AdminWebSocket(clientId: String, logSpace: LogSpace, user: User, deployment: Deployment, agents:Agents, users:Users, proxyFactory:ProxyFactory, resourceSpace:ResourceSpace, admin:AdminSpace) extends PlayWebSocket {
  private[logscape] var auditLogger: Logger = Logger.getLogger("AuditLogger")


  private val fromFile: File = new File("downloads/" + System.getProperty("ui.desktop", "background.jpg"))
  info("Running FROMMMM: " + new File(".").getCanonicalPath + " Exists:" + fromFile.exists() + " File:" + fromFile.getCanonicalPath)
  FileUtil.copyFile(fromFile, new File("work/jetty-0.0.0.0-8443-play.war-_play-any-/webapp/images/desktop/background.jpg"))


  def processMessage(event: String, payload: Map[String, Any], json:String, uuid:String): Option[String] = {
    def actions(): Option[String] = {
      Some(event match {

        case "searchDsList" => searchDsList(user.getDataGroup, uuid);

        case "createDataSource" => createDataSource(payload, uuid)
        case "listDataSources" => getDataSourceList(user.getDataGroup, uuid)
        case "deleteDataSource" => deleteDataSource(payload("id").toString, uuid)
        case "reindexDataSource" => reindexDataSource(user.getDataGroup, payload("id").toString, uuid)
        //case "getTailerBacklog" => getTailerBacklog()

        case "testTimeFormat" => testTimeFormat(payload("sample").toString, payload("format").toString, uuid)

        case "pushDeployedFiles" => pushDeployedFiles(uuid)
        case "listDeployedFiles" => getDeployedFilesList(uuid)
        case "removeDeployedFile" => removeDeployedFile(payload("name").toString, uuid)
        case "undeployDeployedFile" => undeployDeployedFile(payload("name").toString, uuid)
        case "deployDeployedFile" => deployDeployedFile(payload("name").toString, uuid)
        case "bounceSystem" => bounce
        case "getRuntimeInfo" => getRuntimeInfo(uuid)


        case "listAgents" => listAgents(payload("query").toString,payload("property").toString, uuid)

        case "listAgentKV" => listAgentKVs(payload("resourceId").toString, uuid)
        case "listLostAgents" => getLostAgents(uuid)
        case "clearLostAgents" => clearLostAgents(uuid)
        case "bounceAgent" => bounceAgent(payload("agentId").toString, uuid)

        case "listUsers" => listUsers(uuid)
        case "getUser" => getUser(payload("username").toString, uuid)
        case "saveUser" => saveUser(json, uuid)
        case "deleteUser" => deleteUser(payload("username").toString, uuid)

        case "listDGroups" => listDGroups(uuid)
        case "saveDGroup" => saveDGroup(payload, uuid)
        case "deleteDGroup" => deleteDGroup(payload("name").toString, uuid)
        case "evaluateDGroup" => evaluateDGroup(payload("name").toString, uuid)

        case "getSecurityConfig" => getSecurityConfig(uuid)
        case "saveSecurityConfig" => saveSecurityConfig(json)
        case "testSecurityConfig" => testSecurityConfig(json, uuid)
        case "syncSecurityUsers" => syncSecurityUsers(uuid)
        case "changeSecurityModel" => changeSecurityModel(payload("model").toString, uuid)

        case "listDataTypes" => listDataTypes(uuid)
        case "getDataType" => getDataType(payload("name").toString, uuid)
        case "saveDataType" => saveDataType(payload, uuid)
        case "autoGenDataType" => autoGenDataType(payload, uuid)
        case "deleteDataType" => deleteDataType(payload("name").toString, uuid)
        case "testDataType" => testDataType(payload, uuid)
        case "benchDataType" => benchDataType(payload, uuid)
        case "evaluateDataTypeExpression" => evaluateDataTypeExpression(payload("expression").toString, payload("sample").toString, uuid)
        case "getDataTypes" => getDataTypes(uuid)

        case "getAlertingData" => getAlertingConfig(uuid)
        case "getAlertingEvents" => getAlertingEvents("all", uuid)
        case "deleteAlert" => deleteAlert(payload("name").toString, uuid)
        case "saveAlert" => saveAlert(payload, json, uuid)

        case "listHost" => listHost(payload("host").toString, uuid)
        case "listDir" => listDir(payload("host").toString,payload("path").toString, uuid)

        case "getEmailSetup" => getEmailSetup(uuid)
        case "saveEmailSetup" => saveEmailSetup(payload("username").toString, payload("password").toString, payload("server").toString, uuid)
        case "testEmailSetup" => testEmailSetup(payload("username").toString, payload("password").toString, payload("server").toString, payload("from").toString, payload("to").toString, uuid)
        case "saveResourceGroup" => saveResourceGroup(payload("query").toString, payload("group").toString, payload("property").toString, uuid)
        case "deleteResourceGroup" => deleteResourceGroup(payload("group").toString, uuid)
      })
    }

    val requiredPerm = event match {
      case "getDataTypes" => Permission.Read
      case "searchDsList" => Permission.Read
      case "getRuntimeInfo" => Permission.Read
      case _ => Permission.Configure
    }
    executeWithPermission(requiredPerm, user.getPermissions, event, uuid, actions)
  }

  private def getAlertingEvents( datagroup:String, uuid:String) = {
    var results = new StringBuilder()
    val now = DateUtil.shortDateFormat.print(System.currentTimeMillis());
    var root = new File(".").getCanonicalPath
    if (root.endsWith("play")) root = "../master/build/logscape"
    else root = "."
    var f = new File(root + "/work/schedule/" + now + "-schedule-" + datagroup + ".log")
    if (!f.exists()) {
      println("CRAP:" + f.getAbsolutePath)
    }
    val reader: FileReader = new FileReader(f)

    var br = new BufferedReader(reader)
    val events = new scala.collection.mutable.ListBuffer[String]()
    var line = ""
    while (line != null) {
      if (line.length > 0) events += line
      line = br.readLine
    }
    br.close()

    toJson(AlertingEvents(events.toList, uuid))
  }


  private def saveAlert(payload: Map[String, Any], json:String, uuid:String) =  {
    val name = payload("name").toString
    val enabled = payload("enabled").asInstanceOf[Boolean]
    val realTime = payload("realTime").asInstanceOf[Boolean]
    val schedule = payload("schedule").toString
    val search = payload("search").toString
    val dataGroup = payload("dataGroup").toString
    val actions  = payload("actions").asInstanceOf[util.LinkedHashMap[String,util.LinkedHashMap[String,Any]]].toMap
    val triggers  = payload("trigger").asInstanceOf[util.LinkedHashMap[String, util.LinkedHashMap[String,Any]]].toMap


    auditLogger.info("User:" + user.username() + " Action:SaveAlert ID:" + name)

    val webSocketPort = payload("webSocketPort").toString
    val feedScript = payload("feedScript").toString


    def email(key:String) = actions("EmailAction").get(key).asInstanceOf[String]
    def value(action:String) = actions(action).get("value").asInstanceOf[String]

    def emptyString(value:String) = value == null || value.trim.isEmpty


    def trigger = {
      if(!emptyString(triggers("NumericTrigger").get("value").asInstanceOf[String])) triggers("NumericTrigger").get("value").asInstanceOf[String]
      else if(!emptyString(triggers("ExpressionTrigger").get("value").asInstanceOf[String])) triggers("ExpressionTrigger").get("value").asInstanceOf[String]
      else {
        val correlationTrigger = triggers("CorrelationTrigger").asInstanceOf[util.LinkedHashMap[String, String]].toMap
        if(emptyString(correlationTrigger("timeWindow"))) ""
        else {
          "corr: time:" + correlationTrigger("timeWindow") + " type:" + correlationTrigger("corrType") +
            " sequence:" + correlationTrigger("eventValue") + " field:" + correlationTrigger("correlationField") +
            " key:" + correlationTrigger("correlationKey")
        }
      }
    }




    // more shit to extract
    val alert = new Schedule(user.username, name, search, value("ReportAction"),
      value("ScriptAction"), value("FileAction"),
      value("LogAction"), "", trigger, email("from"), email("to"), email("subject"), email("message"),
      realTime, false, schedule, dataGroup, "", enabled,  webSocketPort, feedScript)
    try {
      logSpace.saveSchedule(alert)
      getAlertingConfig(uuid)
    } catch {
      case t : Throwable =>  toJson(Error("Check your Cron Syntax - Failed to Schedule:" + ExceptionUtil.stringFromStack(t,100), uuid))

    }


  }

  private def deleteAlert(name:String, uuid:String) = {
    auditLogger.info("User:" + user.username() + " Action:DeleteAlert ID:" + name)
    logSpace.deleteSchedule(name)
    getAlertingConfig(uuid)
  }

  private def getAlertingConfig(uuid:String) = {
    def scriptAction(schedule:Schedule, actions:List[AlertAction]) = {
      if(schedule.hasScriptAction) ScriptAction(schedule.scriptAction) :: actions
      else actions
    }
    def reportAction(schedule:Schedule, actions:List[AlertAction]) = {
      if(schedule.hasReportAction) ReportAction(schedule.generateReportName) :: actions
      else actions
    }

    def logAction(schedule:Schedule, actions:List[AlertAction]) = {
      if(schedule.hasLogAction) LogAction(schedule.logAction) :: actions
      else actions
    }

    def fileAction(schedule:Schedule, actions:List[AlertAction]) = {
      if(schedule.hasCopyAction) FileAction(schedule.copyAction) :: actions
      else actions
    }

    def emailAction(schedule:Schedule, actions:List[AlertAction]) = {
      if(schedule.hasEmailAction) EmailAction(schedule.emailFrom, schedule.emailTo, schedule.emailSubject, schedule.emailMessage) :: actions
      else actions
    }

    def actions(schedule:Schedule, actions:List[AlertAction]) = {
      scriptAction(schedule, emailAction(schedule, reportAction(schedule, logAction(schedule, fileAction(schedule, actions)))))
    }

    def trigger(schedule:Schedule) = {
      if(schedule.isNumericTrigger) NumericTrigger(schedule.trigger)
      else if(schedule.isExpressionTrigger) ExpressionTrigger(schedule.trigger)
      else {
        val map = CorrEventFeedAssembler.convertCorrelationTriggerToMap(schedule.trigger)
        CorrelationTrigger(schedule.trigger, map.get("time"), map.get("type"), map.get("sequence"), map.get("field"), map.get("key"))
      }
    }

    def toAlert(schedule:Schedule) = {
      Alert(schedule.name, schedule.cron, trigger(schedule), schedule.reportName, schedule.lastTrigger, actions(schedule, List[AlertAction]()), schedule.deptScope, schedule.isEnabled, schedule.isLiveAlert, schedule.lastRun ,schedule.getWebSocketPort, schedule.getBespokeFeedScript)
    }

    def permissionGroup = if(user.username.contains("admin")) "" else user.getPermissionGroup
    val schedules = logSpace.getSchedules(permissionGroup).toList.map(schedule => toAlert(schedule))
    toJson(AlertingData(logSpace.listSearches(user).toList,schedules ,users.listDGroups(uuid), uuid))
  }


  private def listHost(host:String, uuid:String):String = {
    var q = "id == 0 AND hostName contains " + host
    if (host.length == 0) q = "id == 0"
    val resourceIds:List[String] = resourceSpace.findResourceIdsBy(q).toList
    val ids:util.List[String] = new util.ArrayList[String]
    resourceIds.map( x => ids.add(resourceSpace.getResourceDetails(x).getHostName))
    toJson(HostList(ids.toList, uuid))
  }

  private def listDir(host:String, currentDir:String, uuid:String):String = {
    //		if (hostFilter != null && hostFilter.length() > 0) hostFilter = "hostName contains " + hostFilter;
    var query:String = "hostName equals " + host + " AND id == 0";
    if (host == null || host.length() == 0) query = "type contains Management AND id == 0";

    val resourceIds:List[String] = resourceSpace.findResourceIdsBy(query).toList

    if (resourceIds.length == 0) {
      return toJson(DirList(Nil, uuid))
    }

    val resource:List[ResourceProfile] = resourceSpace.findResourceProfilesBy("resourceId equals " + resourceIds.get(0)).toList

    val resourceProfile:ResourceProfile = resource.get(0);
    val agentLogServiceAdmin:AgentLogServiceAdmin = proxyFactory.getRemoteService(AgentLogServiceAdmin.NAME,   classOf[AgentLogServiceAdmin], resourceProfile.getEndPoint());

    val listFiles:List[FileItem]  = agentLogServiceAdmin.listDirContents(currentDir, user.fileExcludes, ".*", false).toList
    info("GetLogDirListing:" + host + "/" + currentDir + " : " + " GotItems:" + listFiles.size);

    val fileList = listFiles.map(x =>  DirItem(x.label, x.getType(), x.path))
    toJson(DirList(fileList.sortBy(_.name), uuid))

  }

  private def searchDsList(dataGroup:String, uuid:String) = {
    val watchDirs = logSpace.watchDirectories(user, "", false).toList

    val dataGroupImpl = admin.getDataGroup(dataGroup, true);
    val filteredSources = watchDirs.filter(x => {
      x.isDataGroupMatch(dataGroupImpl)
    })

    val sources = filteredSources.map(x => {
      var rollEnabled = false;
      if (x.getFileSorter().getClass() == classOf[ContentBasedSorter]) rollEnabled = true;
      DataSource(x.id, x.getHosts, x.getTags, x.getDirName, x.filePattern, x.getTimeFormat, x.getMaxAge, x.getBreakRule, rollEnabled, x.isDiscoveryEnabled, x.isGrokDiscoveryEnabled, x.isSystemFieldsEnabled, x.archivingRules(), -1.0, uuid)
    })
    info("Sending sources: " + sources)
    toJson(DataSourceListResults(sources, uuid))
  }


  private def createDataSource(payload: Map[String, Any], uuid:String) = {
    val tag = payload("tag").toString.trim
    val dir = payload("dir").toString.trim
    val host = payload("host").toString
    val timeFormat = payload("timeFormat").toString
    val fileMask = payload("fileMask").toString.trim
    val ttl = Integer.parseInt(payload("ttl").toString)

    var rollConvention = classOf[NullFileSorter].getName
    val rollEnabledBool = payload("rollEnabled").toString == "true"
    if (rollEnabledBool){
      rollConvention = classOf[ContentBasedSorter].getName
    }

    val discoveryEnabled = payload("discoveryEnabled").toString == "true"
    val grokItEnabled = payload("grokItEnabled").toString == "true"
    val systemFieldsEnabled = payload("systemFieldsEnabled").toString == "true"

    var breakRule = payload("breakRule").toString
    if (!breakRule.startsWith("Explicit:")) breakRule = breakRule.trim
    val archivingRules = payload("archivingRules").toString



    val id = logSpace.addWatch(tag, dir, fileMask, timeFormat, rollConvention, host, ttl, archivingRules, discoveryEnabled, breakRule, payload("id").toString, grokItEnabled, systemFieldsEnabled)

    info("User:" + user.username() + " Action:AddedDataSource DataSourceId:" + id)

    toJson(DataSource(id, host, tag, dir, fileMask, timeFormat, ttl, breakRule, rollEnabledBool, discoveryEnabled, grokItEnabled, systemFieldsEnabled, archivingRules, -1.0, uuid))
  }

  private def getDataSourceList(dataGroup:String, uuid:String) = {

    val watchDirs = logSpace.watchDirectories(user, "", false).toList
    val dataGroupImpl = admin.getDataGroup(dataGroup, true)

    val filteredSources = watchDirs.filter(x => {
      x.isDataGroupMatch(dataGroupImpl)
    })
    val sources = filteredSources.map(x => {
      var rollEnabled = false;
      if (x.getFileSorter().getClass() == classOf[ContentBasedSorter]) rollEnabled = true;
      DataSource(x.id, x.getHosts, x.getTags, x.getDirName, x.filePattern, x.getTimeFormat, x.getMaxAge, x.getBreakRule, rollEnabled, x.isDiscoveryEnabled, x.isGrokDiscoveryEnabled, x.isSystemFieldsEnabled, x.archivingRules(), -1.0, uuid)
    })
    info("Sending sources: " + sources)


    val sched:ScheduledExecutorService = proxyFactory.getScheduler

    proxyFactory.getScheduler.schedule(
      new Runnable() {
        override def run(): Unit = {
          var vvv:java.util.Map[String, java.lang.Double] = logSpace.getWatchVolumes("");

          val sources2 = filteredSources.map(x => {
              var rollEnabled = false;
              if (x.getFileSorter().getClass() == classOf[ContentBasedSorter]) rollEnabled = true;
              var volume = vvv.get(x.getTags)
              if (volume == null) volume = -1.0
              DataSource(x.id, x.getHosts, x.getTags, x.getDirName, x.filePattern, x.getTimeFormat, x.getMaxAge, x.getBreakRule, rollEnabled, x.isDiscoveryEnabled, x.isGrokDiscoveryEnabled, x.isSystemFieldsEnabled, x.archivingRules(), volume, uuid)
            })

              connection.sendMessage(toJson(DataSourceList(sources2, uuid)))

        }
      }
    , 10, java.util.concurrent.TimeUnit.MILLISECONDS)



    toJson(DataSourceList(sources, uuid))

  }

  private def pushDeployedFiles(uuid:String) = toJson(DeployedFilesList("", deployment.push(), uuid))
  private def getDeployedFilesList(uuid:String) = {
    val list = deployment.list()
    var status = ""
    if (list.size > 0) {
      var waitingCount = 0;
      val count = list.get(0).downloaded;
      list.toList.foreach(f => {
        if (f.downloaded != count) {
          waitingCount = waitingCount + 1
        };
      })
      if (waitingCount > 0) status = "Waiting for " + waitingCount + " download(s) to complete"
    }
    toJson(DeployedFilesList(status, list, uuid))
  }
  private def removeDeployedFile(name: String, uuid:String) = toJson(deployment.remove(name, uuid))
  private def undeployDeployedFile(name: String, uuid:String) = toJson(deployment.undeploy(name, uuid))
  private def deployDeployedFile(name: String, uuid:String) = toJson(deployment.deploy(name, uuid))

  private def bounce = toJson(deployment.bounce())
  private def getRuntimeInfo(uuid:String) = toJson(deployment.getRuntimeInfo(user, uuid))


  private def listAgents(query:String, property:String, uuid:String) = toJson(agents.list(query,property, uuid))
  private def listAgentKVs(resourceId:String, uuid:String) = toJson(agents.listKVs(resourceId, uuid))
  private def getLostAgents(uuid:String) = toJson(agents.getLostAgents(uuid))
  private def clearLostAgents(uuid:String) = toJson(agents.clearLostAgents(uuid))
  private def bounceAgent(agentId:String, uuid:String) = toJson(agents.bounceAgent(agentId, uuid))

  private def saveResourceGroup(query:String, group:String, property:String, uuid:String) = {
    if(query.trim().isEmpty || group.trim().isEmpty) {
      "{ msg: \"query or group is empty\" }";
    }else {
      resourceSpace.registerResourceGroup(new ResourceGroup(group, query, group, DateTimeFormat.longDateTime().print(DateTimeUtils.currentTimeMillis())))
      listAgents(query,property, uuid)
    }
  }

  private def deleteResourceGroup(group:String, uuid:String) = {
    resourceSpace.unRegisterResourceGroup(group)
    listAgents("","",uuid)
  }



  private def listUsers(uuid:String) = toJson(users.list(uuid))

  private def getUser(username:String, uuid:String) = toJson(users.getUser(username, uuid))
  private def saveUser(json:String, uuid:String) = {
    val ss = getNode("data\":",json)
    val userModel: UserModel = parse[UserModel](ss)
    auditLogger.info("User:" + user.username() + " Action:SaveUser ID:" + userModel.userId)
    toJson(users.saveUser(userModel, uuid))
  }

  private def deleteUser(username:String, uuid:String) = {
    auditLogger.info("User:" + user.username() + " Action:DeleteUser ID:" + username)
    toJson(users.deleteUser(username, uuid))
  }


  private def listDGroups(uuid:String) = toJson(users.listDGroups(uuid))
  private def evaluateDGroup(name:String, uuid:String) = toJson(users.evaluateDGroup(name, uuid))

  private def saveDGroup(payload: Map[String, Any], uuid:String) = {
    auditLogger.info("User:" + user.username() + " Action:SaveDGroup ID:" + payload("name").toString)
    toJson(users.saveDGroup(payload("name").toString,payload("includes").toString,payload("excludes").toString,payload("children").toString,java.lang.Boolean.parseBoolean(payload("enabled").toString), payload("resourceGroup").toString, uuid))
  }

  private def deleteDGroup(name:String, uuid:String) = {
    auditLogger.info("User:" + user.username() + " Action:DeleteDGroup ID:" + name)
    toJson(users.deleteDGroup(name, uuid))
  }
  
  private def getSecurityConfig(uuid:String) = toJson(users.getSecurityConfig(uuid))
  private def saveSecurityConfig(json:String) = {
    val ss = getNode("data\":",json)
    toJson(users.saveSecurityConfig(parse[DirectoryServicesConfig](ss)))
  }

  private def testSecurityConfig( json:String, uuid:String) = {
    val ss = getNode("data\":",json)
    try {
      toJson(users.testSecurityConfig(parse[DirectoryServicesConfig](ss), uuid))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        warn(e.toString,e)
        warn(ExceptionUtil.stringFromStack(e,-1));
        toJson(DirectoryServicesConfigTest(new Date() + " " + ExceptionUtil.stringFromStack(e,-1), uuid))

        // terminate request
      }
    }
    //makeDirList(DirectoryServicesConfigTest("Failed", uuid))

  }
  private def syncSecurityUsers(uuid:String):String = {
    return toJson(users.syncUsers(uuid))
  }

  private def getNode(from:String, json:String):String = {
    return json.substring(json.indexOf(from) + from.length, json.length-1)
  }
  private def changeSecurityModel(newModel:String, uuid:String) = toJson(users.changeSecurityModel(newModel, uuid))

  private def deleteDataSource(id: String, uuid:String) = {
    info("User:" + user.username() + " removedDatasourced:" + id)
    logSpace.removeWatch(id)
    toJson(DataSourceDeleted(id, uuid))
  }
  private def reindexDataSource(dataGroup:String, id: String, uuid:String) = {
    logSpace.reindexWatch(id)
    getDataSourceList(dataGroup, uuid:String)
  }
  private def getTailerBacklog() = {

  }
  private def testTimeFormat(sample: String, format:String, uuid:String) = {
    toJson(TimeTesterResults(DateTimeExtractor.testWithFormat(format, sample), uuid));
  }

  private def listDataTypes(uuid:String) = {
    val list:util.List[String] = logSpace.fieldSetList()
    toJson(FieldSetList(list.toList, uuid))

  }
  private def getDataType(name:String, uuid:String) = {
    val fieldSet:com.liquidlabs.log.fields.FieldSet = logSpace.getFieldSet(name)
    if (fieldSet == null) {
      " { msg: \"not found\" }"
    } else {
      val fields:List[FieldDTO] = fieldSet.toDTO.toList
      val jFields = fields.map( (x:FieldDTO) => Field(x.groupId, x.name, x.funct, x.isVisible, x.isSummary, x.isIndexed, x.synthSrcField, x.synthExpression, x.description))
      toJson(FieldSet(fieldSet.getId,jFields,fieldSet.filePathMask, fieldSet.fileNameMask, fieldSet.priority,fieldSet.exampleText,fieldSet.expression, uuid))
    }
  }

  private def getDataTypes(uuid:String) = {
    val cutDownFieldSets: List[CutDownFieldSet] = logSpace.fieldSets().toList.map((fs: com.liquidlabs.log.fields.FieldSet) => CutDownFieldSet(fs.getId, fs.fields.toList.map((f: FieldI) => f.name)))
    toJson(AllCutDownFieldSets(cutDownFieldSets, uuid))
  }

  private def autoGenDataType(payload: Map[String, Any], uuid:String) = {
    // convert back to FieldSet object
    val fs:com.liquidlabs.log.fields.FieldSet = getFieldSet(payload)
    val ex:Array[String] = fs.example
    val fieldSet:com.liquidlabs.log.fields.FieldSet =  new DelegatingFieldGenerator(fs.id, fs.priority, fs.filePathMask, fs.fileNameMask).guess2(ex)

    val fields:List[FieldDTO] = fieldSet.toDTO.toList
    val jFields = fields.map( (x:FieldDTO) => Field(x.groupId, x.name, x.funct, x.isVisible, x.isSummary, x.isIndexed, x.synthSrcField, x.synthExpression, x.description))
    toJson(FieldSet(fieldSet.getId,jFields,fieldSet.filePathMask, fieldSet.fileNameMask, fieldSet.priority,fieldSet.exampleText,fieldSet.expression, uuid))
  }


  private def testDataType(payload: Map[String, Any], uuid:String) = {
    // convert back to FieldSet object
    val fs:com.liquidlabs.log.fields.FieldSet = getFieldSet(payload)
    fs.addDefaultFields(fs.id,"host","filename.log","/path","tag","agent","sourceURL",100,true);

    val s:String = FieldSetUtil.toXML(fs, fs.example);
    toJson(FieldSetTestResults(XML.toJSONObject(s).toString, uuid))
  }
  private def saveDataType(payload: Map[String, Any], uuid:String) = {
    // convert back to FieldSet object
    val fs:com.liquidlabs.log.fields.FieldSet = getFieldSet(payload)
    logSpace.saveFieldSet(fs);
    "{ msg: \"done\", uuid:\"" + uuid + "\" }"
  }

  private def deleteDataType(name:String, uuid:String) = {
    logSpace.removeFieldSet(name)
    "{ msg: \"done\" uuid:\"" + uuid + "\" }"
  }


  private def benchDataType(payload: Map[String, Any], uuid:String) = {
    val fs:com.liquidlabs.log.fields.FieldSet = getFieldSet(payload)

    val s:String = FieldSetUtil.testPerformance(fs,BreakRuleUtil.getLinesFromTextBlock(payload.get("sample").toString))
    info(s)
    toJson(FieldSetBenchResults(s, uuid))
  }

  private def evaluateDataTypeExpression(expression:String, text:String, uuid:String) = {
    val lines:Array[String] =  BreakRuleUtil.getLinesFromTextBlock(text)
    var testing:String = lines(0);
    if (testing.startsWith("#")) testing = lines(1)


    val results = RegExpUtil.testJRegExp(expression, testing).replaceAll("\n", "<br>");
    if (results.contains("Expression Performance")) {
      toJson(ExpressionBenchResults(results.substring(results.indexOf("Expression Performance")), uuid))
    } else {
      toJson(ExpressionBenchResults(results, uuid))
    }

  }


  private def getFieldSet(payload: Map[String, Any]) = {
    val sf:Some[Any] = payload.get("fields").asInstanceOf[Some[Any]]
    val fields:java.util.ArrayList[java.util.LinkedHashMap[String,String]] = sf.get.asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]]


    val fieldSet:com.liquidlabs.log.fields.FieldSet = new com.liquidlabs.log.fields.FieldSet()
    fieldSet.id = payload.get("name").asInstanceOf[Some[Any]].get.toString.trim
    fieldSet.expression = payload.get("expression").asInstanceOf[Some[Any]].get.toString
    fieldSet.filePathMask =payload.get("dir").asInstanceOf[Some[Any]].get.toString.trim
    fieldSet.fileNameMask =payload.get("file").asInstanceOf[Some[Any]].get.toString.trim
    fieldSet.priority =  StringUtil.isInteger(   payload.get("priority").asInstanceOf[Some[Any]].get.toString).intValue()
    fieldSet.example = BreakRuleUtil.getLinesFromTextBlock(payload.get("sample").asInstanceOf[Some[Any]].get.toString)



    fields.map( (f:java.util.LinkedHashMap[String,String]) => {
      fieldSet.addField(f.get("name").trim, f.get("funct"), f.get("visible").asInstanceOf[Boolean], f.get("groupId").asInstanceOf[Int], f.get("summary").asInstanceOf[Boolean], f.get("srcField"), f.get("synthExpr"), f.get("desc"), f.get("indexed").asInstanceOf[Boolean])
    })
    fieldSet

  }
  def closeConnection() {
  }



  private def getEmailSetup(uuid:String) = {
    val email:com.liquidlabs.admin.EmailConfig = admin.getEmailConfig
    toJson(new com.logscape.play.model.EmailConfig(email.username, email.password, email.protocol + "://"+ email.host + ":" + email.port, uuid))
  }
  private def saveEmailSetup(user: String, password:  String, server: String, uuid:String) = {
    val url = new URI(server)
    admin.setEmailConfig(new com.liquidlabs.admin.EmailConfig(url.getScheme, url.getHost, url.getPort, user, password))
    ""
  }

  private def testEmailSetup(user: String, password:  String, server: String, from:String, to:String, uuid:String) = {
    val url = new URI(server)
    admin.setEmailConfig(new com.liquidlabs.admin.EmailConfig(url.getScheme, url.getHost, url.getPort, user, password))
    val msg = admin.sendEmail(from, util.Arrays.asList(to), "Logscape:test email sent at:" + new Date(), "test email contents");
    info("Email Test Msg:" + msg)
    toJson(new EmailConfigTest(msg, uuid))
  }



}
