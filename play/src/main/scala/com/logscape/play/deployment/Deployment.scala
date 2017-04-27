package com.logscape.play.deployment

import java.io.{FilenameFilter, File}
import java.util
import com.liquidlabs.services.AppLoader

import scala._
import scala.collection.JavaConversions._
import com.logscape.play.model.{RuntimeInfo, DeployedFile, DeployedFilesList}
import com.liquidlabs.vso.agent.ResourceProfile
import com.liquidlabs.common.file.FileUtil
import java.util.concurrent.{TimeUnit, ConcurrentHashMap}
import com.liquidlabs.common.HashGenerator
import com.liquidlabs.vso.resource.ResourceSpace
import com.liquidlabs.admin._
import com.liquidlabs.vso.VSOProperties
import com.liquidlabs.replicator.data.{Upload, MetaInfo}
import com.liquidlabs.replicator.ReplicatorProperties
import com.liquidlabs.replicator.service.{Uploader, ReplicationService}
import com.liquidlabs.vso.deployment.DeploymentService
import com.liquidlabs.log.space.{LogStats, LogSpace}
import org.joda.time.{Period, Interval, DateTime}
import java.text.DecimalFormat
import org.apache.log4j.Logger
import com.liquidlabs.common.concurrent.{ExecutorService, NamingThreadFactory}
import com.logscape.play.servlet.Services
import org.apache.commons.lang3.exception.ExceptionUtils

//import java.lang.String
import scala.Predef.String
import org.fusesource.scalate.util.Logging
import com.liquidlabs.vso.deployment.bundle.{Bundle, BundleUnpacker, BundleSpace}
import com.liquidlabs.dashboard.server.vscape.dto.FileItem

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 05/04/2013
 * Time: 14:49
 * To change this template use File | Settings | File Templates.
 */

class Deployment(user: User) extends Logging with Services {
  def auditLogger = Logger.getLogger("AuditLogger")
  def vsauditLogger = Logger.getLogger("VAuditLogger")

  val appLoader :AppLoader = new AppLoader(scheduler)


  //   IDE Test Mode
  //   val rootDir = "/Volumes/Media/LOGSCAPE/TRUNK/LogScape/master/build.run/logscape/"
  //   VSOProperties.setRootDir(rootDir)

  val deployDir:File = new File(VSOProperties.getDownloadsDir())
  val bundleDir:String  = VSOProperties.getDeployedBundleDir()
  val unpacker :BundleUnpacker  = new BundleUnpacker(new File(VSOProperties.getSystemBundleDir()), new File(VSOProperties.getDeployedBundleDir()))

  info("DEPLOY: " + new File(VSOProperties.getDownloadsDir()).getAbsolutePath)

  def getRuntimeInfo(user:User, uuid:String): RuntimeInfo =  {
    val agents = resourceSpace.findResourceIdsBy("id = 0").size()
    val indexers = resourceSpace.findResourceIdsBy("workId contains Indexer").size()
    val manager = resourceSpace.findResourceProfilesBy("type contains Management AND id = 0")
    val stats = getLogStats
    stats.indexedTotal
    val df = new DecimalFormat("0.00");

    RuntimeInfo(getUptime, manager.get(0).getStartTime, getBuildId, user.username(), user.getRole.name(), user.getPermissions.getPerm, agents, indexers, df.format(stats.indexedToday.toDouble/FileUtil.GB.toDouble) + " Today, " + df.format(stats.indexedTotal.toDouble/FileUtil.GB.toDouble) + " Total", uuid)
  }
  def getBuildId(): String = {

    val items =  resourceSpace.findResourceIdsBy("type containsAny Manag,Failover AND id = 0")
    if (items.size() > 0) {
      val mgmtId = items.get(0)
      val  resourceDetails:ResourceProfile = resourceSpace.getResourceDetails(mgmtId)
      return resourceDetails.getResourceId + "/" + resourceDetails.buildInfo
    } else {
      return "Unknown"
    }
  }
  def getUptime():String = {
    val  interval : Interval = new Interval(startTime, new DateTime())
    val period : Period  = interval.toPeriod
    period.getDays() + "d " + period.getHours + "h " + period.getMinutes + "m"
  }

  @transient
  private var lastLogStats: LogStats = null

  def getLogStats: LogStats = {
    try {
      val result: LogStats = new LogStats
      val logStats: List[LogStats] = logSpace.getLogStats.toList

      for (logStats2 <- logStats) {
        result.add(logStats2)
      }
      lastLogStats = result
      return result
    }
    catch {
      case t: Throwable => {
        error("Failed to getLogStats from:" + logSpace, t)
      }
    }
    return lastLogStats
  }



  def bounce() {
    info("event:BOUNCE_SYSTEM");
    auditLogger.info("event:BOUNCE_SYSTEM " + user.ndc)
    bundleSpace.bounceSystem()

    val r = new Runnable() {
      def run() {
        info("Process Exiting")
        System.exit(10);
      }
    }
    scheduler.schedule(r, Integer.getInteger("db.shutdown.delay",5).intValue(), TimeUnit.SECONDS);
    Thread.sleep(2000);
  }


  def deploy(name:String, uuid:String): DeployedFilesList =  {
    info("Deploying:" + name);
    auditLogger.info("event:DEPLOY " + user.ndc)

    if (name.endsWith(".config")) {
      auditLogger.info("event:ConfigOverWrite file:" + name + user.ndc)
      val contents = FileUtil.readAsString(VSOProperties.getDownloadsDir() + "/" + name);
      logSpace.importConfig(contents,false, true);

      return DeployedFilesList("Deployed:" + name,list(), uuid)
    }
    val bundleName:String = name.replaceAll(".zip", "");
    val deployedFlag:String = getDeployedFlag(bundleName);
    val sysStatus:String = bundleSpace.getBundleStatus( bundleName);
    if (deployedFlag.equals("DEPLOYED") && !sysStatus.equals("UNINSTALLED")) {
      return DeployedFilesList( "It is already DEPLOYED", list(), uuid)
    }

    val theFile:File = new File(deployDir, name);
    auditLogger.info("event:DEPLOY " + user.ndc + " file:" + name + " ")

    val bundle:Bundle = unpacker.getBundle(theFile);
    if (isNestedDirectory(theFile, name)) {
      return DeployedFilesList( "Found a subdirectory - '" + name + "' which should not exist - should it be in the root path of the zip?", list(),uuid)
    }
    if (bundle != null) {

      unpacker.unpack(theFile, false);
      val meta:MetaInfo  = new MetaInfo(theFile, ReplicatorProperties.getChunkSizeKB());

      // will write the deployed file
      deploymentService.deploy(bundle.getId(), meta.hash(), bundle.isSystem());
      Thread.sleep(1000);
      //super.deploy(bundle.getId());


      appLoader.loadConfigXMLIntoLogScape(bundle.getId, logSpace);
    } else {
      info("Cannot find App.bundle file:" + theFile.getAbsolutePath());
      //e += " invalid bundle, cannot find App.bundle file:" + name;
    }

    // wait for an error
    Thread.sleep(2 * 1000);
    return DeployedFilesList("Deployed:" + name,list(), uuid)
  }
  def undeploy(name:String, uuid:String): DeployedFilesList =  {
    try {

      info("UNDEPLOY:" + name);
      auditLogger.info("event:UNDEPLOY " + user.ndc + " file:" + name + " ")
      vsauditLogger.info("event:UNDEPLOY " + user.ndc + " file:" + name + " ")

      val file:File = new File(deployDir, name);
      if (unpacker.isBundle(file)) {
        val meta:MetaInfo  = new MetaInfo(file, ReplicatorProperties.getChunkSizeKB())
        val bundle:Bundle= unpacker.getBundle(file);
        if (bundle == null) {
          error("Cannot undeploy file as it is not a bundle:" + file)
          return DeployedFilesList("Cannot undeploy file as it is not a bundle:" + file, list(), uuid)
        }
        info("UNDEPLOY:" + file.getAbsolutePath() + " ID:" + bundle.getId());
        appLoader.unloadApp(bundle.getId(), logSpace);
        deploymentService.undeploy(bundle.getId(), meta.hash());
      }

    } catch {
      case npe: NullPointerException => error("NullPtr", npe)
      case unknown : Throwable => error("Undeploy - Exception happened:" + ExceptionUtils.getStackTrace(unknown), unknown)
    }
    return DeployedFilesList(name + " was Undeployed", list(), uuid)
  }

  def  remove(name:String, uuid:String): DeployedFilesList = {

    auditLogger.info("event:REMOVE " + user.ndc + " file:" + name + " ")
    vsauditLogger.info("event:UNDEPLOY " + user.ndc + " file:" + name + " ")

    undeploy(name, uuid);

    try {
      val toRemove:File = new File(deployDir, name);
      val meta:MetaInfo = new MetaInfo(toRemove, ReplicatorProperties.getChunkSizeKB())
      if (unpacker.isBundle(toRemove)) {
        val bundleName:String  = unpacker.getBundleName(toRemove.getName())
        try {
          deploymentService.undeploy(bundleName, meta.hash())
        } catch {
          case unknown : Throwable => error("Undeploy - Exception happened: " + ExceptionUtils.getStackTrace(unknown), unknown)
        }
        FileUtil.deleteDir(new File(bundleDir, bundleName))
      }
      replicationService.remove(String.format("Uninstall Bundle[%s] called from GUI - forceRemove=true", toRemove), new Upload(meta.hash(), meta.name(), meta.path(), meta.pieceCount()));
      toRemove.delete();
    } catch {
      case npe: NullPointerException => error("NullPtr", npe)
      case unknown : Throwable => error("Remove - Exception happened: " + ExceptionUtils.getStackTrace(unknown), unknown)
    }

    return DeployedFilesList("Removed:" + name, list(), uuid)
  }

  def  push(): List[DeployedFile] = {
    info("PUSH Replicated files")
    val fileArray : Array[File] = deployDir.listFiles()
    if (fileArray == null) {
      return list()
    }
    val files : List[File] =  List.fromArray(fileArray)
    files.foreach( (file : File) => {
      if (!file.getName.startsWith(".") && !file.isDirectory) {
        try {
          info("PUSH Replicated:" +file.getPath())
          uploader.deployFile(file.getPath(), true)
        } catch {
          case e: Exception => {
            warn("Push Error:", e);
          }
        }

      }
    })
    list

  }
  def  list(): List[DeployedFile] = {

    val fileArray : Array[File] = deployDir.listFiles();


    if (fileArray == null) return List[DeployedFile]()

    val files : List[File] =  List.fromArray(fileArray)

    val results:util.List[FileItem] = new util.ArrayList[FileItem]()
    val resourceIds : util.List[String] = resourceSpace.findResourceIdsBy("instanceId equals 0")
    val ids : util.List[String] = resourceSpace.findResourceIdsBy("instanceId equals 0")

    files.foreach( (file:File) => {
      if (!file.getName().startsWith(".") && !file.getName().endsWith(".bak") ){
        var sysStatus = bundleSpace.getBundleStatus( file.getName().replaceAll(".zip", ""))
        if (file.getName.equals("boot.zip")) sysStatus = "ACTIVE"
        val bundleXML = bundleSpace.getBundleXML( file.getName().replaceAll(".zip", ""))

        val deployedFlag = getDeployedFlag(file.getName().replaceAll(".zip", ""))
        val view = getViewContent(file)
        results.add(new FileItem(file, sysStatus, deployedFlag, bundleXML, 0, resourceIds.size(), view))
      }
    })


    resourceIds.toList.foreach( (resourceId:String) => {
      val profile :ResourceProfile = resourceSpace.getResourceDetails(resourceId);
      results.foreach( (fileItem:FileItem) => {
        if (isDownloadedCount(fileItem.file, profile)) {
          fileItem.incrementDownloaed()
        }
      })
    })

    return results.toList.map( (x: FileItem)  => DeployedFile(x.name,x.time,x.size,x.status,x.downloaded) )
  }
  def  getViewContent(file : File) : String = {
    var result = "unknown";
    if (!file.exists()) return "Deleted:" + file.getAbsolutePath();
    if (file.getName().endsWith(".zip")) result = "ZipFile";
    if (file.getName().endsWith(".tar")) result = "TarBall";
    if (file.getName().endsWith(".tgz")) result = "TGZFile";
    if (file.getName().endsWith(".properties")) result = FileUtil.readAsString(file.getAbsolutePath());
    if (file.getName().endsWith(".txt")) result = FileUtil.readAsString(file.getAbsolutePath());
    if (file.getName().endsWith(".log")) result = FileUtil.readAsString(file.getAbsolutePath());
    if (result.length() > 1024) result = result.substring(0, 1023);
    return result;
  }

  /**
   * cache hashed values
   * TODO: convert this into a ProxyFactory.localObject(object) - which can provide the hash-cache functionality using all params
   */
  val hashCache:util.Map[String, String]  = new ConcurrentHashMap[String, String]();
  def  isDownloadedCount(file:File, profile:ResourceProfile) : Boolean = {
    try {
      val cacheKey = file.getAbsolutePath()+file.length() + file.lastModified();
      var fileHash = ""
      synchronized {
        fileHash = hashCache.get(cacheKey)
        if (fileHash == null) {
          fileHash = new HashGenerator().createHash(file.getName(), file)
          hashCache.put(cacheKey, fileHash);
        }
      }

      if (profile.isFileDownloaded(file.getName(), file.lastModified(), file.length()/1024, fileHash)) return true;

    } catch {
      case npe: NullPointerException => error("NullPtr", npe)
      case unknown : Throwable => error("IsDownloaded - Exception happened: " + ExceptionUtils.getStackTrace(unknown), unknown)
    }
    return false;
  }
  def  getDeployedFlag(bundle:String) :String = {
    if (new File("deployed-bundles/" + bundle + "/" + "DEPLOYED").exists()) return "DEPLOYED";
    if (new File("system-bundles/" + bundle + "/" + "DEPLOYED").exists()) return "DEPLOYED";
    return "STAGED";
  }

  def isNestedDirectory(theFile:File , filename:String):Boolean = {
    val foundFiles:Array[String] = theFile.list(new FilenameFilter() {
      def  accept(dir:File , name:String) :Boolean = {
        return name.equals(filename.substring(0, filename.indexOf(".zip")));
      }
    })
    return foundFiles != null && foundFiles.length == 1;
  }

}
