package com.logscape.play.deployment

import org.fusesource.scalate.util.Logging
import java.util
import com.logscape.play.model.{DGroupResult, ChangeSecurityModelOutput, DirectoryServicesConfigTest, DirectoryServicesConfig, DGroup, DGroupList, UserList, UserSummary, UserModel}
import scala.collection.JavaConversions._
import scala._
import com.liquidlabs.admin.{Permission, AdminConfig, DataGroup, User, AdminSpace}
import org.apache.log4j.Logger
import com.liquidlabs.admin.AdminConfig.SecurityModel

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 10/04/2013
 * Time: 12:41
 * To change this template use File | Settings | File Templates.
 */
class Users (authUser:User, adminSpace:AdminSpace) extends Logging {

  def auditLogger = Logger.getLogger("AuditLogger")

  def getUser(name:String, uuid:String) : UserModel = {
    val u:User = adminSpace.getUser(name)
    if (u != null) return UserModel(u.username(), u.getRole().name(), u.password(), u.getEmail, u.getApps, u.getDataGroup, u.getPermissions.getPerm, u.getReportLogo, u.lastMod, uuid)
    null
  }
  def saveUser(user:UserModel, uuid:String) : UserList =  {
    auditLogger.info("event:SAVE_USER " + authUser.ndc + " against:" + user.userId)
    adminSpace.addUser(new User(user.userId, user.email, user.pwd, user.pwd.hashCode, "", Long.MaxValue, "", "", "", user.dataGroup, null, user.apps, user.logo, User.ROLE.valueOf(user.role)), true)
    list(uuid)
  }
  def deleteUser(name:String, uuid:String) : UserList = {
    adminSpace.deleteUser(name)
    list(uuid, name)
  }

  def  list(uuid:String, except:String="_AB_RA_CA_DA_BR_A_"): UserList = {
    val users: util.List[User] =  adminSpace.getUsers
    val results = users.toList.map((u:User) => UserSummary(u.username(), u.getEmail, u.getApps, u.getDataGroup, u.getPermissions.getPerm, u.lastMod, u.getRole.name()))

    UserList(results.filter((s:UserSummary) => !except.equals(s.userId)), uuid)
  }

  def syncUsers(uuid:String) : DirectoryServicesConfigTest = {

    adminSpace.changeSecurityModel(SecurityModel.DEFAULT.name());
    Thread.sleep(1000);
    adminSpace.changeSecurityModel(SecurityModel.EXTERNAL_LDAP.name());

    val users: util.List[User] =  adminSpace.getUsers
    val secName = adminSpace.getAdminConfig.getSecurityType.name()
    DirectoryServicesConfigTest("SecurityModel:" + secName + " Listed User Count:" + users.size(), uuid);
  }

  def  listDGroups(uuid:String) = {
    val dgroups: util.List[DataGroup] =  adminSpace.getDataGroups

    var results: List[DGroup] = List[DGroup]()

    dgroups.toList.foreach( (dg:DataGroup) => {
      results = results:::List(DGroup(dg.getName, dg.getInclude, dg.getExclude, dg.getChildren, dg.isEnabled, dg.getResourceGroup))
    })

    DGroupList(results, uuid)
  }
  def saveDGroup(name:String, include:String, exclude:String, children:String, enabled:Boolean, resourceGroup:String, uuid:String) : DGroupList = {
    auditLogger.info("event:SAVE_DG " +  authUser.ndc + " against:" + name)
    adminSpace.saveDataGroup(new DataGroup(name, include, exclude, children, enabled, resourceGroup))
    listDGroups(uuid)
  }
  def deleteDGroup(name:String, uuid:String) : DGroupList = {
    auditLogger.info("event:DELETE_DG " +  authUser.ndc + " against:" + name)
    adminSpace.deleteDataGroup(name)
    listDGroups(uuid)
  }
  def evaluateDGroup(name:String, uuid:String) : DGroupResult = {
    val msg = adminSpace.evaluateDGroup(name)
    new DGroupResult(msg,uuid)

  }

  def getSecurityConfig(uuid:String) : DirectoryServicesConfig = {
    val config: AdminConfig = adminSpace.getAdminConfig
    return DirectoryServicesConfig(config.getSecurityType.name(), config.externalLdapContext,
      config.externalLdapUser, config.externalLdapRole,
      config.externalLdapURL, config.externalLdapSystemUser, config.externalLdapSystemCredential, config.externalLdapFilter, config.externalLdapAdminFilter, uuid)
  }
  def saveSecurityConfig(config:DirectoryServicesConfig) : String = {
    auditLogger.info("event:SAVE_SEC " +  authUser.ndc)
    val adminConfig: AdminConfig = convertToAdminConfig(adminSpace.getAdminConfig,config)
    adminSpace.setAdminConfig(adminConfig)
    return "done"
  }
  def testSecurityConfig(config:DirectoryServicesConfig, uuid:String) : DirectoryServicesConfigTest = {
    //    adminSpace.getAdminConfig
    val adminConfig: AdminConfig =  convertToAdminConfig(new AdminConfig,config)
    val result = adminSpace.testAdminConfig(adminConfig)
    return DirectoryServicesConfigTest(result, uuid)
  }
  def changeSecurityModel(newModel:String, uuid:String) : ChangeSecurityModelOutput = {
    auditLogger.info("event:CHANGE_SEC_MODEL " +  authUser.ndc + " against:" + newModel)
    val result:String = adminSpace.changeSecurityModel(newModel)
    return ChangeSecurityModelOutput(result, uuid)

  }
  def convertToAdminConfig(admin:AdminConfig, config:DirectoryServicesConfig) : AdminConfig = {
    admin.securityType = config.currentModel
    admin.externalLdapContext = config.baseCN
    admin.externalLdapUser = config.userCN
    admin.externalLdapRole = config.groupCN
    admin.externalLdapURL = config.providerURL
    admin.externalLdapSystemUser = config.sysUser
    admin.externalLdapSystemCredential = config.sysCreds
    admin.externalLdapFilter = config.userFilter
    admin.externalLdapAdminFilter = config.adminFilter
    return admin
  }

}
