package com.liquidlabs.admin;

import com.liquidlabs.admin.AdminConfig.SecurityModel;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AdminSpaceImpl implements AdminSpace {

    private static final long DXXV = DateUtil.DX;
    private static final String VSP = "vspub.key";
    private static final String CN = "CompanyName";
    private static final String DD = "demo";
    private static final Logger LOGGER = Logger.getLogger(AdminSpaceImpl.class);
    private Emailer emailer;
    DateTimeFormatter formatter = DateTimeFormat.shortDateTime();
    private UserSpace userSpace;

    final SpaceServiceImpl adminConfigService;
    final UserSpaceSelector userSpaceSelector;
    private final ResourceSpace resourceSpace;

    public AdminSpaceImpl(LookupSpace lookupSpace, SpaceServiceImpl adminConfigService, ResourceSpace resourceSpace, UserSpaceSelector userSpaceSelector) {
        this.adminConfigService = adminConfigService;
        this.resourceSpace = resourceSpace;
        this.userSpaceSelector = userSpaceSelector;

    }
    public void start() {
        this.adminConfigService.start();
        this.adminConfigService.start(this,"admin-1.0");

        AdminConfig securityConfig = getAdminConfig();
        if (securityConfig == null) securityConfig = new AdminConfig();

        if (VSOProperties.isManagerOnly()) {
            if (this.adminConfigService.containsKey(EmailConfig.class, EmailConfig.ID)) {
                LOGGER.info("Using Stored EMAIL Config");
                this.emailer = new Emailer(this.adminConfigService.findById(EmailConfig.class, EmailConfig.ID));
            } else {
                LOGGER.info("Using Built-In EMAIL Config");
                this.emailer = new Emailer(new EmailConfig("smtps", "smtp.gmail.com", 465, System.getProperty("email.account","ll.email007@gmail.com"), System.getProperty("email.password","ll4bs008")));
                this.adminConfigService.store(this.emailer.getConfig(),-1);
            }
            this.adminConfigService.store(securityConfig, -1);

            setupPreregisteredData();
        } else {
            this.emailer = new Emailer(null);
        }
        this.userSpace = userSpaceSelector.getUserSpace(securityConfig);
        LOGGER.info("Got UserSpace:" + this.userSpace);

    }
    public void stop() {
        LOGGER.info("Stopping");
        this.userSpaceSelector.stopExistingStuff();
        this.adminConfigService.stop();
        LOGGER.info("Stopped");
    }

    public AdminConfig getAdminConfig() {
        return this.adminConfigService.findById(AdminConfig.class, AdminConfig.ID);
    }
    public String setAdminConfig(AdminConfig config) {
        return this.adminConfigService.store(config, -1);
    }

    public String testAdminConfig(AdminConfig config) {
        return "Not Implemented";
    }

    /**
     * One Of
     * @param newSecurityModelType  { DEFAULT, INTERNAL_LDAP, EXTERNAL_LDAP }
     * @return
     */
    public String changeSecurityModel(String newSecurityModelType) {
        LOGGER.info("changeSecurityModel" + newSecurityModelType);
        if (getLLC(true) == -1) {
            LOGGER.info("Not licensed - cannot change security model");
            return "Not licensed - cannot change security model";
        }
        AdminConfig securityConfig = getAdminConfig();
        if (securityConfig == null) securityConfig = new AdminConfig();

        if (securityConfig.securityType.equals(newSecurityModelType)) {
            return "Already using security Model:" + newSecurityModelType;
        }
        securityConfig.securityType = SecurityModel.valueOf(newSecurityModelType).name();
        this.adminConfigService.store(securityConfig, -1);
        this.userSpace = this.userSpaceSelector.getUserSpace(securityConfig);
        return "Config changed to:" + newSecurityModelType;
    }
    public String addUser(User user, boolean modifyExisting) {
        return userSpace.addUser(user, modifyExisting);
    }
    public void deleteUser(String username) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("DeleteUser:" + username);
        userSpace.deleteUser(username);
    }
    public List<String> getUserIds() {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("GetUserIds:");
        return userSpace.getUserIds();
    }
    public List<User> getUsers() {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("GetUsers:");
        return userSpace.getUsers();
    }

    public List<String> listDataGroups() {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("listDataGroups:");
        return userSpace.listDataGroups();
    }

    @Override
    public void acceptEula() {
        adminConfigService.store(new EulaAccepted(), -1);
    }

    @Override
    public boolean hasAcceptedEula() {
        return adminConfigService.findById(EulaAccepted.class, EulaAccepted.YES) != null;
    }

    public Set<String> getUserIdsFromDataGroup(String userId, String department) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("getUserIdsFromDataGroup:");
        return userSpace.getUserIdsFromDataGroup(userId, department);
    }
    public User getUser(String uid) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("getUser:");
        User user = userSpace.getUser(uid);
        if (user == null) {
            LOGGER.error("Failed to getUser:" + uid);
            return null;
        }
        if (this.getLLC(true) == -1) {
            if (user.username().equals("admin"))  {
                user.setUserRole(User.ROLE.System_Administrator);
                addUser(user, true);
            }
        }
        resourceSpace.setLLC(getLLC(true));
        DataGroup dg = getDataGroup(user.getDataGroup(), true);
        if (dg != null) user.setDataGroup(dg);
        return user;
    }
    public boolean authenticate(String username, String pwd) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("authenticate:");
        return userSpace.authenticate(username, pwd);
    }
    public boolean authorize(String uid, String action) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("authorize:");
        return userSpace.authorize(uid, action);
    }
    public List<String> getGroups() {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("getGroups:");
        return userSpace.getGroups();
    }
    synchronized final public long  getLLC(boolean reloadFromDisk) {

        return -1;
    }




    public void setEmailConfig(EmailConfig config) {
        emailer.configure(config);
        adminConfigService.store(config, -1);
    }
    public EmailConfig getEmailConfig(){
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Getting EmailConfig");
        return  adminConfigService.findById(EmailConfig.class, EmailConfig.ID);
    }
    public String sendEmail(String from, List<String> to, String title, String content) {
        return emailer.sendEmail(getEmailConfig(), from, to, title, content);
    }
    public String sendEmail(String from, List<String> to, String title, String content, String... attachmentsFilenames) {
        return emailer.sendEmail(getEmailConfig(), from, to, title, content, attachmentsFilenames);
    }
    private void setupPreregisteredData() {
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                LOGGER.warn("Stopping:" + AdminSpaceImpl.this.toString());
                AdminSpaceImpl.this.stop();
            }
        });

        if (getAdminConfig() == null) {
            changeSecurityModel(SecurityModel.DEFAULT.name());
        }
    }

    private void addFiles(ArrayList<String> result, String[] files, String dir) {
        if (files == null) return;
        for (String file : files) {
            result.add(dir + "/" + file);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) args = new String[] { "stcp://localhost:11000" };
        String path = new File(".").getAbsolutePath();
        System.out.println("Running From:" + path);

        boot(args[0]);
    }

    public static void boot(String lookupAddress){


        ORMapperFactory mapperFactory = LookupSpaceImpl.getSharedMapperFactory();

        LOGGER.info("Starting sharedEndpoint:" + mapperFactory.getPort());
        LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(), "AdminSpaceBoot");
        SpaceServiceImpl adminStateService = new SpaceServiceImpl(lookupSpace, mapperFactory, NAME, mapperFactory.getScheduler(), true, true, true);

        ResourceSpace resourceSpace = ResourceSpaceImpl.getRemoteService("adminSpace", lookupSpace, mapperFactory.getProxyFactory());

        AdminSpaceImpl service = new AdminSpaceImpl(lookupSpace, adminStateService, resourceSpace, new UserSpaceSelector(NAME, lookupSpace, mapperFactory));
        mapperFactory.getProxyFactory().registerMethodReceiver(NAME, service);
        service.start();
    }
    public static AdminSpace getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
        AdminSpace remoteService = SpaceServiceImpl.getRemoteService(whoAmI, AdminSpace.class, lookupSpace, proxyFactory, AdminSpace.NAME, false, false);
        LOGGER.info(String.format("%s Getting AdminSpace: %s", whoAmI, remoteService));
        return remoteService;
    }

    @Override
    public List<DataGroup> getDataGroups() {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("getDataGroups:");
        return userSpaceSelector.getEmbeddedSpace().getDataGroups();
    }

    @Override
    public DataGroup getDataGroup(String name, boolean evaluate) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("getDataGroup:");
        return userSpaceSelector.getEmbeddedSpace().getDataGroup(name, evaluate);
    }

    @Override
    public String evaluateDGroup(String name) {
        return userSpaceSelector.getEmbeddedSpace().evaluateDGroup(name);
    }

    @Override
    public User getUserForGroup(String group) {
        return userSpaceSelector.getEmbeddedSpace().getUserForGroup(group);
    }

    @Override
    public List<DataGroup> getDataGroups(String commaList) {
        return userSpaceSelector.getEmbeddedSpace().getDataGroups(commaList);
    }

    @Override
    public void saveDataGroup(DataGroup datagroup) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("saveDataGroup:");
        userSpaceSelector.getEmbeddedSpace().saveDataGroup(datagroup);
    }

    @Override
    public String deleteDataGroup(String name) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("deleteDataGroup:");
        userSpaceSelector.getEmbeddedSpace().deleteDataGroup(name);
        return "";
    }

    @Override
    public String exportConfig(String filter) {
        return userSpaceSelector.getEmbeddedSpace().exportConfig(filter);
    }

    public void importConfig(String xmlConfig) {
        userSpaceSelector.getEmbeddedSpace().importConfig(xmlConfig);
    }
}
