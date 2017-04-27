package com.logscape.meter;

import com.liquidlabs.admin.DataGroup;
import com.liquidlabs.admin.User;
import com.liquidlabs.admin.UserSpace;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 29/10/14
 * Time: 08:35
 * To change this template use File | Settings | File Templates.
 */
public class AccountManagerImpl implements AccountManager {

    private static String iid = "AccountManager";
    private static final Logger LOGGER = Logger.getLogger(AccountManager.class);
    private final AccountManagerJMX accountManagerJMX;

    public String getId() {
        return iid;
    }
    private final UserSpace userSpace;
    private final LogSpace logSpace;
    private MeterService meterService;

    public AccountManagerImpl(UserSpace userSpace, LogSpace logSpace, MeterService meterService, ScheduledExecutorService scheduler) {
        this.userSpace = userSpace;
        this.logSpace = logSpace;
        this.meterService = meterService;
        this.accountManagerJMX = new AccountManagerJMX(this);
        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    deleteInactiveAccounts();
                    Thread.sleep(60 * 1000);
                    deleteInactiveData();
                } catch (Exception e) {
                    LOGGER.warn("Cleanup Failure:" + e, e);

                }
            }
        }, 1, 1, TimeUnit.DAYS);


    }

    private void deleteInactiveAccounts() {

        List<String> userIds = userSpace.getUserIds();
        LOGGER.info("Total Accounts Before:" + userIds);
        for (String userId : userIds) {
            if (userId.contains("@")) {
                try {
                    User user = userSpace.getUser(userId);
                    if (user.getLastMod() < new DateTime().minusDays(SAASProperties.getMaxInactiveDaya()).getMillis()) {
                        LOGGER.info("Expiring:" + user);
                        deleteUserAccount(userId);
                    }
                } catch (Throwable t) {
                    LOGGER.warn("OperationFailed:" +t, t);
                }
            }
        }
        List<String> userIds2 = userSpace.getUserIds();
        LOGGER.info("Total Accounts After:" + userIds2);

    }
    private void deleteInactiveData() {

        List<String> userIds = userSpace.getUserIds();
        LOGGER.info("Total Accounts Before:" + userIds);
        for (String userId : userIds) {
            if (userId.contains("@")) {
                try {
                    meterService.deleteInactiveData(userId);
                } catch (Throwable t) {
                    LOGGER.warn("OperationFailed:" +t, t);
                }
            }
        }
        List<String> userIds2 = userSpace.getUserIds();
        LOGGER.info("Total Accounts After:" + userIds2);

    }

    public void makeRemotable(ProxyFactory proxy, LookupSpace lookupSpace){
        proxy.registerMethodReceiver(iid, this);
        ServiceInfo serviceInfo = new ServiceInfo(AccountManager.class.getSimpleName(), proxy.getEndPoint(), null, JmxHtmlServerImpl.locateHttpUrL(), "saas-meter", getId(), VSOProperties.getZone(), VSOProperties.getResourceType());
        lookupSpace.registerService(serviceInfo, -1);
    }


    public String createUserAccount(String email, String pwd, String hostsList, int dailyMb, int dataRetentionDays) {
        String userId = email;
        User user = this.userSpace.getUser(userId);

        if (user != null) {
            return "ERROR: This account already exists. Choose a different name than:" + userId + " (or delete the account)";
        }

        LOGGER.info("Creating Account:" + email);

        DataGroup datagroup = new DataGroup(userId, "tag:" + userId, "", "", true, "");
        User theUser = new User(userId, email, pwd, pwd.hashCode(), "", Long.MAX_VALUE, "", "", "", datagroup.getName(), null, "", "", User.ROLE.Read_Write_User);

        String snap = WatchDirectory.ARCHIVE_TASKS.Snappy_Compress.name();
        WatchDirectory watchDirectory = new WatchDirectory(userId, theUser.username() +  "/**", "*", "", "", dataRetentionDays, "0,2," + snap, true, "", userId, true, true);

        this.userSpace.addUser(theUser, true);
        this.userSpace.saveDataGroup(datagroup);
        this.logSpace.saveWatch(watchDirectory);

        this.meterService.createAccount(userId, hostsList, dailyMb, true, null, dataRetentionDays);


        return theUser.toString();
    }

    @Override
    public String addIpToAccount(String userId, String ip) {
        User user = this.userSpace.getUser(userId);
        if (user == null) return "User not found:" + userId;
        meterService.flush();
        meterService.addIp(userId, ip);
        return "Ip:" + ip + " Added to:" + userId;
    }

    boolean isUserIdValid(String userId) {
        if (userId.contains(" ") || userId.contains("\t")) return false;
        for (char c : userId.toCharArray()) {
            if (isBetween(c, '0', '9')) continue;
            if (isBetween(c, 'A', 'Z')) continue;
            if (isBetween(c, 'a', 'z')) continue;
            return false;
        }
        return userId.length() >=6 && userId.length() <= 15;
    }

    private boolean isBetween(char c, char from, char too) {
        return c >= from && c <= too;
    }

    public User getUserAccount(String userId) {
        LOGGER.info("Get User:" + userId);

        User user = this.userSpace.getUser(userId);
        if (user != null) {
            user.lastMod =  DateUtil.shortDateTimeFormat2.print(System.currentTimeMillis());
            this.userSpace.addUser(user, true);
        }
        return user;

    }

    public String deleteUserAccount(String userId) {

        if (isNotAllowedToDelete(userId)) {
            LOGGER.info("Cannot delete account:" + userId);
            return "Cannot delete admin accounts!";
        }

        LOGGER.info("Deleting User:" + userId);
        User user = this.userSpace.getUser(userId);
        List<String> workSpaceIds = this.logSpace.listWorkSpaces(user);
        for (String workSpaceId : workSpaceIds) {
            if (workSpaceId.startsWith(userId)) this.logSpace.deleteWorkspace(workSpaceId, user);
        }
        List<String> searches = this.logSpace.listSearches(user);
        for (String searchId : searches) {
            if (searchId.startsWith(userId)) {
                try {
                    this.logSpace.deleteSearch(searchId, user);
                } catch (Throwable t) {
                }
            }
        }
        List<String> scheduleNames = this.logSpace.getScheduleNames(userId);
        for (String scheduleName : scheduleNames) {
            if (scheduleName.startsWith(userId)) {
                try {
                    this.logSpace.deleteSchedule(scheduleName);
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }

        this.userSpace.deleteUser(userId);
        this.userSpace.deleteDataGroup(userId);
        this.logSpace.removeWatch(userId);
        this.meterService.deleteAccount(userId);

        return "";
    }

    private boolean isNotAllowedToDelete(String userId) {
        return userId == null || userId.length() == 0 || userId.equals("admin") || userId.equals("sysadmin");
    }

    public String listAccounts() {
        return userSpace.getUserIds().toString();
    }



    public int getMaxDataVolume() {
        return SAASProperties.getMaxDataVolume();
    }

    public int getMaxDataRetention() {
        return SAASProperties.getMaxDataRetention();
    }

    String status = "Normal!";
    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String status() {
        return status;
    }

    public static AccountManager getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
        return getRemoteService(whoAmI, lookupSpace, proxyFactory, true);
    }

    public static AccountManager getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory, boolean wait) {
        return SpaceServiceImpl.getRemoteService(whoAmI, AccountManager.class, lookupSpace, proxyFactory, AccountManager.class.getSimpleName(), wait, false);
    }

}
