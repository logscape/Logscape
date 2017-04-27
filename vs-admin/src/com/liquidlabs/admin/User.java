package com.liquidlabs.admin;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.regex.FileMaskAdapter;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.orm.Id;
import com.liquidlabs.vso.agent.metrics.DefaultOSGetter;
import com.thoughtworks.xstream.XStream;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class User {


    private boolean admin;

    public enum ROLE { Read_Only_User, Read_Write_User, Team_Administrator, System_Administrator};

    public static ROLE defaultRole = ROLE.valueOf(System.getProperty("user.default.role", ROLE.Read_Write_User.name()));
    public static String defaultTeam = System.getProperty("user.default.team","all");

    enum SCHEMA { username, email, password, groups, passwordHash, expiresTime, expires, fileExcludes, department, metaData, fileIncludes, apps, defaultReportLogo, permissions, created, lastMod, role };


    @Id
    private String username;
    public String email;
    public String password;
    public String groups;
    public String department = "";
    public long passwordHash;
    public String expiresTime;
    public long expires;
    public String fileExcludes = "";
    public String fileIncludes = "*";
    public String apps = "";
    public String defaultReportLogo = "";
    public String created = "";
    public String lastMod = "";
    public int role = ROLE.Read_Write_User.ordinal();
    public transient String ndc;
    /**
     * User XML metadata
     */
    public String metaData;
    public Permission permissions;

    public User(){
        this.lastMod = DateUtil.shortDateTimeFormat.print(System.currentTimeMillis());
    }

    public User(String username, String email, String password, long passwordHash, String groups, long expires, String expiresTime,
                String fileIncludes, String fileExcludes, String department, HashMap<String, Object> metaData, String apps, String defaultReportLogo, ROLE role){
        setUsername(username);
        this.email = email;
        this.password = password.trim();
        this.passwordHash = passwordHash;
        this.expires = expires;
        this.expiresTime = expiresTime;
        this.groups = groups;

        if (fileExcludes == null) fileExcludes = "";
        this.fileExcludes = fileExcludes;
        if (fileIncludes == null) fileIncludes = "";
        this.fileIncludes = fileIncludes;
        if (apps == null) apps = "";
        this.apps = apps;
        if (department == null || department.length() == 0) department = "all";
        this.department = department.trim();
        this.permissions = Permission.make(role);
        this.role = role.ordinal();
        // never lockout the sysadmin user
        if (this.username == "sysadmin") this.role = ROLE.System_Administrator.ordinal();

        this.created = DateUtil.shortDateTimeFormat2.print(System.currentTimeMillis());
        this.lastMod = DateUtil.shortDateTimeFormat2.print(System.currentTimeMillis());


        if (defaultReportLogo == null) defaultReportLogo = "";
        this.defaultReportLogo = defaultReportLogo;

        if (metaData == null) metaData = new HashMap<String, Object>();
        metaData.put("fileIncl:", fileIncludes);
        metaData.put("fileExcl:", fileExcludes);
        metaData.put("apps:", apps);
        metaData.put("defaultReportLogo:", defaultReportLogo);
        this.setMetaData(metaData);
    }
    public long getLastMod() {
        try {
            return DateUtil.shortDateTimeParser2.parse(this.lastMod).getTime();
        } catch (ParseException e) {
            return 0;

        }
    }
    public void setUserRole (ROLE role) {
        this.role = role.ordinal();
    }

    public int hashCode() {
        return username.hashCode();
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof User)) return false;
        return username.equals(((User)obj).username);
    }

    private static String safeGet(Attribute userIdAttr) {
        try {
            if (userIdAttr != null) return userIdAttr.get().toString();
        } catch (Throwable t) {
            t.printStackTrace();;
        }
        return "";
    }


    public void setGroupsFromList(List<String> groups2) {
        Collections.sort(groups2);
        StringBuilder groupsString = new StringBuilder();
        for (String string : groups2) {
            groupsString.append(string).append(",");
        }
        this.groups = groupsString.toString();
    }

    public String toString() {
        return "User [email=" + email + ", expires=" + expires
                + ", expiresTime=" + expiresTime + ", groups=" + groups + ", password=XXX"
                 + ", passwordHash=" + passwordHash + ", username="
                + username + ", department=" + department + ", apps=" + apps +", defaultReportLogo=" + defaultReportLogo + "]";
    }

    public void sortGroups() {
        if (groups.length() == 0 || groups.equals(",")) {
            groups = "";
            return;
        }
        setGroupsFromList(getGroupsAsList());
    }

    public boolean isAuthorized(String action) {
        return getGroupsAsList().contains(action);
    }

    private ArrayList<String> getGroupsAsList() {
        return new ArrayList<String>(Arrays.asList(this.groups.split(",")));
    }
    public void setMetaData(HashMap<String, Object> metaData) {
        if (metaData == null) {
            this.metaData = "none";
            return;
        }
        XStream stream = new XStream();
        this.metaData = stream.toXML(metaData);
    }
    public HashMap<String,Object> getMetaData() {
        if (metaData == null) return new HashMap<String,Object>();
        XStream stream = new XStream();
        return (HashMap<String, Object>) stream.fromXML(this.metaData);
    }
    public static HashMap<String,Object> getMetaData(String metaDataXML) {
        if (metaDataXML == null || metaDataXML.equals("none")) return new HashMap<String,Object>();
        try {
            XStream stream = new XStream();
            return (HashMap<String, Object>) stream.fromXML(metaDataXML);
        } catch (Throwable t) {
            return new HashMap<String,Object>();
        }
    }
    public String getReportLogo() {
        if (defaultReportLogo == null || defaultReportLogo.trim().length() == 0) {
            defaultReportLogo = "./system-bundles/dashboard-1.0/logo.png";
        }
        return defaultReportLogo;
    }


    public boolean isFileAllowed(String host, String filename, String fileTag) {
        if (isExcluded(filename, fileTag)) return false;
        return isIncluded(filename, fileTag);
    }
    public boolean isIncluded(String filename, String fileTag) {

        buildFileIncludeWildcards();
        for (String fileMask : fileIncludeWildcards) {
            if (filename.matches(fileMask)) return true;
        }
        if (isMatchingTag(fileTag, getFileIncludes())) return true;
        return false;
    }

    private boolean isMatchingTag(String fileTag, String fileIncludes) {
        if (fileIncludes == null || fileIncludes.length() == 0) {
            fileIncludes = ".*";
        }
        String[] userIncludes = fileIncludes.split(",");
        for (String tag : userIncludes) {
            if (tag.startsWith("tag:")) {
                String cutTag = tag.substring("tag:".length());
                if (cutTag.equals(fileTag) || fileTag.contains(cutTag)) return true;
                else if (tag.contains("*")) {
                    String rex = SimpleQueryConvertor.convertSimpleToRegExp(cutTag);
                    if (fileTag.matches(rex)) return true;
                    else {
                        rex = cutTag.replaceAll("\\*", ".*");
                        if (fileTag.matches(rex)) return true;
                    }
                }
            }
        }
        return false;
    }
    public boolean isExcluded(String filename, String fileTag) {
        buildFileExcludeWildcards();
        for (String excludeCard : fileExcludeWildcards) {
            if (filename.matches(excludeCard)) return true;
        }
        if (isMatchingTag(fileTag, fileExcludes)) return true;
        return false;
    }

    private transient List<String> fileExcludeWildcards;
    synchronized private void buildFileExcludeWildcards() {
        if (fileExcludeWildcards == null) {
            String fileExcludesRegexpString = FileMaskAdapter.adapt(this.fileExcludes, DefaultOSGetter.isA());
            String[] fileExcludesRegexpParts = fileExcludesRegexpString.split(",");
            fileExcludeWildcards = new ArrayList<String>();
            for (String fileExcludeWildCard : fileExcludesRegexpParts) {
                fileExcludeWildcards.add(fileExcludeWildCard);
            }
        }
    }
    private transient List<String> fileIncludeWildcards;

    synchronized private void buildFileIncludeWildcards() {
        if (fileIncludeWildcards == null) {
            String regexpString = FileMaskAdapter.adapt(this.getFileIncludes(), DefaultOSGetter.isA());
            String[] regexpParts = regexpString.split(",");
            fileIncludeWildcards = new ArrayList<String>();
            for (String regexpPart : regexpParts) {
                if (regexpPart.trim().length() > 0) fileIncludeWildcards.add(regexpPart);
            }
        }
    }

    public String getDefaultApp() {
        if (apps.length() == 0) return "Logscape";
        String[] split = this.apps.split(",");
        if (split.length == 0) return "Logscape";
        return split[0];
    }

    /**
     * Retrun user includes plus their implicit scoping within logscape-dept
     * @return
     */
    public String getFileIncludes() {
        return this.fileIncludes + ",*logscape*schedule-all*,*logscape*schedule-" + department + "*";
    }
    public String getFileExcludes() {
        return this.fileExcludes;
    }


    public String username() {
        return username;
    }
    public String getEmail() {
        return this.email;
    }
    public String getDataGroup() {
        return this.department;
    }
    public String getPermissionGroup() {
        return this.groups;
    }
    public String getApps() {
        return this.apps;
    }
    public void setDataGroup(DataGroup dg) {
        this.fileIncludes = dg.getInclude();
        this.fileExcludes = dg.getExclude();
    }
    public String password() {
        return this.password;
    }
    public Permission getPermissions() {
        return permissions;
    }

    public void setPermissions(Permission permissions) {
        this.permissions = permissions;
    }
    public void setUsername(String username) {
        this.username = username.trim();
    }
    public String getGroupFilter(String entityIdFieldName) {
        if (isSystemAdministrator()) return "";
        return entityIdFieldName + " startsWith " + getDataGroup() +".";
    }
    public String getGroupId(String entityId) {
        if ( isSystemAdministrator() ) return entityId.trim();
        if (!entityId.startsWith(getDataGroup()+".")) return getDataGroup() +"." + entityId.trim();
        else return entityId.trim();
    }
    public ROLE getRole() {
        return ROLE.values()[this.role];
    }
    public boolean isSystemAdministrator() {
        return this.role == ROLE.System_Administrator.ordinal();
    }




}
