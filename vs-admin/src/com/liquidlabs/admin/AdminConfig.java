package com.liquidlabs.admin;

import java.util.HashMap;
import java.util.Hashtable;

import javax.naming.Context;

//import org.apache.directory.server.constants.ServerDNConstants;

import com.liquidlabs.orm.Id;

public class AdminConfig {
    public enum SecurityModel { DEFAULT, INTERNAL_LDAP, EXTERNAL_LDAP };
    public static String ID = AdminConfig.class.getSimpleName();

    enum SCHEMA {   externalLdapAuthentication, externalLdapContext, externalLdapFilter, externalLdapRole, externalLdapSystemCredential, externalLdapSystemUser, externalLdapURL, externalLdapUser,
        id,
        internalLdapAuthentication, internalLdapContext, internalLdapRole, internalLdapSystemCredential, internalLdapSystemUser, internalLdapURL, internalLdapUser,
        securityType, externalLdapAdminFilter};

    @Id
    String id = ID;

    public String securityType = SecurityModel.DEFAULT.name();

    public String internalLdapURL = "ldap://localhost:3269";
    public String internalLdapContext = "dc=logscape,dc=com";
    public String internalLdapUser = "ou=people";
    public String internalLdapRole = "ou=role";
    public String internalLdapSystemUser="admin";//ServerDNConstants.ADMIN_SYSTEM_DN;
    public String internalLdapSystemCredential="secret";
    public String internalLdapAuthentication="simple";


    public String externalLdapURL = "ldap://192.168.70.227:10389";
    public String externalLdapContext = "dc=logscape,dc=com";
    public String externalLdapUser = "ou=people";
    public String externalLdapRole = "ou=role";
    public String externalLdapFilter = "";
    public String externalLdapSystemUser="admin";//ServerDNConstants.ADMIN_SYSTEM_DN;
    public String externalLdapSystemCredential="secret";
    public String externalLdapAuthentication="simple";
    public String externalLdapAdminFilter = "";      // ADDED to Schema

    public AdminConfig() {

    }
    public AdminConfig(String externalURL, String externalUser, String externalCred, String externalContext, String externalUsers, String externalRoles, String externalFilter) {
        this.externalLdapURL = externalURL;
        this.externalLdapSystemUser = externalUser;
        this.externalLdapSystemCredential = externalCred;
        this.externalLdapContext = externalContext;
        this.externalLdapUser = externalUsers;
        this.externalLdapRole = externalRoles;
        this.externalLdapFilter = externalFilter;
    }



    public Hashtable getLdapEnv(SecurityModel internalOrExternal) {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        if (internalOrExternal.equals(SecurityModel.INTERNAL_LDAP)) {
            env.put(Context.PROVIDER_URL, internalLdapURL);
            env.put( Context.SECURITY_PRINCIPAL, internalLdapSystemUser);
            env.put( Context.SECURITY_CREDENTIALS, internalLdapSystemCredential);
            env.put( Context.SECURITY_AUTHENTICATION, internalLdapAuthentication);
        } else {
            env.put(Context.PROVIDER_URL, externalLdapURL);
            env.put( Context.SECURITY_PRINCIPAL, externalLdapSystemUser);
            env.put( Context.SECURITY_CREDENTIALS, externalLdapSystemCredential);
            env.put( Context.SECURITY_AUTHENTICATION, externalLdapAuthentication );

        }
        return env;
    }

    public SecurityModel getSecurityType() {
        return SecurityModel.valueOf(this.securityType);
    }

    public static AdminConfig fromMap(HashMap<String, String> payload) {
        AdminConfig result = new AdminConfig();
        result.securityType = payload.get("securityType");
        result.externalLdapURL = payload.get("externalLdapURL");
        result.externalLdapContext = payload.get("externalLdapContext");
        result.externalLdapUser = payload.get("externalLdapUser");
        result.externalLdapRole = payload.get("externalLdapRole");
        result.externalLdapSystemUser = payload.get("externalLdapSystemUser");
        result.externalLdapSystemCredential = payload.get("externalLdapSystemCredential");
        return result;
    }
}
