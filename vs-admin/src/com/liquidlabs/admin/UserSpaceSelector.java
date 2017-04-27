package com.liquidlabs.admin;

import org.apache.log4j.Logger;

import com.liquidlabs.admin.AdminConfig.SecurityModel;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.lookup.LookupSpace;

public class UserSpaceSelector {
    private static final Logger LOGGER = Logger.getLogger(UserSpaceSelector.class);

    private UserSpace userspace;
    private final String name;

    public UserSpaceSelector(String name, LookupSpace lookupSpace, ORMapperFactory mapperFactory) {
        this.name = name;

        SpaceServiceImpl userStateService = new SpaceServiceImpl(lookupSpace, mapperFactory, "USER" + name, mapperFactory.getScheduler(), true, true, false);
        userSpaceImpl = new UserSpaceImpl(userStateService);
        userSpaceImpl.start();
    }

    /**
     *
     * @return
     */
    UserSpace getUserSpace(AdminConfig adminConfig) {
        LOGGER.info("Activating Security Model:" + adminConfig.securityType);
        stopExistingStuff();

        if (adminConfig.getSecurityType().equals(SecurityModel.DEFAULT)) {
            try {
                return getEmbeddedSpace();
            } catch (Throwable t) {
                LOGGER.error("Failed to activate:" + adminConfig, t);
                throw new RuntimeException("Failed to switch security:" + adminConfig.securityType, t);
            }
        } else if (adminConfig.getSecurityType().equals(SecurityModel.INTERNAL_LDAP)) {
            LOGGER.error("MODE NOT AVAILABLE:" + adminConfig);
            throw new RuntimeException("MODE NOT AVAILABLE:" + adminConfig.securityType);
        } else if (adminConfig.getSecurityType().equals(SecurityModel.EXTERNAL_LDAP)) {
            LOGGER.error("MODE NOT AVAILABLE:" + adminConfig);
            throw new RuntimeException("MODE NOT AVAILABLE:" + adminConfig.securityType);
        }

        throw new RuntimeException("Failed to start  config specified:" + adminConfig.securityType);
    }

    UserSpaceImpl userSpaceImpl = null;
    public UserSpaceImpl getEmbeddedSpace() {
        return userSpaceImpl;
    }

    public void stopExistingStuff() {
        try {
            if (this.userspace != null && this.userspace != this.userSpaceImpl) this.userspace.stop();
        } catch (Throwable t) {
            LOGGER.error(t.toString(), t);
        }
    }


}
