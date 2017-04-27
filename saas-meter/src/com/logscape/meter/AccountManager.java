package com.logscape.meter;

import com.liquidlabs.admin.User;
import com.liquidlabs.admin.UserSpace;
import com.liquidlabs.transport.proxy.Remotable;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 29/10/14
 * Time: 08:38
 * To change this template use File | Settings | File Templates.
 */
public interface AccountManager  extends Remotable {

    String createUserAccount(String email, String pwd, String hostsList, int dailyMb, int dataRetentionDays);

    User getUserAccount(String userId);

    String deleteUserAccount(String userId);

    String addIpToAccount(String uid, String ip);

    String listAccounts();

    int getMaxDataVolume();

    int getMaxDataRetention();

    String status();
    void setStatus(String status);
}
