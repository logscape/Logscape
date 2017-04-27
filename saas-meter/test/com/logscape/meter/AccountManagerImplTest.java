package com.logscape.meter;

import com.liquidlabs.admin.UserSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.logserver.LogServer;
import junit.framework.Assert;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 29/10/2014
 * Time: 13:57
 * To change this template use File | Settings | File Templates.
 */
public class AccountManagerImplTest {

    Mockery context = new Mockery();
    final LogSpace logSpace = context.mock(LogSpace.class);
    final UserSpace userSpace = context.mock(UserSpace.class);
    final MeterService meterService= context.mock(MeterService.class);
    @Test
    public void testIsUserIdValid() throws Exception {
        context.checking(new Expectations() {{
            ignoring(logSpace);
            ignoring(userSpace);
            ignoring(meterService);
        }});


        AccountManagerImpl accountManager = new AccountManagerImpl(userSpace, logSpace, meterService, Executors.newScheduledThreadPool(1));
        Assert.assertFalse("Too short", accountManager.isUserIdValid("neil"));
        Assert.assertTrue("Valid Pwd", accountManager.isUserIdValid("neil1234"));


    }
}
