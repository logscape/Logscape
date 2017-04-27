package com.liquidlabs.admin;


import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UserSpaceTest {

    Mockery context = new Mockery();
    private LookupSpace lookupSpace;
    private UserSpaceImpl userSpace;
    User.ROLE role = User.ROLE.Read_Write_User;

    @Before
    public void setUp() throws Exception {
        VSOProperties.setResourceType(VSOProperties.MANAGEMENT);

        lookupSpace = context.mock(LookupSpace.class);


        context.checking(new Expectations() {{
            one(lookupSpace).registerService(with(any(ServiceInfo.class)), with(any(long.class)));
            one(lookupSpace).unregisterService(with(any(ServiceInfo.class)));
        }});

        ORMapperFactory mapperFactory = new ORMapperFactory();
        SpaceServiceImpl spaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, "USER", Executors.newScheduledThreadPool(10), false, false, true);
        userSpace = new UserSpaceImpl(spaceService);
        userSpace.start();
    }

    @After
    public void tearDown() throws Exception {
        userSpace.stop();
    }


    @Test
    public void shouldStoreAndRetrieveUser() throws Exception {
        User user = new User("username","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "none", new HashMap<String,Object>(), "apps", "logo.png", role);
        userSpace.addUser(user, true);
        User user2 = userSpace.getUser("username");
        assertEquals(user.username(), user2.username());
        assertEquals("includeMe", user2.fileIncludes);
    }

    @Test
    public void shouldStoreAndListUser() throws Exception {
        User user = new User("username","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "none", new HashMap<String,Object>(), "apps", "logo.png", role);
        userSpace.addUser(user, true);
        List<User> users = userSpace.getUsers();
        for (User user2 : users) {
            if (user2.username().equals("username")) {
                assertEquals(user, user2);
                assertEquals("includeMe", user2.fileIncludes);
            }
        }
    }

    @Test
    public void shouldValidateUser() throws Exception {
        User user = new User("username","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "none", new HashMap<String,Object>(), "apps", "logo.png", role);
        userSpace.addUser(user, true);
        boolean validateUser = userSpace.authenticate("username", "pwd");
        assertTrue(validateUser);
    }

}
