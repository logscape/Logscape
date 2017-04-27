package com.liquidlabs.admin;


import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.*;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.serialization.ObjectTranslator;
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
import java.util.Set;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataGroupTest {

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
    public void shouldSafelyRemoveDataGroupWithEmptyName() throws Exception {
        userSpace.saveDataGroup(new DataGroup("all", "", "excl", "c1,c2", true, "hosts:AA"));
        userSpace.saveDataGroup(new DataGroup("c1","tag:includeMe","excl","",true,"hosts:BB"));
        userSpace.saveDataGroup(new DataGroup("c2","tag:xx","excl","c3",true,"group:CC"));
        userSpace.saveDataGroup(new DataGroup("c3","tag:logscape-logs","excl","",true,"group:DD"));

        try {
            userSpace.deleteDataGroup("");
            assertTrue("Should have blown up!", false);
        } catch (Throwable t) {
            assertTrue(true);
            System.err.println("Expect an exception here");
            t.printStackTrace();;
        }

        DataGroup dg = userSpace.getDataGroup("all", true);
        assertEquals("hosts:AA,hosts:BB,group:CC,group:DD", dg.getResourceGroup());
    }



    @Test
    public void shouldEvalSimpleDataGroupWithHostFilter() throws Exception {
        userSpace.saveDataGroup(new DataGroup("all", "", "excl", "c1,c2", true, "hosts:AA"));
        userSpace.saveDataGroup(new DataGroup("c1","tag:includeMe","excl","",true,"hosts:BB"));
        userSpace.saveDataGroup(new DataGroup("c2","tag:xx","excl","c3",true,"group:CC"));
        userSpace.saveDataGroup(new DataGroup("c3","tag:logscape-logs","excl","",true,"group:DD"));

        DataGroup dg = userSpace.getDataGroup("all", true);
        assertEquals("hosts:AA,hosts:BB,group:CC,group:DD", dg.getResourceGroup());
    }
    @Test
    public void shouldEvalSimpleDataGroup() throws Exception {

        String content = "com.liquidlabs.admin.DataGroup" + Config.OBJECT_DELIM + "sysadmin*\" + Config.OBJECT_DELIM + \"O_STR\" + Config.OBJECT_DELIM + \"true\" + Config.OBJECT_DELIM + \"O_STR\" + Config.OBJECT_DELIM + \"O_STR";
        DataGroup objectFromFormat = new ObjectTranslator().getObjectFromFormat(DataGroup.class, content);

        userSpace.saveDataGroup(new DataGroup("all", "", "excl", "c1", true, ""));
        userSpace.saveDataGroup(new DataGroup("c1","tag:includeMe","excl","c1",true,""));

        DataGroup dg = userSpace.getDataGroup("all", true);
        assertEquals("tag:includeMe", dg.getInclude());
    }


    @Test
    public void shouldListDataGroups() throws Exception {
        userSpace.addUser(new User("user1","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept1", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        userSpace.addUser(new User("user2","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept2", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        userSpace.addUser(new User("user3","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "all", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        List<String> results = userSpace.listDataGroups();
        assertEquals("got:" + results, 6, results.size());     //admin, guest, sysadmin are defaults, we add 3 giving 6
    }

    @Test
    public void shouldListAllUsersWhenIAmAInGroup() throws Exception {
        userSpace.addUser(new User("user1","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept1", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        userSpace.addUser(new User("user2","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept2", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        userSpace.addUser(new User("user3","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "all", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        Set<String> results = userSpace.getUserIdsFromDataGroup("", "dept1");
        assertEquals("[user1]", results.toString());
    }

    @Test
    public void shouldListAllUserWhenIAmAdmin() throws Exception {
        userSpace.addUser(new User("user1","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept1", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        userSpace.addUser(new User("user2","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept2", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        userSpace.addUser(new User("user3","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "all", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        Set<String> results = userSpace.getUserIdsFromDataGroup("admin", "all");
        assertEquals("[user2, admin, user1, user3, guest, sysadmin, user]", results.toString());
    }
    @Test
    public void shouldListUserInDepartmentPlusAll() throws Exception {
        userSpace.addUser(new User("user1","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept1", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        userSpace.addUser(new User("user2","email", "pwd", "pwd".hashCode(), "Admin", -1, "future","includeMe","", "dept2", new HashMap<String,Object>(), "apps", "logo.png", role), true);
        Set<String> results = userSpace.getUserIdsFromDataGroup("aUser", "dept2");
        assertEquals("[user2]", results.toString());

    }

}
