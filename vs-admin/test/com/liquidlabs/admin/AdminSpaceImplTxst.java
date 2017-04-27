package com.liquidlabs.admin;

import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 12/04/2013
 * Time: 17:12
 * To change this template use File | Settings | File Templates.
 */
public class AdminSpaceImplTxst {


    //    @Test  DodgyTest
    public void shouldGetUserList() throws Exception {

        //AdminSpaceImpl.main(new String[] { } );
        ORMapperFactory mapper = new ORMapperFactory(11999, "CRAP", 20 * 1024, 11999);
        mapper.start();

        LookupSpace lu = LookupSpaceImpl.getLookRemoteSimple("stcp://localhost:11000",mapper.getProxyFactory(),"xx");

        AdminSpace admin = AdminSpaceImpl.getRemoteService("TEST", lu, mapper.getProxyFactory());

        String sss = admin.addUser(new User("ggg", "email", "pwd", "pwd".hashCode(), "all", -1, "", "", "", "all", null, "*", "logo.png", User.ROLE.Read_Write_User), true);
        List<String> userIds = admin.getUserIds();

        System.out.println("GOT:" + userIds);

//        AdminSpaceImpl.boot("${LookupSpaceAddress}")

    }


}
