package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.common.net.URI;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 18/11/14
 * Time: 21:23
 * To change this template use File | Settings | File Templates.
 */
public class HashbleAddressByParamImplTest {

    HashbleAddressByParamImpl client = new HashbleAddressByParamImpl();


    @Test
    public void shouldFinaAUrlInTheList() throws Exception {
        URI first = new URI("http://192.168.70.8:11000");
        URI second = new URI("http://192.168.71.17:11000");
        List<URI> list = Arrays.asList(first, second);

        assertTrue(client.hashUrl(Integer.MAX_VALUE, list).toString().contains("71.17"));
        assertTrue(client.hashUrl(100, list).toString().contains("71.17"));

        assertNotNull(client.hashUrl("agent.log".hashCode(), list));

        assertTrue(client.hashUrl(-10, list).toString().contains("70.8"));
        assertTrue(client.hashUrl(Integer.MIN_VALUE, list).toString().contains("70.8"));
    }
    @Test
    public void shouldGetGoodHashIndexWOff() throws Exception {
        int index = 0;


        for (int i = -10; i < 10; i += 5) {
            index = client.getHashForList(i,-10, +10, 4);
            System.out.println(i + ">"+index);
//			assertEquals("pos:"+ i, i, index);
        }

    }
    @Test
    public void shouldGetGoodHashIndex() throws Exception {
        int index = 0;


        index = client.getHashForList("someHostc:/file.stuff.log".hashCode() , ((long)Integer.MAX_VALUE) * 2l, Arrays.asList("one","two").size(), Integer.MAX_VALUE);
        System.out.println(index);

        int maxPos = 10;
        for (int i = 0; i < maxPos; i++) {
            index = client.getHashForList(i,0,maxPos,10);
            System.out.println(i + ">"+index);
            assertEquals(i, index);
        }

        index = client.getHashForList(Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Arrays.asList("one","two").size()-1);
        System.out.println(index);
        assertEquals(1, index);

        index = client.getHashForList(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Arrays.asList("one","two").size()-1);
        System.out.println(index);
        assertEquals(0, index);
    }
}
