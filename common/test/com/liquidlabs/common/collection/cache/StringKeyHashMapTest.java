package com.liquidlabs.common.collection.cache;

import org.joda.time.DateTime;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 18/07/2013
 * Time: 16:04
 * To change this template use File | Settings | File Templates.
 */
public class StringKeyHashMapTest {
    @Test
    public void shouldDoSImpleStuff() throws Exception {
        StringKeyHashMap map = new StringKeyHashMap();
        map.put("hello", "helloValue");
        assertTrue(map.containsKey("hello"));
        assertEquals("helloValue", map.get("hello"));

        map.put("hello2", "helloValue2");
        map.put("hello2", "helloValue2");
        assertTrue(map.containsKey("hello2"));
        assertEquals("helloValue2", map.get("hello2"));


    }

}
