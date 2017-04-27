package com.liquidlabs.common.collection.cache;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 17/12/14
 * Time: 13:02
 * To change this template use File | Settings | File Templates.
 */
public class ConcurrentLRUCacheTest {

    @Test
    public void shouldDoStuff() throws Exception {


        ConcurrentLRUCache<Integer, String> cache = new ConcurrentLRUCache<Integer, String>(5, 3);
        cache.put(100, "100");
        cache.put(101, "101");
        cache.get(100);
        Map<Integer,String> oldestAccessedItems = cache.getOldestAccessedItems(1);
        Assert.assertEquals(101,oldestAccessedItems.keySet().iterator().next().intValue());


    }

}