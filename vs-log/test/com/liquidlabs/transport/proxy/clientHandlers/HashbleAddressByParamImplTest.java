package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.net.URI;
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 20/11/2014
 * Time: 09:32
 * To change this template use File | Settings | File Templates.
 */
public class HashbleAddressByParamImplTest {
    @Test
    public void testHashUrlTwoAddresses() throws Exception {

        HashbleAddressByParamImpl hasher = new HashbleAddressByParamImpl();
        List<URI> list = Arrays.asList(new URI("stcp://localhost:10000"), new URI("stcp://localhost:1111"));
        Set<String> hashed = new HashSet<String>();
        for (int i = 0; i < 10; i++) {
            int hash2 = MurmurHash3.hashString((i + ""), 1024);
            URI uri = hasher.hashUrl(hash2, list);
            System.out.println(i + "\t " + hash2 + ":\t" + uri);
            hashed.add(uri.toString());
        }
        Assert.assertEquals("Should have distributed across 2 addresses", 2, hashed.size());
    }

    @Test
    public void testHashUrlThreeAddresses() throws Exception {

        HashbleAddressByParamImpl hasher = new HashbleAddressByParamImpl();
        List<URI> list = Arrays.asList(new URI("stcp://localhost:10000"), new URI("stcp://localhost:1111"), new URI("stcp://localhost:2222"));
        Set<String> hashed = new HashSet<String>();
        for (int i = 0; i < 10; i++) {
            int hash2 = MurmurHash3.hashString((i + ""), 1024);
            URI uri = hasher.hashUrl(hash2, list);
            System.out.println(i + "\t " + hash2 + ":\t" + uri);
            hashed.add(uri.toString());
        }
        Assert.assertEquals("Should have distributed across 3 addresses", 3, hashed.size());
    }

}
