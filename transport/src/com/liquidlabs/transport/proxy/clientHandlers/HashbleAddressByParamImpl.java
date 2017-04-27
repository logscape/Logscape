package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 18/11/14
 * Time: 20:48
 * To change this template use File | Settings | File Templates.
 */
public class HashbleAddressByParamImpl implements ClientHandler {

    @HashableAddressByParam
    public void sample() {
    }

    @Override
    public Object invoke(ProxyCaller client, AddressHandler addressHandler, Method method, Object[] args) throws InterruptedException {
        if (args.length > 0) {
            String hashParam = args[0].toString();
            if (hashParam.contains(".")) hashParam = hashParam.substring(0, hashParam.lastIndexOf("."));
            int hash = MurmurHash3.hashString((hashParam), 1024);
            List<URI> endPointURIs = addressHandler.getEndPointURIs();
            Collections.sort(endPointURIs, new Comparator<URI>() {
                public int compare(URI o1, URI o2) {
                    String s1 = o1.getHost() + o1.getPort();
                    String s2 = o2.getHost() + o2.getPort();
                    return s1.compareToIgnoreCase(s2);
                }
            });
            // now hash to a matching endpoint and send it...
            URI useUrl = hashUrl(hash, endPointURIs);
            try {
                return client.clientExecute(method, useUrl, args, false, -1);
            } catch (Exception e) {
                addressHandler.registerFailure(useUrl);
                throw new RuntimeException(e);
            }
        }


        return null;
    }
    URI hashUrl(int hash, List<URI> endPointURIs) {
        int chooseItem = getHashForList(hash, Integer.MIN_VALUE, Integer.MAX_VALUE, endPointURIs.size());
        if (chooseItem > endPointURIs.size() -1) {
            chooseItem = endPointURIs.size()-1;
        }
        if (chooseItem < 0) chooseItem = 0;
        return endPointURIs.get(chooseItem);
    }

    /**
     * Need a number less than the size
     * @param hash (Hashes can be negative)
     * @return
     */
    int getHashForList(long hash, long minValue, long maxValue, long totalUnits) {
        long range = maxValue - minValue;
        // -5 - range -10 - +10 - range 20, -5 =

        long hashStart = hash - minValue;
        double fraction = ((double) hashStart)/(range);
        int i = (int) (fraction * totalUnits);

        // this should not be possible unless there are bad input values
        if (i < 0) return 0;
        if (i > maxValue) return (int) maxValue;
        return i;
    }

}
