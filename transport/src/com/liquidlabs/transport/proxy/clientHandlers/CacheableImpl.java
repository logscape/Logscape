package com.liquidlabs.transport.proxy.clientHandlers;

import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 18/11/14
 * Time: 21:31
 * To change this template use File | Settings | File Templates.
 */
public class CacheableImpl implements ClientHandler {

    transient Map<String,CacheValue> cached = new ConcurrentHashMap<String, CacheValue>();

    @Cacheable
    public void sample() {
    }

    @Override
    public Object invoke(ProxyCaller client, AddressHandler addressHandler, Method method, Object[] args) throws InterruptedException {
        cleanupCachedInvs();

        Cacheable annotation = method.getAnnotation(Cacheable.class);
        int secondsToLive = annotation.ttl();
        try {

            String argsString = argsToString(args);
            String cacheKey = method.toString() + argsString;
            synchronized (this) {
                if (getCached().containsKey(cacheKey)) {
                    CacheValue cacheValue = getCached().get(cacheKey);
                    if (cacheValue.isRecentEnough()) return cacheValue.result;
                    else getCached().remove(cacheKey);
                }
            }

            Object result = client.sendWithRetry(method, args);
            getCached().put(cacheKey, new CacheValue(result, secondsToLive));
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private String argsToString(Object[] args) {
        if (args == null) return "null";
        StringBuilder result = new StringBuilder();
        for (Object object : args) {
            if (object == null) result.append("null");
            else result.append(String.valueOf(object));
            result.append(",");
        }
        return result.toString();
    }

    private Map<String, CacheValue> getCached() {
        if (cached == null) cached = new ConcurrentHashMap<String, CacheValue>();
        return cached;
    }


    protected void cleanupCachedInvs() {
        try {

            Set<String> keySet = getCached().keySet();
            for (String key : keySet) {
                CacheValue cacheValue = getCached().get(key);
                synchronized (this) {
                    if (!cacheValue.isRecentEnough()) getCached().remove(key);
                }
            }
        } catch (Throwable t) {
        }
    }


    private static class CacheValue {
        public long cachedTime = 0;
        public Object result;
        private final int secondsToLive;
        public CacheValue(Object value, int secondsToLive) {
            this.secondsToLive = secondsToLive;
            this.cachedTime = System.currentTimeMillis();
            this.result = value;
        }
        public boolean isRecentEnough() {
            return (cachedTime > System.currentTimeMillis() -  secondsToLive * 1000);
        }
    }
}
