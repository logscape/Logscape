package com.liquidlabs.vso.resource;

import com.google.common.base.Splitter;
import com.google.common.hash.BloomFilter;
import com.liquidlabs.common.StringUtil;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 20/03/2014
 * Time: 12:14
 * To change this template use File | Settings | File Templates.
 */
public class BloomMatcher {
    BloomFilter<CharSequence> bloomFilter;
    Set<String> hostsPattern;

    public BloomMatcher(){
    }
    public BloomMatcher(BloomFilter filter, Set<String> hosts){
        setMatchers(filter, hosts);
    }

    public void setMatchers(BloomFilter filter, Set<String> hosts) {
        this.bloomFilter = filter;
        this.hostsPattern = hosts;
    }
    public boolean isMatch(String hostname) {
        if (bloomFilter == null && (hostsPattern == null || hostsPattern.size() == 0)) return true;
        if (bloomFilter != null && bloomFilter.mightContain(hostname)) return true;

        for (String splitPath : hostsPattern) {
            if (splitPath.length() == 0) continue;
            if (splitPath.contains("*") && hostname.matches(splitPath)) return true;

            if (StringUtil.containsIgnoreCase(hostname, splitPath)) return true;
        }

        return false;

    }
}
