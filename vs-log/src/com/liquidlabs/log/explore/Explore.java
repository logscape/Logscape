package com.liquidlabs.log.explore;

import com.liquidlabs.admin.User;

import java.util.*;
/**
 * Created by neil.avery on 25/03/2016.
 */
public interface Explore {
    Set<String> hosts(User user);
    List<String> dirs(User user, String host);
    String url();

}
