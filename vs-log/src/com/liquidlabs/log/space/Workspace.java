package com.liquidlabs.log.space;

import com.liquidlabs.orm.Id;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/02/2013
 * Time: 14:44
 * To change this template use File | Settings | File Templates.
 */
public class Workspace {
    @Id
    String name;
    String content;

    public Workspace(){
    }
    public Workspace(String name, String content) {
        this.name = name;
        this.content = content;
    }
    public String name() {
        return this.name;
    }
    public String content() {
        return this.content;
    }
}
