package com.liquidlabs.vso.lookup;

import com.liquidlabs.orm.Id;


public class Thing {
    @Id
    public String id;
    public String message;

    public Thing(){};

    public Thing(String id, String message){
        this.id = id;
        this.message = message;
    }


}
