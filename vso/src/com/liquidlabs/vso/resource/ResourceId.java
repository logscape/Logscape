package com.liquidlabs.vso.resource;

import com.liquidlabs.orm.Id;

public class ResourceId {
    enum SCHEMA {  id, address, lastSeen, role }
    @Id
    String id;

    String address;

    String lastSeen;
    String role = "";

    public ResourceId() {
    }
    public ResourceId(String id, String address, String lastSeen, String role) {
        this.id = id;
        this.address = address;
        this.lastSeen = lastSeen;
        this.role = role;
    }
    public String toString() {
        if (lastSeen == null) lastSeen = "";
        return id + " / " + address + "/" + role +  " LastSeen:" + lastSeen;
    }

}
