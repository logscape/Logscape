package com.logscape.portal;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Session {

    private String id;
    private User user;

    public Session() {
    }
    public Session(String id, User user) {
        this.id = id;
        this.user = user;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    public User getUser() {
        return this.user;
    }
    public void setUser(User user) {
        this.user = user;
    }

}