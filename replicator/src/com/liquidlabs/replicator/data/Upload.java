package com.liquidlabs.replicator.data;

import org.joda.time.DateTimeUtils;

import com.liquidlabs.orm.Id;

public class Upload {
    @Id
    public String id;
    public String hash;
    public String fileName;
    public String path;
    public int pieces;
    public long when;

    public Upload(){}

    public Upload(String hash, String fileName, String path, int pieces) {
        this.id = String.format("%s%s%s", path,fileName,hash);
        this.hash = hash;
        this.fileName = fileName;
        this.path = path.replace("\\","/");
        this.pieces = pieces;
        this.when = DateTimeUtils.currentTimeMillis();
    }
    public void setWhen(long when) {
        this.when = when;
    }
    public boolean equals(Object arg0) {
        return this.id.equals(((Upload)arg0).id);
    }

}
