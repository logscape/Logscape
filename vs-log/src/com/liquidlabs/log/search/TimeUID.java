package com.liquidlabs.log.search;

import com.liquidlabs.common.UID;

import java.io.*;

/**
 * Created by Neiil on 10/27/2015.
 */
public class TimeUID implements Comparable, Externalizable {


    public long timeMs;
    public String uid;

    public TimeUID(){
    }

    public TimeUID(long timeMs){
        this.timeMs = timeMs;
        this.uid = UID.getUUID();
    }
    public TimeUID(long timeMs, String uid){
        this.timeMs = timeMs;
        this.uid = uid;
    }
    public static TimeUID fromString(String item) {
        return new TimeUID(Long.parseLong(item.split("_")[0]), item.split("_")[1]);
    }
    public String toString() {
        return timeMs + "_" + uid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeUID timeUID = (TimeUID) o;

        if (timeMs != timeUID.timeMs) return false;
        return uid.equals(timeUID.uid);

    }

    @Override
    public int hashCode() {
        int result = (int) (timeMs ^ (timeMs >>> 32));
        result = 31 * result + uid.hashCode();
        return result;
    }

    @Override
    public int compareTo(Object o) {
        TimeUID other = (TimeUID) o;
        if (other.timeMs != this.timeMs) return Long.compare(this.timeMs, other.timeMs);
        return this.uid.compareTo(other.uid);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(this.timeMs);
        out.writeUTF(this.uid);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.timeMs = in.readLong();
        this.uid = in.readUTF();

    }
}
