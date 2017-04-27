package com.liquidlabs.log.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.log.LogProperties;
import com.persistit.Key;
import com.persistit.encoding.CoderContext;
import com.persistit.exception.ConversionException;
import org.joda.time.DateTime;

//import com.twitter.prestissimo.*;
//import com.twitter.prestissimo.encoding.*;
//import com.twitter.prestissimo.exception.*;

public class BucketKey implements KryoSerializable, Comparable {


    int logId;

    int timeMin;

    public BucketKey() {
    }

    public BucketKey(int logId, long timeMs) {
        this.logId = logId;
        this.timeMin = LogProperties.fromMsToMin(timeMs);
    }
    public BucketKey(int logId, int timeMin) {
        this.logId = logId;
        this.timeMin = timeMin;
    }

    public long getTimeMs() {
        return LogProperties.fromMinToMs(this.timeMin);
    }
    public int getTimeMin() {
        return this.timeMin;
    }
    public int logId() {
        return logId;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("[BucketKey:");
        buffer.append(" logId:");
        buffer.append(logId);
//		buffer.append(" timeMs:");
//		buffer.append(timeMin);
        buffer.append(" " + new DateTime(getTimeMs()));
        buffer.append("]");
        return buffer.toString();
    }
    //    public static class KeyCoder implements com.twitter.prestissimo.encoding.KeyCoder {
    public static class KeyCoder implements com.persistit.encoding.KeyCoder {
        @Override
        public void appendKeySegment(Key key, Object object, CoderContext context) throws ConversionException {
            BucketKey ff = (BucketKey) object;
            key.append(ff.logId);
            key.append(ff.timeMin);
        }

        @Override
        public Object decodeKeySegment(Key key, Class clazz, CoderContext context) throws ConversionException {
            int a = key.decodeInt();
            long b = key.decodeInt();
            return new  BucketKey(a,b);
        }
        @Override
        public boolean isZeroByteFree() throws ConversionException {
            return false;
        }
    }



    public int hashCode() {
        int hashCode = 1;
        hashCode = 31 * hashCode + (int) (+logId ^ (logId >>> 32));
        hashCode = 31 * hashCode + (int) (+timeMin ^ (timeMin >>> 32));
        return hashCode;
    }

    /**
     * Returns <code>true</code> if this <code>BucketKey</code> is the same as the o argument.
     *
     * @return <code>true</code> if this <code>BucketKey</code> is the same as the o argument.
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (o.getClass() != getClass()) {
            return false;
        }
        BucketKey castedObj = (BucketKey) o;
        return ((this.logId == castedObj.logId) && (this.timeMin == castedObj.timeMin));
    }
    public long hashMe() {

        // max 1 million files
        long maxFileIds = 1 * 1000 * 1000;
        // knock off the seconds...
        long tt = (timeMin)* maxFileIds;
        return tt + logId;
    }

    public int compareTo(Object o) {
        final BucketKey oo = (BucketKey) o;
        return compareLong(this.hashMe(), oo.hashMe());

    }
    static int compareLong(long l1, long l2)
    {
        if (l2 > l1)
            return -1;
        else if (l1 > l2)
            return 1;
        else
            return 0;
    }

    public String pk() {
        return "" + this.logId + this.timeMin;
    }

    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, this.logId);
        kryo.writeObject(output, this.timeMin);
    }

    public void read(Kryo kryo, Input input) {
        this.logId = kryo.readObject(input, int.class);
        this.timeMin = kryo.readObject(input, int.class);
    }

}
