package com.liquidlabs.log.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.LogProperties;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.exception.ConversionException;

//import com.twitter.prestissimo.*;
//import com.twitter.prestissimo.encoding.*;
//import com.twitter.prestissimo.exception.*;


public class Bucket implements KryoSerializable {
	public BucketKey key;
	
	public long startPos = Long.MAX_VALUE;
	public int firstLine;	
	public int lastLine = -1;
    private int lastTime;
	
	public Bucket(){};
	
	public Bucket(int logId, long time) {
		this.key = new BucketKey(logId, time);
		this.lastTime = LogProperties.fromMsToMin(time);
	}
	
	public void update(Line line) {
        int lineTime = LogProperties.fromMsToMin(line.time());
		if (line.position() < startPos) {
			startPos = line.position();
			firstLine = line.number();
		}
		if (line.number() > lastLine) {
			lastLine = line.number();
			lastTime = lineTime;
		}

		if (lineTime >= lastTime) {
			lastTime = lineTime;
			lastLine = line.number();
		}
	}

	public int numberOfLines() {
		return (lastLine - firstLine)+1;
	}

	public long startingPosition() {
		return startPos;
	}

	public int lastLine() {
		return lastLine;
	}
	
	public int firstLine() {
		return firstLine;
	}

	final public long time() {
		return key.getTimeMs();
	}
    final public int timeMin() {
        return this.lastTime;
    }

    public String toString() {
        if (key == null) key = new BucketKey();
		return String.format("Bucket %s time:%s lines %d<->%d filePos:%d", key.toString(), DateUtil.shortDateTimeFormat3.print(key.getTimeMs()), firstLine, lastLine, startPos);
	}

	public boolean containsLine(int line) {
		return this.firstLine <= line && this.lastLine >= line;
	}

    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, this.key);
        kryo.writeObject(output, this.startPos);
        kryo.writeObject(output, this.firstLine);
        kryo.writeObject(output, this.lastLine);
        kryo.writeObject(output, this.lastTime);
    }

    public void read(Kryo kryo, Input input) {
        this.key = kryo.readObject(input, BucketKey.class);
        this.startPos = kryo.readObject(input, long.class);
        this.firstLine = kryo.readObject(input, int.class);
        this.lastLine = kryo.readObject(input, int.class);
        this.lastTime = kryo.readObject(input, int.class);
    }

    public void setKey(BucketKey key) {
        this.key = key;
    }

    public BucketKey getKey() {
        return key;
    }

//    public static class ValueCoder implements com.twitter.prestissimo.encoding.ValueCoder {
    public static class ValueCoder implements com.persistit.encoding.ValueCoder {

        @Override
        public void put(Value value, Object object, CoderContext context) throws ConversionException {
            Bucket oo = (Bucket) object;
            value.put(oo.startPos);
            value.put(oo.firstLine);
            value.put(oo.lastLine);
            value.put(oo.lastTime);
        }

        @Override
        public Object get(Value value, Class clazz, CoderContext context) throws ConversionException {
            Bucket bucket = new Bucket();
            value.registerEncodedObject(bucket);
            bucket.set(value.getLong(), value.getInt(), value.getInt(), value.getInt());
            return bucket;
        }    }

    private void set(long startPos, int firstLine, int lastLine, int lastTime) {
        this.startPos = startPos;
        this.firstLine = firstLine;
        this.lastLine = lastLine;
        this.lastTime = lastTime;
    }
}
