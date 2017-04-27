/**
 * 
 */
package com.liquidlabs.log.search.functions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

final public class IntValue implements KryoSerializable, Externalizable {
	int value;
	public IntValue(int intValue) {
		this.value = intValue;
	}
	public IntValue() {
	}
	final public int increment() {
		value++;
		return value;
	}
    final public void increment(int value) {
        this.value += value;
    }
	final public int value() {
		return value;
	}
	public String toString() {
		return Integer.toString(value);
	}
	
	public void read(Kryo kryo, Input in) {
		this.value = kryo.readObject(in, int.class);
	}
	public void write(Kryo kryo, Output out) {
		kryo.writeObject(out, value);
	}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.value);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.value = in.readInt();
    }
}