/**
 * Used to represent Before and After state of items being written into/from a Space. It also contains the owner so it should prevent
 * recipients from wiping out non-owned data
 */
package com.liquidlabs.space.map;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.liquidlabs.transport.serialization.Convertor;

public class NewState implements Externalizable {
	
	String srcUID;
	String partition;
	int id;
	int index;
	String existingKey;
	String existingValue;
	String newKey;
	String newValue;


	public NewState() {
	}
	public NewState(String srcUID, String partition, int id, int index, String existingKey, String existingValue, String newKey, String newValue) {
		this.srcUID = srcUID;
		this.partition = partition;
		this.id = id;
		this.index = index;
		this.newKey = newKey;
		this.existingKey = existingKey;
		this.existingValue = existingValue;
		this.newValue = newValue;
	}
	public static NewState deserialize(byte[] stringVersion) {
		NewState deserialized;
		try {
			deserialized = (NewState) Convertor.deserialize(stringVersion);
			return deserialized;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	public void readExternal(ObjectInput is) throws IOException, ClassNotFoundException {
		this.srcUID = is.readUTF();
		this.partition = is.readUTF();
		this.id = is.readInt();
		this.index = is.readInt();
		
		this.existingKey = is.readUTF();
		if (this.existingKey.equals("null")) this.existingKey = null;
		
		this.existingValue = is.readUTF();
		if (this.existingValue.equals("null")) this.existingValue = null;
		
		this.newKey = is.readUTF();
		if (this.newKey.equals("null")) this.newKey = null;
		
		this.newValue = is.readUTF();
		if (this.newValue.equals("null")) this.newValue = null;
		
	}
	public void writeExternal(ObjectOutput os) throws IOException {
		os.writeUTF(this.srcUID);
		os.writeUTF(this.partition);
		os.writeInt(this.id);
		os.writeInt(this.index);
		os.writeUTF(this.existingKey == null ? "null" : this.existingKey);
		os.writeUTF(this.existingValue == null ? "null" : this.existingValue);
		os.writeUTF(this.newKey == null ? "null" : this.newKey);
		os.writeUTF(this.newValue == null ? "null" : this.newValue);
	}
	@Override
	public String toString() {
		return getClass().getName() + " src:" + srcUID + " nK:"+ this.newKey + " eK:" + existingKey;
	}
	public boolean isRemove() {
		return newKey != null && newValue == null;
	}
}