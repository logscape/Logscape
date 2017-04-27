package com.liquidlabs.vso;

import com.liquidlabs.orm.Id;

public class ClientTestData {

	@Id
	String id;
	
	String payload;

	public void set(String string, String string2) {
		this.id = string;
		this.payload = string2;
	}
	public String toString() {
		return super.toString() + " id:" + id + " p:" + this.payload;
	}
}
