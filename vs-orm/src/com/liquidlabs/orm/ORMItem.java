package com.liquidlabs.orm;

public class ORMItem {
	public ORMItem() {
	}
	public ORMItem(String type, String id, String contents) {
		this.type = type;
		this.id = id;
		this.contents = contents;
	}
	String type;
	String id;
	String contents;

}
