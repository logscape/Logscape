package com.liquidlabs.orm;

import java.lang.reflect.Field;

/**
 * The Mapping is used to store and reassemble object relationships from the space
 * TODO: make work with one-many relationships
 * TODO: use annotations to drive
 */
public class Mapping {
	private String type = "<Mapping>";
	private String parentId;
	private String parentTypeName;
	private String fieldName;
	private String childId;
	private String childTypeName;

	@SuppressWarnings("unchecked")
	public Mapping(String parentId, Class parentType, String fieldName, String childId, Class childType) {
		this.parentId = parentId;
		this.parentTypeName = parentType.getName();
		this.fieldName = fieldName;
		this.childId = childId;
		this.childTypeName = childType.getName();
	}

	public Mapping(Object parent, String fieldName, Object child) {
		this(getId(parent), parent.getClass(), fieldName, getId(child), child.getClass());
	}

	public Mapping() {
	}

	public void apply(Object parent, Object child) {
		try {
			Field declaredField = parent.getClass().getDeclaredField(fieldName);
			declaredField.setAccessible(true);
			declaredField.set(parent, child);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	static private String getId(Object parent) {
		try {
			Field parentId = parent.getClass().getDeclaredField("id");
			parentId.setAccessible(true);
			return parentId.get(parent).toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	public String getParentId() {
		return parentId;
	}

	public String getParentTypeName() {
		return parentTypeName;
	}

	public String getFieldName() {
		return fieldName;
	}

	public String getChildId() {
		return childId;
	}

	public String getChildTypeName() {
		return childTypeName;
	}


}
