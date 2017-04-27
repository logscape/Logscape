package com.liquidlabs.vso.container;

public interface AddListener {
	void success(String resourceId);
	void failed(String resourceId, String errorMsg);
}
