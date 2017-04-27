package com.liquidlabs.util;

import com.liquidlabs.orm.Id;

public class AccountHolder {

	@Id
	String UID;

	public AccountHolder(String bundleId) {
		this.UID = bundleId;
	}

	public void addCost(String date, int cost) {

	}

}
