package com.liquidlabs.vso.deployment;

import com.liquidlabs.orm.Id;

public class Deployed {
	@Id
	public String hash;
	public String name;
	
	public Deployed(){}
	public Deployed(String hash, String name) {
		this.hash = hash;
		this.name = name;
	}
	

}
