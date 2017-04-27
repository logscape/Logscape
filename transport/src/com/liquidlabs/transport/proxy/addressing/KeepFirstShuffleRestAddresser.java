package com.liquidlabs.transport.proxy.addressing;

import java.util.Collections;

public class KeepFirstShuffleRestAddresser extends KeepOrderedAddresser {
	
	public KeepFirstShuffleRestAddresser() {
		super();
	}
	public KeepFirstShuffleRestAddresser(String serviceName) {
		super(serviceName);
	}
	
	@Override
	protected void messWithList() {
		Collections.shuffle(this.endPoints);
	}

}
