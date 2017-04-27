package com.liquidlabs.log.space;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.Remotable;

public interface FieldSetListener extends Remotable {
	
	String getId();
	
	@FailFastAndDisable
	void remove(FieldSet data);
	
	@FailFastAndDisable
	void add(FieldSet data);

	@FailFastAndDisable
	void update(FieldSet data);

}
