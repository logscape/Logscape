package com.liquidlabs.log.alert;

import java.util.List;

public interface TriggerFiredCallBack {

	void fired(List<String> leadingEvents);

}
