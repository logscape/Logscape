package com.liquidlabs.log;

import com.liquidlabs.log.explore.Explore;
import com.liquidlabs.log.space.FieldSetListener;
import com.liquidlabs.log.space.LogConfigListener;
import com.liquidlabs.log.space.LogRequestHandler;

public interface TailerListener extends CancellerListener, LogConfigListener, LogRequestHandler, FieldSetListener, Explore {

}
