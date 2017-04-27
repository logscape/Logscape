/**
 * 
 */
package com.liquidlabs.transport.protocol;

public enum State {
	HEADER, TYPE, SIZE, BODY, CALL_READER
}