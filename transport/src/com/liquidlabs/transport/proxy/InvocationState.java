/**
 * 
 */
package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.serialization.Convertor;

public enum InvocationState implements Partial {
	HEADER(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			return super.next(payload);
		}; 
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
		}
	},
	SRC_ID(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)) spaceEvent.setSrcId(payload);
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			super.append(spaceEvent.getSrcId(), stringBuilder);
		}
	},
	DEST_ID(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)) spaceEvent.setDestId(payload);
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			super.append(spaceEvent.getDestId(), stringBuilder);
		}
	},
	CLIENT(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)) spaceEvent.setClientURI(payload);
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			super.append(spaceEvent.getClientURI(), stringBuilder);
		}
	},
	PARTITION(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)) spaceEvent.setPartitionName(payload);
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			super.append(spaceEvent.getPartitionName(), stringBuilder);
		}
	},
	METHOD_NAME(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)) spaceEvent.setMethodName(payload);
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			super.append(spaceEvent.getMethodName(), stringBuilder);
		}
	},
	RESULT(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)) {
				if (payload.contains(VSCAPE_REMOTE_EXCEPTION)) {
					throw new RuntimeException(payload);
				}
				spaceEvent.setResult(convertor.getObjectFromString(spaceEvent.getMethod().getReturnType(), payload));
			}
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			if (convertor != null) {
				String stringFromObject = convertor.getStringFromObject(spaceEvent.getResult());
				super.append(stringFromObject, stringBuilder);
			};
		}
	},
	ARGS(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)) {
				String decodeString = convertor.decodeString(payload);
				spaceEvent.setArgs(convertor.getObjectFromString(spaceEvent.getMethod().getParameterTypes(), decodeString));
			}
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			if (nulifyParams) {
				String nullStringFromObject = convertor.getNullStringFromObject(spaceEvent.getArgs());
				stringBuilder.append(convertor.encodeString(nullStringFromObject));
			} else {
				if (convertor != null) {
					String stringFromObject = convertor.getStringFromObject(spaceEvent.getArgs());
					stringBuilder.append(convertor.encodeString(stringFromObject));
				}
			}
			stringBuilder.append(DELIM);
		}
	},
	EVENT_TYPE(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			InvocationState next = super.next(payload);
			if (next.equals(this)){
//				spaceEvent.eventType = Type.valueOf(payload);
			}
			return next;
		}
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
			super.append(Event.Type.READ.name(), stringBuilder);
		}
	},
	FOOTER(null){
		public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
			return super.next(payload);
		}; 
		public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
			super.append(this.name(), stringBuilder);
		}
	};
	
	public static final String VSCAPE_REMOTE_EXCEPTION = "VScapeRemoteException";

	public static final String DELIM = Config.NETWORK_SPLIT_2;
	private final Partial partial;
	
	private InvocationState(Partial partial){
		this.partial = partial;
	}
	public void append(String value, StringBuilder stringBuilder) {
		stringBuilder.append(value).append(DELIM);
	}
	public InvocationState next(String payload) {
		int nextIndex = this.ordinal()+1;
		if (nextIndex < InvocationState.values().length && payload.equals(InvocationState.values()[nextIndex].name())){
			return InvocationState.values()[nextIndex];
		}
		return this;
	}
	
	public InvocationState parseStringToObject(String payload, Invocation spaceEvent, Convertor convertor) {
		return partial.parseStringToObject(payload, spaceEvent, convertor);
	}
	public void parseObjectToString(Invocation spaceEvent, StringBuilder stringBuilder, Convertor convertor, boolean nulifyParams) {
	}
}