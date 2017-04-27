package com.liquidlabs.transport;

public interface EndPoint extends Sender, Receiver, PeerSender {
	public Receiver getReceiver();

}
