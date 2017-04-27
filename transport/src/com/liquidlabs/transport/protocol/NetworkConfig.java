package com.liquidlabs.transport.protocol;

public class NetworkConfig {
	
	public static String TCP_PROTOCOL = "TCP";
	public static String HEADER = "LL_" + TCP_PROTOCOL;
	public static byte[] HEADER_BYTES = HEADER.getBytes();
	public static byte[] HEADER_BYTE_SIZE_1 = new byte[HEADER.length() + (Integer.SIZE) / 8];
	public static byte[] HEADER_BYTE_SIZE_2 = new byte[HEADER.length() + (2 * Integer.SIZE) / 8];

}
