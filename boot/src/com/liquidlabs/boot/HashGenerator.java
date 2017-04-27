package com.liquidlabs.boot;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashGenerator {
	
	// can also use SHA-1 but it is slower
	String digestType = System.getProperty("vso.digest.type", "MD5");
	public String createHash(String filename, File file) throws NoSuchAlgorithmException, IOException {
		MessageDigest digest = MessageDigest.getInstance(digestType);
		FileInputStream inputStream = new FileInputStream(file);
		try {
			byte[] buffer = new byte[16 * 1024];
			digest.update(filename.getBytes());
			while (inputStream.available() > 0) {
				int count = inputStream.read(buffer);
				digest.update(buffer, 0, count);
			}
		} finally {
			inputStream.close();
		}
		return byteArrayToByteString(digest.digest());
	}
	
	public String createHash(byte [] buf) throws NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		digest.update(buf);
		return byteArrayToByteString(digest.digest());
	}
	
	// Taken from http://www.devx.com/tips/Tip/13540
    private String byteArrayToByteString(byte in[]) {
        byte ch = 0x00;
        int i = 0;
        if (in == null || in.length <= 0)
            return null;

        String pseudo[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                          "A", "B", "C", "D", "E", "F"};
        StringBuffer out = new StringBuffer(in.length * 2);

        while (i < in.length) {
            ch = (byte) (in[i] & 0xF0); // Strip off high nibble
            ch = (byte) (ch >>> 4); // shift the bits down
            ch = (byte) (ch & 0x0F); // must do this is high order bit is on!
            out.append(pseudo[(int) ch]); // convert the nibble to a String
            // Character
            ch = (byte) (in[i] & 0x0F); // Strip off low nibble
            out.append(pseudo[(int) ch]); // convert the nibble to a String
            // Character
            i++;
        }

        return out.toString();

    }

}
