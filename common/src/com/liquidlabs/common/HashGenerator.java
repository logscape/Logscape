package com.liquidlabs.common;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashGenerator {
	
	int bufferSize = 16 * 1024;
	// can also use SHA-1 - but it is slower
	String digestType = System.getProperty("vso.digest.type", "MD5");
	public String createHash(String filename, File file) throws NoSuchAlgorithmException, IOException {
		
		if (!file.exists()) return "-1";
		if (file.isDirectory()) {
			throw new RuntimeException("Cannot generate HASH on directory:" + file.getAbsolutePath() + " need a file with data");
		}
		if (!file.canRead()) {
			throw new RuntimeException("Insufficient PERMS:" + file.getAbsolutePath());
		}
		MessageDigest digest = MessageDigest.getInstance(digestType);
		InputStream inputStream = new BufferedInputStream(new FileInputStream(file), bufferSize);
		try {
			byte[] buf = new byte[bufferSize];
			digest.update(filename.getBytes());
			while (inputStream.available() > 0) {
				int count = inputStream.read(buf);
				digest.update(buf, 0, count);
			}
		} finally {
			inputStream.close();
		}
		return byteArrayToByteString(digest.digest());
	}
	
	public String createHash(byte [] buf) throws NoSuchAlgorithmException {
		
		if (buf == null) throw new RuntimeException("Given NULL buf[] to createHash from");
		if (buf.length == 0) throw new RuntimeException("Given buf[0] createHash from");
		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		if (digest == null) throw new RuntimeException("Failed to access MessageDigest.getInstance(\"SHA-1\") - suspect invalid JRE install");
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
        StringBuilder out = new StringBuilder(in.length * 2);

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
