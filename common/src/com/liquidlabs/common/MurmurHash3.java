package com.liquidlabs.common;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 18/07/2013
 * Time: 14:34
 * To change this template use File | Settings | File Templates.
 */
public class MurmurHash3 {

    private MurmurHash3() {}

    public static int hashString(String data, int seed) {

        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;
        int length = data.length();
        int roundedEnd = (length & 0xfffffffc);  // round down to 4 byte block

        for (int i = 0; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data.charAt(i) & 0xff) | ((data.charAt(i + 1) & 0xff) << 8) | ((data.charAt(i + 2) & 0xff) << 16) | (data.charAt(i + 3) << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch(length & 0x03) {
            case 3:
                k1 = (data.charAt(roundedEnd + 2) & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data.charAt(roundedEnd + 1) & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= data.charAt(roundedEnd) & 0xff;
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= c2;
                h1 ^= k1;
            default:
        }

        // finalization
        h1 ^= length;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }


    /** Returns the MurmurHash3_x86_32 hash. */
    public static int hash(byte[] data, int offset, int len, int seed) {

        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        for (int i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch(len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= data[roundedEnd] & 0xff;
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
                k1 *= c2;
                h1 ^= k1;
            default:
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

}
