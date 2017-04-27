package com.liquidlabs.common.collection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

public class CompactCharSequence implements CharSequence, Serializable, Comparable<CompactCharSequence>
{
    static final long serialVersionUID = 1L;

    private static final String ENCODING = System.getProperty("file.encoding");
    public static final String EMPTY_STRING = "";
    public static CompactCharSequence EMPTY = new CompactCharSequence("");
    private final byte[] data;

    public CompactCharSequence(byte[] data)
    {
        this.data = data;
    }

    public CompactCharSequence(String str)
    {
        try
        {
            data = str.getBytes(ENCODING);
        } catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException("Unexpected: " + ENCODING + " not supported!");
        }
    }
    public int compareTo(CompactCharSequence other){
        int len1 = data.length;
        int len2 = other.data.length;
        int lim = Math.min(len1, len2);
        byte v1[] = data;
        byte v2[] = other.data;

        int k = 0;
        while (k < lim) {
            byte c1 = v1[k];
            byte c2 = v2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }


    public char charAt(int index){
        int ix = index;
        if (ix >= data.length) {
            throw new StringIndexOutOfBoundsException("Invalid index " +
                    index + " length " + length());
        }
        return (char) (data[ix] & 0xff);
    }

    public int length(){
        return data.length;
    }
    public byte[] data() {
        return data;
    }

    @Override
    public int hashCode() {
        return MurmurHash3.hash(this.data, 0, this.data.length, 128);
    }

    @Override
    public boolean equals(Object obj) {
        CompactCharSequence other = (CompactCharSequence) obj;
        if (this.data.length !=  other.data.length) return false;
        if ((this.data.length > 0 && other.data.length > 0) && (this.data[0] != other.data[0])) return false;

        byte v1[] = data;
        byte v2[] = other.data;
        int i = 0;
        int j = 0;
        int n = data.length;
        while (n-- != 0) {
            if (v1[i++] != v2[j++])
                return false;
        }
        return true;
    }

    public CharSequence subSequence(int start, int end)
    {
        if (start < 0 || end >= (data.length))
        {
            throw new IllegalArgumentException("Illegal range " +
                    start + "-" + end + " for sequence of length " + length());
        }
        return new CompactCharSequence(data);
    }

    public String toString()
    {
        try
        {
            if (this.length() == 0) return EMPTY_STRING;
            char[] charBuffer = new char[data.length];
            int bpos = 0;
            for(int i = 0; i < data.length; i++) {
                charBuffer[bpos++] = (char) data[i];
            }
            return StringUtil.wrapCharArray(charBuffer);
        // OLD    return new String(data, ENCODING);
        } catch (Exception e)
        {
            throw new RuntimeException("Unexpected: " + ENCODING + " not supported:" + toString().toString());
        }
    }

    public boolean equals(CompactCharSequence other){
        return this != null && this.toString().equals(other.toString());
    }

    public static CompactCharSequence[] toArray(String[] strings, ConcurrentLRUCache<String, CompactCharSequence> dedupCache) {
        CompactCharSequence[] results = new CompactCharSequence[strings.length];
        for (int i = 0; i < strings.length; i++) {
            String stringKey = strings[i];
            if (dedupCache != null) {
                CompactCharSequence existing = dedupCache.get(stringKey);
                if (existing != null) {
                    results[i] = existing;
                }
                else {
                    CompactCharSequence newItem = new CompactCharSequence(stringKey);
                    results[i] = newItem;
                    dedupCache.put(stringKey, newItem);
                }
            } else {
                results[i] = new CompactCharSequence(stringKey);
            }
        }
        return results;
    }
    public static class Serializer extends com.esotericsoftware.kryo.Serializer {
        @Override
        public void write(Kryo kryo, Output output, Object o) {

            CompactCharSequence cc = (CompactCharSequence) o;
            kryo.writeObject(output, cc.data);
        }

        @Override
        public Object read(Kryo kryo, Input input, Class aClass) {
            CompactCharSequence compactCharSequence = new CompactCharSequence(kryo.readObject(input, byte[].class));
            if (compactCharSequence.length() == 0) return CompactCharSequence.EMPTY;
            return compactCharSequence;
        }
    }


}