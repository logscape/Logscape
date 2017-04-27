package com.liquidlabs.common.collection.cache;

import javolution.util.FastComparator;

/**
 *https://gist.github.com/badboy/6267743
 *
 * java hashmap with int
 */
public class JavoIntKeyEquality extends FastComparator<Integer> {

    @Override
    public boolean areEqual(Integer integer, Integer integer2) {
        if (integer == null || integer2 == null) return false;
        return integer.intValue() ==  integer2.intValue();
    }

    @Override
    public int hashCodeOf(Integer integer) {
//        if (true) return integer;
        int a = integer.intValue();
//        if (a == 0) return 0;
//        return a * 1024 * 1024 * 1024 * 1024;
        a ^= (a << 13);
        a ^= (a >>> 17);
        a ^= (a << 5);
        return a;
    }
    public int hash32shift(int key)
    {
        key = ~key + (key << 15); // key = (key << 15) - key - 1;
        key = key ^ (key >>> 12);
        key = key + (key << 2);
        key = key ^ (key >>> 4);
        key = key * 2057; // key = (key + (key << 3)) + (key << 11);
        key = key ^ (key >>> 16);
        return key;
    }
    public int hash32shiftmult(int key)
    {
        int c2=0x27d4eb2d; // a prime or an odd constant
        key = (key ^ 61) ^ (key >>> 16);
        key = key + (key << 3);
        key = key ^ (key >>> 4);
        key = key * c2;
        key = key ^ (key >>> 15);
        return key;
    }
    public int hashStackOverflow(int a) {
        a ^= (a << 13);
        a ^= (a >>> 17);
        a ^= (a << 5);
        return a;
    }


    @Override
    public int compare(Integer x, Integer y) {
        return (x.intValue() < y.intValue()) ? -1 : ((x.intValue() == y.intValue()) ? 0 : 1);
    }
}