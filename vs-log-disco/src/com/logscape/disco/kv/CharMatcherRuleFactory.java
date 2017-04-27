package com.logscape.disco.kv;

import gnu.trove.set.hash.TCharHashSet;

public class CharMatcherRuleFactory {
    public static KVRule.Rule getMatcher(String preString) {

        if (preString.startsWith("[")) {
            String substring = preString.substring(1, preString.length() - 1);
            char[] chars = substring.toCharArray();
            if (chars.length == 1) {
                return new SingleCharMatch(chars[0]);
            } else if (chars.length == 2) {
                return new TwoCharMatch(chars[0], chars[1]);
            } else if (chars.length == 3) {
                return new ThreeCharMatch(chars[0], chars[1], chars[2]);
            } else return new SetCharMatch(substring.toCharArray());
        } else {
            return new SingleCharMatch(preString.toCharArray()[0]);
        }
    }


    static class SingleCharMatch extends KVRule.Rule {
        char c;
        public SingleCharMatch(char c) {
            this.c =c;
        }
        public boolean isValid(char c) {
            return this.c == c;
        }
    }
    static class TwoCharMatch extends KVRule.Rule {
        char c1;
        char c2;
        public TwoCharMatch(char c1, char c2) {
            this.c1 =c1;
            this.c2 =c2;
        }
        public boolean isValid(char c) {
            return this.c1 == c || this.c2 == c;
        }
    }
    static class ThreeCharMatch extends KVRule.Rule {
        char c1;
        char c2;
        char c3;
        public ThreeCharMatch(char c1, char c2, char c3) {
            this.c1 =c1;
            this.c2 =c2;
            this.c3 = c3;
        }
        public boolean isValid(char c) {
            return this.c1 == c || this.c2 == c || this.c3 == c;
        }
    }
    static class SetCharMatch extends KVRule.Rule {
        TCharHashSet cset;
        public SetCharMatch(char[] cs) {
            this.cset = new TCharHashSet(cs);
        }
        public boolean isValid(char c) {
            return cset.contains(c);
        }
    }


}

